/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2013, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.wildfly.clustering.web.hotrod.session;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSessionActivationListener;
import javax.servlet.http.HttpSessionEvent;

import org.wildfly.clustering.ee.Batch;
import org.wildfly.clustering.ee.Batcher;
import org.wildfly.clustering.ee.hotrod.HotRodBatcher;
import org.wildfly.clustering.service.concurrent.ServiceExecutor;
import org.wildfly.clustering.service.concurrent.StampedLockServiceExecutor;
import org.wildfly.clustering.web.IdentifierFactory;
import org.wildfly.clustering.web.hotrod.Keyed;
import org.wildfly.clustering.web.hotrod.Logger;
import org.wildfly.clustering.web.session.ImmutableHttpSessionAdapter;
import org.wildfly.clustering.web.session.ImmutableSession;
import org.wildfly.clustering.web.session.ImmutableSessionAttributes;
import org.wildfly.clustering.web.session.Session;
import org.wildfly.clustering.web.session.SessionAttributes;
import org.wildfly.clustering.web.session.SessionExpirationListener;
import org.wildfly.clustering.web.session.SessionManager;
import org.wildfly.clustering.web.session.SessionMetaData;

/**
 * Generic HotRod-based session manager implementation - independent of cache mapping strategy.
 * @author Paul Ferraro
 */
public class HotRodSessionManager<K, MV extends Keyed<K>, AV, L> implements SessionManager<L, Batch> {
    private final SessionExpirationListener expirationListener;
    private final SessionFactory<K, MV, AV, L> factory;
    private final IdentifierFactory<String> identifierFactory;
    private volatile Duration defaultMaxInactiveInterval = Duration.ofMinutes(30L);
    private final ServletContext context;

    private volatile Scheduler scheduler;
    private volatile ServiceExecutor executor;

    public HotRodSessionManager(SessionFactory<K, MV, AV, L> factory, HotRodSessionManagerConfiguration configuration) {
        this.factory = factory;
        this.expirationListener = configuration.getExpirationListener();
        this.context = configuration.getServletContext();
        this.identifierFactory = configuration.getIdentifierFactory();
    }

    @Override
    public void start() {
        this.executor = new StampedLockServiceExecutor();
        this.scheduler = new SessionExpirationScheduler(new ExpiredSessionRemover<>(this.factory, this.expirationListener));
    }

    @Override
    public void stop() {
        this.executor.close(() -> {
            this.scheduler.close();
        });
    }

    @Override
    public Batcher<Batch> getBatcher() {
        return HotRodBatcher.INSTANCE;
    }

    @Override
    public Duration getDefaultMaxInactiveInterval() {
        return this.defaultMaxInactiveInterval;
    }

    @Override
    public void setDefaultMaxInactiveInterval(Duration duration) {
        this.defaultMaxInactiveInterval = duration;
    }

    @Override
    public String createIdentifier() {
        return this.identifierFactory.createIdentifier();
    }

    @Override
    public Session<L> findSession(String id) {
        SessionEntry<K, MV, AV> entry = this.factory.findValue(id);
        if (entry == null) {
            Logger.ROOT_LOGGER.tracef("Session %s not found", id);
            return null;
        }
        ImmutableSession session = this.factory.createImmutableSession(id, entry);
        if (session.getMetaData().isExpired()) {
            Logger.ROOT_LOGGER.tracef("Session %s was found, but has expired", id);
            this.expirationListener.sessionExpired(session);
            this.factory.remove(id);
            return null;
        }
        this.scheduler.cancel(id);
        this.triggerPostActivationEvents(session);
        return new SchedulableSession(this.factory.createSession(id, entry), session);
    }

    @Override
    public Session<L> createSession(String id) {
        SessionEntry<K, MV, AV> entry = this.factory.createValue(id, null);
        if (entry == null) return null;
        Session<L> session = this.factory.createSession(id, entry);
        session.getMetaData().setMaxInactiveInterval(this.defaultMaxInactiveInterval);
        return new SchedulableSession(session, session);
    }

    @Override
    public ImmutableSession viewSession(String id) {
        SessionEntry<K, MV, AV> entry = this.factory.findValue(id);
        return (entry != null) ? new SimpleImmutableSession(this.factory.createImmutableSession(id, entry)) : null;
    }

    @Override
    public Set<String> getActiveSessions() {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getLocalSessions() {
        return Collections.emptySet();
    }

    @Override
    public int getMaxActiveSessions() {
        return Integer.MAX_VALUE;
    }

    @Override
    public long getActiveSessionCount() {
        return this.getActiveSessions().size();
    }

    void triggerPrePassivationEvents(ImmutableSession session) {
        List<HttpSessionActivationListener> listeners = findListeners(session);
        if (!listeners.isEmpty()) {
            HttpSessionEvent event = new HttpSessionEvent(new ImmutableHttpSessionAdapter(session, this.context));
            listeners.forEach(listener -> listener.sessionWillPassivate(event));
        }
    }

    void triggerPostActivationEvents(ImmutableSession session) {
        List<HttpSessionActivationListener> listeners = findListeners(session);
        if (!listeners.isEmpty()) {
            HttpSessionEvent event = new HttpSessionEvent(new ImmutableHttpSessionAdapter(session, this.context));
            listeners.forEach(listener -> listener.sessionDidActivate(event));
        }
    }

    void schedule(ImmutableSession session) {
        this.scheduler.schedule(session.getId(), session.getMetaData());
    }

    private static List<HttpSessionActivationListener> findListeners(ImmutableSession session) {
        ImmutableSessionAttributes attributes = session.getAttributes();
        return attributes.getAttributeNames().stream().map(name -> attributes.getAttribute(name))
                .filter(attribute -> attribute instanceof HttpSessionActivationListener)
                .map(attribute -> (HttpSessionActivationListener) attribute).collect(Collectors.toList());
    }

    // Session decorator that performs scheduling on close().
    private class SchedulableSession implements Session<L> {
        private final Session<L> session;
        private final ImmutableSession immutableSession;

        SchedulableSession(Session<L> session, ImmutableSession immutableSession) {
            this.session = session;
            this.immutableSession = immutableSession;
        }

        @Override
        public String getId() {
            return this.session.getId();
        }

        @Override
        public SessionMetaData getMetaData() {
            if (!this.session.isValid()) {
                throw Logger.ROOT_LOGGER.invalidSession(this.getId());
            }
            return this.session.getMetaData();
        }

        @Override
        public boolean isValid() {
            return this.session.isValid();
        }

        @Override
        public void invalidate() {
            if (!this.session.isValid()) {
                throw Logger.ROOT_LOGGER.invalidSession(this.getId());
            }
            this.session.invalidate();
        }

        @Override
        public SessionAttributes getAttributes() {
            if (!this.session.isValid()) {
                throw Logger.ROOT_LOGGER.invalidSession(this.getId());
            }
            return this.session.getAttributes();
        }

        @Override
        public void close() {
            boolean valid = this.session.isValid();
            if (valid) {
                HotRodSessionManager.this.triggerPrePassivationEvents(this.immutableSession);
            }
            this.session.close();
            if (valid) {
                HotRodSessionManager.this.schedule(this.immutableSession);
            }
        }

        @Override
        public L getLocalContext() {
            return this.session.getLocalContext();
        }
    }
}
