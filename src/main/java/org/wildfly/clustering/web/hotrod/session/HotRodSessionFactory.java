/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2015, Red Hat, Inc., and individual contributors
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

import java.util.UUID;

import org.wildfly.clustering.web.LocalContextFactory;
import org.wildfly.clustering.web.session.ImmutableSession;
import org.wildfly.clustering.web.session.ImmutableSessionAttributes;
import org.wildfly.clustering.web.session.ImmutableSessionMetaData;
import org.wildfly.clustering.web.session.Session;

/**
 * @author Paul Ferraro
 */
public class HotRodSessionFactory<V, L> implements SessionFactory<UUID, HotRodSessionMetaData<L>, V, L> {

    private final SessionMetaDataFactory<UUID, HotRodSessionMetaData<L>, L> metaDataFactory;
    private final SessionAttributesFactory<UUID, V> attributesFactory;
    private final LocalContextFactory<L> localContextFactory;

    public HotRodSessionFactory(SessionMetaDataFactory<UUID, HotRodSessionMetaData<L>, L> metaDataFactory, SessionAttributesFactory<UUID, V> attributesFactory, LocalContextFactory<L> localContextFactory) {
        this.metaDataFactory = metaDataFactory;
        this.attributesFactory = attributesFactory;
        this.localContextFactory = localContextFactory;
    }

    @Override
    public SessionEntry<UUID, HotRodSessionMetaData<L>, V> createValue(String id, Void context) {
        HotRodSessionMetaData<L> metaDataValue = this.metaDataFactory.createValue(id, context);
        if (metaDataValue == null) return null;
        UUID key = metaDataValue.getId();
        V attributesValue = this.attributesFactory.createValue(key, context);
        return new HotRodSessionEntry<>(key, metaDataValue, attributesValue);
    }

    @Override
    public SessionEntry<UUID, HotRodSessionMetaData<L>, V> findValue(String id) {
        HotRodSessionMetaData<L> metaData = this.metaDataFactory.findValue(id);
        if (metaData != null) {
            UUID key = metaData.getId();
            V attributes = this.attributesFactory.findValue(key);
            if (attributes != null) {
                return new HotRodSessionEntry<>(key, metaData, attributes);
            }
            // Purge obsolete meta data
            this.metaDataFactory.remove(id);
        }
        return null;
    }

    @Override
    public boolean remove(String id) {
        UUID key = this.metaDataFactory.remove(id);
        if (key == null) return false;
        this.attributesFactory.remove(key);
        return true;
    }

    @Override
    public SessionMetaDataFactory<UUID, HotRodSessionMetaData<L>, L> getMetaDataFactory() {
        return this.metaDataFactory;
    }

    @Override
    public SessionAttributesFactory<UUID, V> getAttributesFactory() {
        return this.attributesFactory;
    }

    @Override
    public Session<L> createSession(String id, SessionEntry<UUID, HotRodSessionMetaData<L>, V> entry) {
        HotRodSessionMetaData<L> metaDataValue = entry.getMetaDataValue();
        InvalidatableSessionMetaData metaData = this.metaDataFactory.createSessionMetaData(id, metaDataValue);
        SessionAttributes attributes = this.attributesFactory.createSessionAttributes(entry.getId(), entry.getAttributesValue());
        return new HotRodSession<>(id, metaData, attributes, metaDataValue.getLocalContext(), this.localContextFactory, this);
    }

    @Override
    public ImmutableSession createImmutableSession(String id, ImmutableSessionMetaData metaData, ImmutableSessionAttributes attributes) {
        return new HotRodImmutableSession(id, metaData, attributes);
    }
}
