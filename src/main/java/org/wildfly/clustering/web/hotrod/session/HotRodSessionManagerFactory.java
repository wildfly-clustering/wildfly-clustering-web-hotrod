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

import java.util.UUID;

import javax.servlet.ServletContext;

import org.wildfly.clustering.ee.Batch;
import org.wildfly.clustering.marshalling.spi.Marshallability;
import org.wildfly.clustering.marshalling.spi.MarshalledValueFactory;
import org.wildfly.clustering.marshalling.spi.MarshalledValueMarshaller;
import org.wildfly.clustering.web.IdentifierFactory;
import org.wildfly.clustering.web.LocalContextFactory;
import org.wildfly.clustering.web.hotrod.session.coarse.CoarseSessionAttributesFactory;
import org.wildfly.clustering.web.hotrod.session.fine.FineSessionAttributesFactory;
import org.wildfly.clustering.web.session.SessionExpirationListener;
import org.wildfly.clustering.web.session.SessionManager;
import org.wildfly.clustering.web.session.SessionManagerConfiguration;
import org.wildfly.clustering.web.session.SessionManagerFactoryConfiguration;
import org.wildfly.clustering.web.session.SessionManagerFactory;

/**
 * Factory for creating session managers.
 * @author Paul Ferraro
 */
public class HotRodSessionManagerFactory<C extends Marshallability> implements SessionManagerFactory<Batch> {

    private final HotRodSessionManagerFactoryConfiguration<C> config;

    public HotRodSessionManagerFactory(HotRodSessionManagerFactoryConfiguration<C> config) {
        this.config = config;
    }

    @Override
    public <L> SessionManager<L, Batch> createSessionManager(final SessionManagerConfiguration<L> configuration) {
        HotRodSessionManagerConfiguration config = new HotRodSessionManagerConfiguration() {
            @Override
            public SessionExpirationListener getExpirationListener() {
                return configuration.getExpirationListener();
            }

            @Override
            public ServletContext getServletContext() {
                return configuration.getServletContext();
            }

            @Override
            public IdentifierFactory<String> getIdentifierFactory() {
                return configuration.getIdentifierFactory();
            }
        };
        return new HotRodSessionManager<>(this.createSessionFactory(configuration.getLocalContextFactory()), config);
    }

    private <L> SessionFactory<?, ?, ?, L> createSessionFactory(LocalContextFactory<L> localContextFactory) {
        SessionMetaDataFactory<UUID, HotRodSessionMetaData<UUID, L>, L> metaDataFactory = new HotRodSessionMetaDataFactory<>(this.config.getSessionManagerFactoryConfiguration().getDeploymentName(), this.config.getCache());
        return new HotRodSessionFactory<>(metaDataFactory, this.createSessionAttributesFactory(), localContextFactory);
    }

    private SessionAttributesFactory<UUID, ?> createSessionAttributesFactory() {
        SessionManagerFactoryConfiguration<C> config = this.config.getSessionManagerFactoryConfiguration();
        MarshalledValueFactory<C> factory = config.getMarshalledValueFactory();
        C context = config.getMarshallingContext();

        switch (this.config.getSessionManagerFactoryConfiguration().getAttributePersistenceStrategy()) {
            case FINE: {
                return new FineSessionAttributesFactory<>(this.config.getCache(), this.config.getCache(), new MarshalledValueMarshaller<>(factory, context));
            }
            case COARSE: {
                return new CoarseSessionAttributesFactory<>(this.config.getCache(), new MarshalledValueMarshaller<>(factory, context));
            }
            default: {
                // Impossible
                throw new IllegalStateException();
            }
        }
    }
}
