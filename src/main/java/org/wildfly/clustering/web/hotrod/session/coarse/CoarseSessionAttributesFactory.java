/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2016, Red Hat, Inc., and individual contributors
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

package org.wildfly.clustering.web.hotrod.session.coarse;

import java.util.Map;
import java.util.UUID;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.client.hotrod.RemoteCache;
import org.wildfly.clustering.ee.Mutator;
import org.wildfly.clustering.ee.hotrod.RemoteCacheEntryMutator;
import org.wildfly.clustering.marshalling.spi.InvalidSerializedFormException;
import org.wildfly.clustering.marshalling.spi.Marshaller;
import org.wildfly.clustering.web.hotrod.Logger;
import org.wildfly.clustering.web.hotrod.session.SessionAttributes;
import org.wildfly.clustering.web.hotrod.session.SessionAttributesFactory;
import org.wildfly.clustering.web.session.ImmutableSessionAttributes;

/**
 * @author Paul Ferraro
 */
public class CoarseSessionAttributesFactory<V> implements SessionAttributesFactory<UUID, Map.Entry<Map<String, Object>, V>> {

    private final RemoteCache<SessionAttributesKey, V> cache;
    private final Marshaller<Map<String, Object>, V> marshaller;

    public CoarseSessionAttributesFactory(RemoteCache<SessionAttributesKey, V> cache, Marshaller<Map<String, Object>, V> marshaller) {
        this.cache = cache;
        this.marshaller = marshaller;
    }

    @Override
    public Map.Entry<Map<String, Object>, V> createValue(UUID id, Void context) {
        Map<String, Object> attributes = new ConcurrentHashMap<>();
        V value = this.marshaller.write(attributes);
        this.cache.put(new SessionAttributesKey(id), value);
        return new SimpleImmutableEntry<>(attributes, value);
    }

    @Override
    public Map.Entry<Map<String, Object>, V> findValue(UUID id) {
        V value = this.cache.get(new SessionAttributesKey(id));
        if (value != null) {
            try {
                Map<String, Object> attributes = this.marshaller.read(value);
                return new SimpleImmutableEntry<>(attributes, value);
            } catch (InvalidSerializedFormException e) {
                Logger.ROOT_LOGGER.failedToActivateSession(e, id.toString());
                this.remove(id);
            }
        }
        return null;
    }

    @Override
    public SessionAttributes createSessionAttributes(UUID id, Map.Entry<Map<String, Object>, V> entry) {
        Mutator mutator = new RemoteCacheEntryMutator<>(this.cache, new SessionAttributesKey(id), entry.getValue());
        return new CoarseSessionAttributes(entry.getKey(), mutator, this.marshaller);
    }

    @Override
    public ImmutableSessionAttributes createImmutableSessionAttributes(UUID id, Map.Entry<Map<String, Object>, V> entry) {
        return new CoarseImmutableSessionAttributes(entry.getKey());
    }

    @Override
    public boolean remove(UUID id) {
        this.cache.remove(new SessionAttributesKey(id));
        return true;
    }
}
