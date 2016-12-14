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
package org.wildfly.clustering.web.hotrod.session.fine;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.infinispan.client.hotrod.RemoteCache;
import org.wildfly.clustering.marshalling.spi.InvalidSerializedFormException;
import org.wildfly.clustering.marshalling.spi.Marshaller;
import org.wildfly.clustering.web.hotrod.Logger;
import org.wildfly.clustering.web.session.ImmutableSessionAttributes;

/**
 * Exposes session attributes for fine granularity sessions.
 * @author Paul Ferraro
 */
public class FineImmutableSessionAttributes<K, V> implements ImmutableSessionAttributes {
    private final K key;
    private final Map<String, Integer> names;
    private final RemoteCache<SessionAttributeKey<K>, V> cache;
    private final Function<Integer, SessionAttributeKey<K>> keyFactory;
    private final Marshaller<Object, V> marshaller;

    public FineImmutableSessionAttributes(K key, Map<String, Integer> names, RemoteCache<SessionAttributeKey<K>, V> cache, Function<Integer, SessionAttributeKey<K>> keyFactory, Marshaller<Object, V> marshaller) {
        this.key = key;
        this.names = names;
        this.cache = cache;
        this.keyFactory = keyFactory;
        this.marshaller = marshaller;
    }

    @Override
    public Set<String> getAttributeNames() {
        return this.names.keySet();
    }

    @Override
    public Object getAttribute(String name) {
        Integer attributeId = this.names.get(name);
        return (attributeId != null) ? this.read(name, this.cache.get(this.keyFactory.apply(attributeId))) : null;
    }

    protected Object read(String name, V value) {
        try {
            return this.marshaller.read(value);
        } catch (InvalidSerializedFormException e) {
            // This should not happen here, since attributes were pre-activated during FineSessionFactory.findValue(...)
            throw Logger.ROOT_LOGGER.failedToReadSessionAttribute(e, this.key.toString(), name);
        }
    }
}
