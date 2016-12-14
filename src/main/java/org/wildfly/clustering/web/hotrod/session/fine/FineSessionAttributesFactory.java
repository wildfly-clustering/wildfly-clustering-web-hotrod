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

package org.wildfly.clustering.web.hotrod.session.fine;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.Flag;
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
 * {@link SessionAttributesFactory} for fine granularity sessions.
 * A given session's attributes are mapped to N+1 co-located cache entries, where N is the number of session attributes.
 * A separate cache entry stores the activate attribute names for the session.
 * @author Paul Ferraro
 */
public class FineSessionAttributesFactory<K, V> implements SessionAttributesFactory<K, SessionAttributeNamesEntry> {

    private final RemoteCache<SessionAttributeNamesKey<K>, SessionAttributeNamesEntry> namesCache;
    private final RemoteCache<SessionAttributeKey<K>, V> attributeCache;
    private final Marshaller<Object, V> marshaller;
    private final Function<K, SessionAttributeNamesKey<K>> attributeNamesKeyFactory;
    private final BiFunction<K, Integer, SessionAttributeKey<K>> attributeKeyFactory;

    public FineSessionAttributesFactory(RemoteCache<SessionAttributeNamesKey<K>, SessionAttributeNamesEntry> namesCache, RemoteCache<SessionAttributeKey<K>, V> attributeCache, Marshaller<Object, V> marshaller) {
        this.namesCache = namesCache;
        this.attributeCache = attributeCache;
        this.marshaller = marshaller;
        this.attributeNamesKeyFactory = id -> new SessionAttributeNamesKey<>(id);
        this.attributeKeyFactory = (id, attributeId) -> new SessionAttributeKey<>(id, attributeId);
    }

    @Override
    public SessionAttributeNamesEntry createValue(K id, Void context) {
        SessionAttributeNamesEntry entry = new SessionAttributeNamesEntry(new AtomicInteger(), new ConcurrentHashMap<>());
        this.namesCache.put(this.attributeNamesKeyFactory.apply(id), entry);
        return entry;
    }

    @Override
    public SessionAttributeNamesEntry findValue(K id) {
        SessionAttributeNamesEntry entry = this.namesCache.get(this.attributeNamesKeyFactory.apply(id));
        if (entry != null) {
            ConcurrentMap<String, Integer> names = entry.getNames();
            Map<SessionAttributeKey<K>, V> attributes = this.attributeCache.getAll(names.values().stream().map(attributeId -> this.attributeKeyFactory.apply(id, attributeId)).collect(Collectors.toSet()));
            Predicate<Map.Entry<String, V>> invalidAttribute = attribute -> {
                V value = attribute.getValue();
                if (value == null) {
                    Logger.ROOT_LOGGER.missingSessionAttributeCacheEntry(id.toString(), attribute.getKey());
                    return true;
                }
                try {
                    this.marshaller.read(attribute.getValue());
                    return false;
                } catch (InvalidSerializedFormException e) {
                    Logger.ROOT_LOGGER.failedToActivateSessionAttribute(e, id.toString(), attribute.getKey());
                    return true;
                }
            };
            if (names.entrySet().stream().map(name -> new AbstractMap.SimpleImmutableEntry<>(name.getKey(), attributes.get(this.attributeKeyFactory.apply(id, name.getValue())))).anyMatch(invalidAttribute)) {
                // If any attributes are invalid - remove them all
                this.remove(id);
                return null;
            }
        }
        return entry;
    }

    @Override
    public boolean remove(K id) {
        SessionAttributeNamesEntry entry = this.namesCache.withFlags(Flag.FORCE_RETURN_VALUE).remove(this.attributeNamesKeyFactory.apply(id));
        if (entry == null) return false;
        entry.getNames().values().forEach(attributeId -> this.attributeCache.remove(this.attributeKeyFactory.apply(id, attributeId)));
        return true;
    }

    @Override
    public SessionAttributes createSessionAttributes(K id, SessionAttributeNamesEntry entry) {
        SessionAttributeNamesKey<K> key = this.attributeNamesKeyFactory.apply(id);
        Mutator mutator = new RemoteCacheEntryMutator<>(this.namesCache, key, entry);
        return new FineSessionAttributes<>(id, entry.getSequence(), entry.getNames(), mutator, this.attributeCache, attribute -> this.attributeKeyFactory.apply(id, attribute), this.marshaller);
    }

    @Override
    public ImmutableSessionAttributes createImmutableSessionAttributes(K id, SessionAttributeNamesEntry entry) {
        return new FineImmutableSessionAttributes<>(id, entry.getNames(), this.attributeCache, attribute -> this.attributeKeyFactory.apply(id, attribute), this.marshaller);
    }
}
