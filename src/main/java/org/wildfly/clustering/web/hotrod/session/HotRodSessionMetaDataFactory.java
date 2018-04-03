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

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.wildfly.clustering.ee.Mutator;
import org.wildfly.clustering.ee.hotrod.RemoteCacheEntryMutator;
import org.wildfly.clustering.web.session.ImmutableSessionMetaData;

/**
 * @author Paul Ferraro
 */
public class HotRodSessionMetaDataFactory<L> implements SessionMetaDataFactory<UUID, HotRodSessionMetaData<L>, L> {

    private final RemoteCache<SessionCreationMetaDataKey, SessionCreationMetaDataEntry<L>> creationMetaDataCache;
    private final RemoteCache<SessionAccessMetaDataKey, SessionAccessMetaData> accessMetaDataCache;
    private final String deployment;

    @SuppressWarnings("unchecked")
    public HotRodSessionMetaDataFactory(String deployment, RemoteCache<?, ?> cache) {
        this.creationMetaDataCache = (RemoteCache<SessionCreationMetaDataKey, SessionCreationMetaDataEntry<L>>) cache;
        this.accessMetaDataCache = (RemoteCache<SessionAccessMetaDataKey, SessionAccessMetaData>) cache;
        this.deployment = deployment;
    }

    @Override
    public HotRodSessionMetaData<L> createValue(String id, Void context) {
        SessionCreationMetaDataEntry<L> creationMetaDataEntry = new SessionCreationMetaDataEntry<>(UUID.randomUUID(), new SimpleSessionCreationMetaData());
        if (this.creationMetaDataCache.withFlags(Flag.FORCE_RETURN_VALUE).putIfAbsent(new SessionCreationMetaDataKey(this.deployment, id), creationMetaDataEntry) != null) {
            return null;
        }
        SessionAccessMetaData accessMetaData = new SimpleSessionAccessMetaData();
        this.accessMetaDataCache.put(new SessionAccessMetaDataKey(creationMetaDataEntry.getId()), accessMetaData);
        return new HotRodSessionMetaData<>(creationMetaDataEntry, accessMetaData);
    }

    @Override
    public HotRodSessionMetaData<L> findValue(String id) {
        SessionCreationMetaDataKey key = new SessionCreationMetaDataKey(this.deployment, id);
        MetadataValue<SessionCreationMetaDataEntry<L>> value = this.creationMetaDataCache.getWithMetadata(key);
        SessionCreationMetaDataEntry<L> creationMetaDataEntry = this.creationMetaDataCache.get(key);
        if (creationMetaDataEntry != null) {
            SessionAccessMetaData accessMetaData = this.accessMetaDataCache.get(new SessionAccessMetaDataKey(creationMetaDataEntry.getId()));
            if (accessMetaData != null) {
                return new HotRodSessionMetaData<>(creationMetaDataEntry, accessMetaData);
            }
            this.creationMetaDataCache.removeWithVersion(key, value.getVersion());
        }
        return null;
    }

    @Override
    public InvalidatableSessionMetaData createSessionMetaData(String id, HotRodSessionMetaData<L> entry) {
        SessionCreationMetaDataKey creationMetaDataKey = new SessionCreationMetaDataKey(this.deployment, id);
        Mutator creationMutator = new RemoteCacheEntryMutator<>(this.creationMetaDataCache, creationMetaDataKey, new SessionCreationMetaDataEntry<>(entry));
        SessionCreationMetaData creationMetaData = new MutableSessionCreationMetaData(entry.getCreationMetaData(), creationMutator);

        SessionAccessMetaDataKey accessMetaDataKey = new SessionAccessMetaDataKey(entry.getId());
        Mutator accessMutator = new RemoteCacheEntryMutator<>(this.accessMetaDataCache, accessMetaDataKey, entry.getAccessMetaData());
        SessionAccessMetaData accessMetaData = new MutableSessionAccessMetaData(entry.getAccessMetaData(), accessMutator);

        return new SimpleSessionMetaData(creationMetaData, accessMetaData);
    }

    @Override
    public ImmutableSessionMetaData createImmutableSessionMetaData(String id, HotRodSessionMetaData<L> entry) {
        return new SimpleSessionMetaData(entry.getCreationMetaData(), entry.getAccessMetaData());
    }

    @Override
    public UUID remove(String id) {
        SessionCreationMetaDataKey key = new SessionCreationMetaDataKey(this.deployment, id);
        SessionCreationMetaDataEntry<L> creationMetaData = this.creationMetaDataCache.withFlags(Flag.FORCE_RETURN_VALUE).remove(key);
        if (creationMetaData == null) return null;
        UUID uuid = creationMetaData.getId();
        this.accessMetaDataCache.remove(new SessionAccessMetaDataKey(uuid));
        return uuid;
    }
}
