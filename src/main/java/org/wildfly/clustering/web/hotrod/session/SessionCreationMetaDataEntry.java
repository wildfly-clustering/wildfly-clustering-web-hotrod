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

import java.util.concurrent.atomic.AtomicReference;

/**
 * Cache entry containing the session creation meta data and local context.
 * @author Paul Ferraro
 */
public class SessionCreationMetaDataEntry<K, L> {

    private final K key;
    private final SessionCreationMetaData metaData;
    private final AtomicReference<L> localContext;

    public SessionCreationMetaDataEntry(K key, SessionCreationMetaData metaData) {
        this(key, metaData, new AtomicReference<>());
    }

    public SessionCreationMetaDataEntry(HotRodSessionMetaData<K, L> entry) {
        this(entry.getKey(), entry.getCreationMetaData(), entry.getLocalContext());
    }

    public SessionCreationMetaDataEntry(K key, SessionCreationMetaData metaData, AtomicReference<L> localContext) {
        this.key = key;
        this.metaData = metaData;
        this.localContext = localContext;
    }

    public K getKey() {
        return this.key;
    }

    public SessionCreationMetaData getMetaData() {
        return this.metaData;
    }

    public AtomicReference<L> getLocalContext() {
        return this.localContext;
    }
}
