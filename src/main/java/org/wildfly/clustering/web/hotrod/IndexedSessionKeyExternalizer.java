/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2018, Red Hat, Inc., and individual contributors
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

package org.wildfly.clustering.web.hotrod;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;

import org.wildfly.clustering.marshalling.Externalizer;
import org.wildfly.clustering.marshalling.spi.IndexSerializer;

/**
 * @author Paul Ferraro
 */
public class IndexedSessionKeyExternalizer<K extends SessionKey<UUID>> implements Externalizer<K> {
    private final Class<K> targetClass;
    private final ToIntFunction<K> index;
    private final BiFunction<UUID, Integer, K> resolver;

    protected IndexedSessionKeyExternalizer(Class<K> targetClass, ToIntFunction<K> index, BiFunction<UUID, Integer, K> resolver) {
        this.targetClass = targetClass;
        this.index = index;
        this.resolver = resolver;
    }

    @Override
    public Class<K> getTargetClass() {
        return this.targetClass;
    }

    @Override
    public K readObject(ObjectInput input) throws IOException, ClassNotFoundException {
        UUID id = SessionKeyExternalizer.IDENTIFIER_EXTERNALIZER.readObject(input);
        int index = IndexSerializer.VARIABLE.readInt(input);
        return this.resolver.apply(id, index);
    }

    @Override
    public void writeObject(ObjectOutput output, K key) throws IOException {
        SessionKeyExternalizer.IDENTIFIER_EXTERNALIZER.writeObject(output, key.getId());
        IndexSerializer.VARIABLE.writeInt(output, this.index.applyAsInt(key));
    }
}
