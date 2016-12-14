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

package org.wildfly.clustering.web.hotrod;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.function.Function;

import org.wildfly.clustering.marshalling.Externalizer;
import org.wildfly.clustering.marshalling.spi.util.UUIDExternalizer;

/**
 * @author Paul Ferraro
 */
public class SessionKeyExternalizer<K extends SessionKey<UUID>> implements Externalizer<K> {

    private static final Externalizer<UUID> UUID_EXTERNALIZER = new UUIDExternalizer();

    public interface SessionKeyFactory<K> {
        K createSessionKey(UUID id, ObjectInput input) throws IOException;
    }

    public interface SessionKeyWriter<K> {
        void writeCompoundKeyComponents(K key, ObjectOutput output) throws IOException;
    }

    private final Class<? extends K> targetClass;
    private final SessionKeyFactory<K> factory;
    private final SessionKeyWriter<K> writer;

    @SuppressWarnings("unchecked")
    public SessionKeyExternalizer(@SuppressWarnings("rawtypes") Class targetClass, SessionKeyFactory<K> factory, SessionKeyWriter<K> writer) {
        this.targetClass = targetClass;
        this.factory = factory;
        this.writer = writer;
    }

    public SessionKeyExternalizer(@SuppressWarnings("rawtypes") Class targetClass, Function<UUID, K> factory) {
        this(targetClass, (id, input) -> factory.apply(id), (key, output) -> {});
    }

    @Override
    public void writeObject(ObjectOutput output, K key) throws IOException {
        UUID_EXTERNALIZER.writeObject(output, key.getId());
        this.writer.writeCompoundKeyComponents(key, output);
    }

    @Override
    public K readObject(ObjectInput input) throws IOException, ClassNotFoundException {
        UUID id = UUID_EXTERNALIZER.readObject(input);
        return this.factory.createSessionKey(id, input);
    }

    @Override
    public Class<? extends K> getTargetClass() {
        return this.targetClass;
    }
}
