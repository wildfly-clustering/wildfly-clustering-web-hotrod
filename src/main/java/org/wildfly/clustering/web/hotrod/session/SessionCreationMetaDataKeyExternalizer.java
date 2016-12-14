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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import org.kohsuke.MetaInfServices;
import org.wildfly.clustering.marshalling.Externalizer;
import org.wildfly.clustering.web.IdentifierExternalizerProvider;

/**
 * @author Paul Ferraro
 */
@MetaInfServices(Externalizer.class)
public class SessionCreationMetaDataKeyExternalizer implements Externalizer<SessionCreationMetaDataKey> {

    private static final Externalizer<String> EXTERNALIZER = StreamSupport.stream(ServiceLoader.load(IdentifierExternalizerProvider.class, IdentifierExternalizerProvider.class.getClassLoader()).spliterator(), false).findFirst().get().getExternalizer();

    @Override
    public void writeObject(ObjectOutput output, SessionCreationMetaDataKey key) throws IOException {
        output.writeUTF(key.getDeployment());
        EXTERNALIZER.writeObject(output, key.getId());
    }

    @Override
    public SessionCreationMetaDataKey readObject(ObjectInput input) throws IOException, ClassNotFoundException {
        String deployment = input.readUTF();
        String sessionId = EXTERNALIZER.readObject(input);
        return new SessionCreationMetaDataKey(deployment, sessionId);
    }

    @Override
    public Class<? extends SessionCreationMetaDataKey> getTargetClass() {
        return SessionCreationMetaDataKey.class;
    }
}
