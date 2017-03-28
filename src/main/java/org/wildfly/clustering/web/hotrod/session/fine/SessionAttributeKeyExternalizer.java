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

import java.util.UUID;

import org.kohsuke.MetaInfServices;
import org.wildfly.clustering.marshalling.Externalizer;
import org.wildfly.clustering.marshalling.spi.IndexExternalizer;
import org.wildfly.clustering.web.hotrod.SessionKeyExternalizer;

/**
 * Externalizer for a {@link SessionAttributeKey}.
 * @author Paul Ferraro
 */
@MetaInfServices(Externalizer.class)
public class SessionAttributeKeyExternalizer extends SessionKeyExternalizer<SessionAttributeKey<UUID>> {

    @SuppressWarnings("unchecked")
    public SessionAttributeKeyExternalizer() {
        super((Class<SessionAttributeKey<UUID>>) (Class<?>) SessionAttributeKey.class, (id, input) -> new SessionAttributeKey<>(id, IndexExternalizer.VARIABLE.readData(input)), (key, output) -> IndexExternalizer.VARIABLE.writeData(output, key.getAttributeId()));
    }
}