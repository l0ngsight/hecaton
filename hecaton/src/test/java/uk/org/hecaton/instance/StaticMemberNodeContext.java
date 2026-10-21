/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.instance;

import uk.org.hecaton.internal.cluster.Joiner;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.instance.impl.HecatonNodeExtension;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeContext;
import uk.org.hecaton.instance.impl.NodeExtension;
import uk.org.hecaton.internal.server.tcp.LocalAddressRegistry;
import uk.org.hecaton.internal.server.tcp.ServerSocketRegistry;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.internal.server.Server;
import uk.org.hecaton.test.TestHecatonInstanceFactory;

import java.util.Collections;
import java.util.UUID;

public class StaticMemberNodeContext implements NodeContext {
    private final NodeContext delegate;
    private final UUID uuid;

    public StaticMemberNodeContext(TestHecatonInstanceFactory factory, Member member) {
        this(factory, member.getUuid(), member.getAddress());
    }

    public StaticMemberNodeContext(TestHecatonInstanceFactory factory, UUID uuid, Address address) {
        this.uuid = uuid;
        delegate = factory.getRegistry().createNodeContext(address, Collections.emptySet());
    }

    @Override
    public NodeExtension createNodeExtension(Node node) {
        return new HecatonNodeExtension(node) {
            @Override
            public UUID createMemberUuid() {
                return uuid;
            }
        };
    }

    @Override
    public AddressPicker createAddressPicker(Node node) {
        return delegate.createAddressPicker(node);
    }

    @Override
    public Joiner createJoiner(Node node) {
        return delegate.createJoiner(node);
    }

    @Override
    public Server createServer(Node node, ServerSocketRegistry registry, LocalAddressRegistry addressRegistry) {
        return delegate.createServer(node, registry, addressRegistry);
    }
}
