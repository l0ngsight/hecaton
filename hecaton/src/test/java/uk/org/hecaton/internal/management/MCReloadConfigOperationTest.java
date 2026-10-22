/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.internal.management;

import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.protocol.codec.MCReloadConfigCodec;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.map.AbstractClientMapTest;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;

public class MCReloadConfigOperationTest
        extends AbstractClientMapTest {

    @Test
    public void test()
            throws ExecutionException, InterruptedException {
        ClientInvocation inv = new ClientInvocation(
                ((HecatonClientProxy) client).client,
                MCReloadConfigCodec.encodeRequest(),
                null
        );
        // configuration reload (F16) runs in the background: the request returns the update's ID (these members
        // have no configuration file, so the update itself fails and is logged)
        assertNotNull(MCReloadConfigCodec.decodeResponse(inv.invoke().get()));
    }
}
