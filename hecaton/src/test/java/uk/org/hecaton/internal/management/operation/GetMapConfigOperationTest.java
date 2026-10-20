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

package uk.org.hecaton.internal.management.operation;

import uk.org.hecaton.client.impl.ClientDelegatingFuture;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.protocol.codec.MCGetMapConfigCodec;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GetMapConfigOperationTest extends HecatonTestSupport {

    private HecatonInstance hz;
    private TestHecatonFactory factory;
    private HecatonClientInstanceImpl client;

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    protected InMemoryFormat getInMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    @Before
    public void setUp() {
        factory = new TestHecatonFactory();

        Config config = getConfig();
        MapConfig withIndex = new MapConfig("map-with-index")
                .setInMemoryFormat(getInMemoryFormat())
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "first"));
        config.addMapConfig(withIndex);

        hz = factory.newHecatonInstance(config);
        client = ((HecatonClientProxy) factory.newHecatonClient()).client;
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void mapWithPreconfiguredIndex_empty() throws Exception {
        MCGetMapConfigCodec.ResponseParameters actual = runCommand(client, hz, "map-with-index").get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        // index is read from config
        assertThat(actual.globalIndexes)
                .usingElementComparatorIgnoringFields("name")
                .containsExactly(new IndexConfig(IndexType.SORTED, "first"));
    }

    @Test
    public void testMapWithPreconfiguredIndex_addedIndex() throws Exception {
        client.getMap("map-with-index").addIndex(new IndexConfig(IndexType.HASH, "second"));
        MCGetMapConfigCodec.ResponseParameters actual = runCommand(client, hz, "map-with-index").get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertThat(actual.globalIndexes)
                .usingElementComparatorIgnoringFields("name")
                .containsExactlyInAnyOrder(
                        new IndexConfig(IndexType.SORTED, "first"),
                        new IndexConfig(IndexType.HASH, "second")
                );
    }

    @Test
    public void testMapWithoutPreconfiguredIndexes_addedIndex() throws Exception {
        client.getMap("map-without-indexes").addIndex(new IndexConfig(IndexType.HASH, "second"));
        MCGetMapConfigCodec.ResponseParameters actual = runCommand(client, hz, "map-without-indexes").get(ASSERT_TRUE_EVENTUALLY_TIMEOUT, SECONDS);
        assertThat(actual.globalIndexes)
                .usingElementComparatorIgnoringFields("name")
                .containsExactlyInAnyOrder(
                        new IndexConfig(IndexType.HASH, "second")
                );
    }

    private ClientDelegatingFuture<MCGetMapConfigCodec.ResponseParameters> runCommand(
            HecatonClientInstanceImpl client, HecatonInstance member, String mapName) {
        ClientInvocation invocation = new ClientInvocation(
                client,
                MCGetMapConfigCodec.encodeRequest(mapName),
                null,
                member.getCluster().getLocalMember().getUuid()
        );

        return new ClientDelegatingFuture<>(
                invocation.invoke(),
                client.getSerializationService(),
                MCGetMapConfigCodec::decodeResponse
        );
    }
}
