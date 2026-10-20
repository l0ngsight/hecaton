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

package uk.org.hecaton.map.impl.wan;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ConsistencyCheckStrategy;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MerkleTreeConfig;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.config.WanConsumerConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.config.WanSyncConfig;
import uk.org.hecaton.internal.config.MergePolicyValidator;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicyProvider;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.wan.impl.DelegatingWanScheme;
import uk.org.hecaton.wan.impl.WanReplicationService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapWanContextTest {

    static String wanReplicationRefName = "wanReplicationRefName";
    static String mergePolicyClassName = "mergePolicyClassName";
    private static MockedStatic<MergePolicyValidator> mockedStatic;

    private MapWanContext mapWanContext;

    @Mock
    private MapContainer mapContainer;
    @Mock
    private MapConfig mapConfig;
    @Mock
    private MapServiceContext mapServiceContext;
    @Mock
    private NodeEngine nodeEngine;
    @Mock
    private WanReplicationRef wanReplicationRef;
    @Mock
    private Config config;
    @Mock
    private MerkleTreeConfig merkleTreeConfig;
    @Mock
    private WanReplicationConfig wanReplicationConfig;
    @Mock
    private SplitBrainMergePolicyProvider splitBrainMergePolicyProvider;
    @Mock
    private SplitBrainMergePolicy wanMergePolicy;
    @Mock
    private WanConsumerConfig wanConsumerConfig;
    @Mock
    private WanReplicationService wanReplicationService;
    @Mock
    private DelegatingWanScheme delegatingWanScheme;

    @BeforeClass
    public static void setUp() {
        mockedStatic = mockStatic(MergePolicyValidator.class);
    }

    @AfterClass
    public static void cleanUpMocks() {
        mockedStatic.close();
    }

    @Before
    public void initMocks() {
        when(mapContainer.getMapConfig()).thenReturn(mapConfig);
        when(mapContainer.getMapServiceContext()).thenReturn(mapServiceContext);
        when(mapServiceContext.getNodeEngine()).thenReturn(nodeEngine);
        when(nodeEngine.getConfig()).thenReturn(config);
        when(mapConfig.getWanReplicationRef()).thenReturn(wanReplicationRef);
        when(nodeEngine.getSplitBrainMergePolicyProvider()).thenReturn(splitBrainMergePolicyProvider);
        when(wanReplicationConfig.getConsumerConfig()).thenReturn(wanConsumerConfig);
        when(nodeEngine.getWanReplicationService()).thenReturn(wanReplicationService);
        when(mapConfig.getWanReplicationRef()).thenReturn(wanReplicationRef);
        when(wanReplicationRef.getName()).thenReturn(wanReplicationRefName);
        when(mapConfig.getMerkleTreeConfig()).thenReturn(merkleTreeConfig);
        when(wanReplicationRef.getMergePolicyClassName()).thenReturn(mergePolicyClassName);
        when(config.getWanReplicationConfig(wanReplicationRefName)).thenReturn(wanReplicationConfig);


        mapWanContext = new MapWanContext(mapContainer);
    }

    @Test
    public void testWanReplicationInitBasicPositive() {
        when(wanReplicationService.hasWanReplicationScheme(wanReplicationRefName)).thenReturn(true);
        when(wanReplicationService.getWanReplicationPublishers(wanReplicationRefName)).thenReturn(delegatingWanScheme);
        when(splitBrainMergePolicyProvider.getMergePolicy(mergePolicyClassName, null)).thenReturn(wanMergePolicy);
        when(wanReplicationConfig.getConsumerConfig()).thenReturn(wanConsumerConfig);
        when(wanConsumerConfig.isPersistWanReplicatedData()).thenReturn(true);

        mapWanContext.start();

        assertEquals(delegatingWanScheme, mapWanContext.getWanReplicationDelegate());
        assertEquals(wanMergePolicy, mapWanContext.wanMergePolicy);
        assertTrue(mapWanContext.isPersistWanReplicatedData());
        assertTrue(mapWanContext.isWanReplicationEnabled());
    }

    @Test
    public void testWanReplicationStartReturnsWhenMapConfigHasNullReplicationRef() {
        when(mapConfig.getWanReplicationRef()).thenReturn(null);

        mapWanContext.start();
        verify(wanReplicationService, Mockito.never()).hasWanReplicationScheme(anyString());
        verify(wanReplicationService, Mockito.never()).getWanReplicationPublishers(anyString());
        verify(splitBrainMergePolicyProvider, Mockito.never()).getMergePolicy(anyString(), eq(null));
        verify(config, Mockito.never()).getWanReplicationConfig(anyString());
        verify(wanReplicationConfig, Mockito.never()).getConsumerConfig();
        verify(wanConsumerConfig, Mockito.never()).isPersistWanReplicatedData();

        assertFalse(mapWanContext.isWanReplicationEnabled());
        assertNull(mapWanContext.getWanReplicationDelegate());
        assertNull(mapWanContext.wanMergePolicy);
        assertFalse(mapWanContext.isPersistWanReplicatedData());
    }

    @Test
    public void testWanReplicationRestartWhenNewDelegate() {
        when(wanReplicationService.hasWanReplicationScheme(wanReplicationRefName)).thenReturn(true);
        when(wanReplicationService.getWanReplicationPublishers(wanReplicationRefName)).thenReturn(delegatingWanScheme);
        when(splitBrainMergePolicyProvider.getMergePolicy(mergePolicyClassName, null)).thenReturn(wanMergePolicy);
        when(wanReplicationConfig.getConsumerConfig()).thenReturn(wanConsumerConfig);
        when(wanConsumerConfig.isPersistWanReplicatedData()).thenReturn(true);

        mapWanContext.start();

        assertEquals(delegatingWanScheme, mapWanContext.getWanReplicationDelegate());

        DelegatingWanScheme newScheme = mock(DelegatingWanScheme.class);
        when(wanReplicationService.getWanReplicationPublishers(wanReplicationRefName)).thenReturn(newScheme);
        mapWanContext.start();

        assertEquals(newScheme, mapWanContext.getWanReplicationDelegate());
        assertTrue(mapWanContext.isWanReplicationEnabled());
    }

    @Test
    public void testWanReplicationRestartWhenNoReplicationSchemeFound() {
        when(wanReplicationService.hasWanReplicationScheme(wanReplicationRefName)).thenReturn(true);
        when(wanReplicationService.getWanReplicationPublishers(wanReplicationRefName)).thenReturn(delegatingWanScheme);
        when(splitBrainMergePolicyProvider.getMergePolicy(mergePolicyClassName, null)).thenReturn(wanMergePolicy);
        when(wanReplicationConfig.getConsumerConfig()).thenReturn(wanConsumerConfig);
        when(wanConsumerConfig.isPersistWanReplicatedData()).thenReturn(true);

        mapWanContext.start();
        assertEquals(delegatingWanScheme, mapWanContext.getWanReplicationDelegate());

        when(wanReplicationService.hasWanReplicationScheme(wanReplicationRefName)).thenReturn(false);
        mapWanContext.start();

        assertNull(mapWanContext.getWanReplicationDelegate());
        assertEquals(wanMergePolicy, mapWanContext.wanMergePolicy);
        assertFalse(mapWanContext.isWanReplicationEnabled());
        assertFalse(mapWanContext.isWanRepublishingEnabled());
    }

    @Test
    public void testMergePersistenceValueWhenRestartingWithWanConsumerConfigChange() {
        when(wanReplicationService.hasWanReplicationScheme(wanReplicationRefName)).thenReturn(true);
        when(splitBrainMergePolicyProvider.getMergePolicy(mergePolicyClassName, null)).thenReturn(wanMergePolicy);
        when(wanReplicationConfig.getConsumerConfig()).thenReturn(wanConsumerConfig);
        when(wanConsumerConfig.isPersistWanReplicatedData()).thenReturn(true);

        mapWanContext.start();

        assertTrue(mapWanContext.isPersistWanReplicatedData());
        when(wanConsumerConfig.isPersistWanReplicatedData()).thenReturn(false);

        mapWanContext.start();
        assertFalse(mapWanContext.isPersistWanReplicatedData());
    }

    @Test
    public void testMergePolicyUpdatesWhenRestartingWithProviderChanges() {
        when(wanReplicationService.hasWanReplicationScheme(wanReplicationRefName)).thenReturn(true);
        when(splitBrainMergePolicyProvider.getMergePolicy(mergePolicyClassName, null)).thenReturn(wanMergePolicy);
        when(wanReplicationConfig.getConsumerConfig()).thenReturn(wanConsumerConfig);
        when(wanConsumerConfig.isPersistWanReplicatedData()).thenReturn(true);

        mapWanContext.start();
        assertEquals(wanMergePolicy, mapWanContext.wanMergePolicy);

        SplitBrainMergePolicy newMergePolicy = mock(SplitBrainMergePolicy.class);
        when(splitBrainMergePolicyProvider.getMergePolicy(mergePolicyClassName, null)).thenReturn(newMergePolicy);
        mapWanContext.start();
        assertEquals(newMergePolicy, mapWanContext.wanMergePolicy);
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testThrowsWhenMerkleTreeNotConfiguredWithPublisherWithMerkleTreeSync() {
        WanBatchPublisherConfig batchPublisherConfig = mock(WanBatchPublisherConfig.class);
        WanSyncConfig syncConfig = mock(WanSyncConfig.class);
        when(batchPublisherConfig.getSyncConfig()).thenReturn(syncConfig);
        when(syncConfig.getConsistencyCheckStrategy()).thenReturn(ConsistencyCheckStrategy.MERKLE_TREES);
        List<WanBatchPublisherConfig> publishers = List.of(batchPublisherConfig);

        when(merkleTreeConfig.getEnabled()).thenReturn(false);
        when(wanReplicationConfig.getBatchPublisherConfigs()).thenReturn(publishers);

        mapWanContext.start();
    }



    @Test
    public void testIsWanReplicationEnabledFalseWhenMergePolicyNull() {
        when(wanReplicationService.hasWanReplicationScheme(wanReplicationRefName)).thenReturn(true);
        when(wanReplicationConfig.getConsumerConfig()).thenReturn(wanConsumerConfig);
        when(wanConsumerConfig.isPersistWanReplicatedData()).thenReturn(true);

        when(splitBrainMergePolicyProvider.getMergePolicy(anyString(), eq(null))).thenReturn(null);

        mapWanContext.start();
        assertFalse(mapWanContext.isWanReplicationEnabled());
    }

    @Test
    public void testIsWanRepublishingEnabledFalseWhenDisabledInMapConfig() {
        when(wanReplicationService.hasWanReplicationScheme(wanReplicationRefName)).thenReturn(true);
        when(splitBrainMergePolicyProvider.getMergePolicy(mergePolicyClassName, null)).thenReturn(wanMergePolicy);
        when(wanReplicationConfig.getConsumerConfig()).thenReturn(wanConsumerConfig);
        when(wanConsumerConfig.isPersistWanReplicatedData()).thenReturn(true);
        when(wanReplicationRef.isRepublishingEnabled()).thenReturn(false);


        mapWanContext.start();

        assertFalse(mapWanContext.isWanRepublishingEnabled());
    }
}
