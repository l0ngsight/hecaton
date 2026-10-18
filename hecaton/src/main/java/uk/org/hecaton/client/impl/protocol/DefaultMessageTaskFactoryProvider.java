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

package uk.org.hecaton.client.impl.protocol;

import uk.org.hecaton.client.impl.protocol.codec.CacheAddEntryListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheAddNearCacheInvalidationListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheAddPartitionLostListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheClearCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheContainsKeyCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheCreateConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheDestroyCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheEntryProcessorCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheEventJournalReadCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheEventJournalSubscribeCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheFetchNearCacheInvalidationMetadataCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheGetAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheGetAndRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheGetAndReplaceCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheGetCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheGetConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheIterateCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheIterateEntriesCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheListenerRegistrationCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheLoadAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheManagementConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.CachePutAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.CachePutCodec;
import uk.org.hecaton.client.impl.protocol.codec.CachePutIfAbsentCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheRemoveAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheRemoveAllKeysCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheRemoveEntryListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheRemoveInvalidationListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheRemovePartitionLostListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheReplaceCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheSetExpiryPolicyCodec;
import uk.org.hecaton.client.impl.protocol.codec.CacheSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.CardinalityEstimatorAddCodec;
import uk.org.hecaton.client.impl.protocol.codec.CardinalityEstimatorEstimateCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientAddClusterViewListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientAddDistributedObjectListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientAddMigrationListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientAddPartitionLostListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientAuthenticationCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientAuthenticationCustomCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientCreateProxiesCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientCreateProxyCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientDeployClassesCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientDestroyProxyCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientFetchSchemaCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientGetDistributedObjectsCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientLocalBackupListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientPingCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientRemoveDistributedObjectListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientRemoveMigrationListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientRemovePartitionLostListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientSendAllSchemasCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientSendSchemaCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientStatisticsCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientTpcAuthenticationCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientTriggerPartitionAssignmentCodec;
import uk.org.hecaton.client.impl.protocol.codec.ContinuousQueryAddListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ContinuousQueryDestroyCacheCodec;
import uk.org.hecaton.client.impl.protocol.codec.ContinuousQueryMadePublishableCodec;
import uk.org.hecaton.client.impl.protocol.codec.ContinuousQueryPublisherCreateCodec;
import uk.org.hecaton.client.impl.protocol.codec.ContinuousQueryPublisherCreateWithValueCodec;
import uk.org.hecaton.client.impl.protocol.codec.ContinuousQuerySetReadCursorCodec;
import uk.org.hecaton.client.impl.protocol.codec.DurableExecutorDisposeResultCodec;
import uk.org.hecaton.client.impl.protocol.codec.DurableExecutorIsShutdownCodec;
import uk.org.hecaton.client.impl.protocol.codec.DurableExecutorRetrieveAndDisposeResultCodec;
import uk.org.hecaton.client.impl.protocol.codec.DurableExecutorRetrieveResultCodec;
import uk.org.hecaton.client.impl.protocol.codec.DurableExecutorShutdownCodec;
import uk.org.hecaton.client.impl.protocol.codec.DurableExecutorSubmitToPartitionCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddCacheConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddCardinalityEstimatorConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddDataConnectionConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddDurableExecutorConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddExecutorConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddFlakeIdGeneratorConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddListConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddMapConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddMultiMapConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddPNCounterConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddQueueConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddReliableTopicConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddReplicatedMapConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddRingbufferConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddScheduledExecutorConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddSetConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddTopicConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddUserCodeNamespaceConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.DynamicConfigAddWanReplicationConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.ExecutorServiceCancelOnMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.ExecutorServiceCancelOnPartitionCodec;
import uk.org.hecaton.client.impl.protocol.codec.ExecutorServiceIsShutdownCodec;
import uk.org.hecaton.client.impl.protocol.codec.ExecutorServiceShutdownCodec;
import uk.org.hecaton.client.impl.protocol.codec.ExecutorServiceSubmitToMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.ExecutorServiceSubmitToPartitionCodec;
import uk.org.hecaton.client.impl.protocol.codec.FlakeIdGeneratorNewIdBatchCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListAddAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListAddAllWithIndexCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListAddCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListAddListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListAddWithIndexCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListClearCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListCompareAndRemoveAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListCompareAndRetainAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListContainsAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListContainsCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListGetAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListGetCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListIndexOfCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListIsEmptyCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListIteratorCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListLastIndexOfCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListListIteratorCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListRemoveListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListRemoveWithIndexCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListSetCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.ListSubCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCApplyMCConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCChangeClusterStateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCChangeClusterVersionCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCDemoteDataMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetClusterMetadataCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetDiagnosticsConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetMapConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetMemberConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetSystemPropertiesCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetThreadDumpCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCGetTimedMemberStateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCInterruptHotRestartBackupCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCMatchMCConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCPollMCEventsCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCPromoteLiteMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCReadMetricsCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCReloadConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCResetQueueAgeStatisticsCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCRunConsoleCommandCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCRunGcCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCRunScriptCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCSetDiagnosticsConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCShutdownClusterCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCShutdownMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCTriggerForceStartCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCTriggerHotRestartBackupCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCTriggerPartialStartCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCUpdateConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCUpdateMapConfigCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapAddEntryListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapAddEntryListenerToKeyCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapAddEntryListenerToKeyWithPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapAddEntryListenerWithPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapAddIndexCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapAddInterceptorCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapAddPartitionLostListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapAggregateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapAggregateWithPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapClearCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapContainsKeyCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapContainsValueCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapDeleteCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapEntriesWithPagingPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapEntriesWithPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapEntrySetCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapEventJournalReadCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapEventJournalSubscribeCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapEvictAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapEvictCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapExecuteOnAllKeysCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapExecuteOnKeyCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapExecuteOnKeysCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapExecuteWithPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapFetchEntriesCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapFetchKeysCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapFetchNearCacheInvalidationMetadataCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapFetchWithQueryCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapFlushCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapForceUnlockCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapGetAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapGetCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapGetEntryViewCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapIsEmptyCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapIsLockedCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapKeySetCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapKeySetWithPagingPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapKeySetWithPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapLoadAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapLoadGivenKeysCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapLockCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapProjectCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapProjectWithPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapPutAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapPutAllWithMetadataCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapPutCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapPutIfAbsentCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapPutIfAbsentWithMaxIdleCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapPutTransientCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapPutTransientWithMaxIdleCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapPutWithMaxIdleCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapRemoveAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapRemoveEntryListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapRemoveIfSameCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapRemoveInterceptorCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapRemovePartitionLostListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapReplaceAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapReplaceCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapReplaceIfSameCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapSetCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapSetTtlCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapSetWithMaxIdleCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapSubmitToKeyCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapTryLockCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapTryPutCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapTryRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapUnlockCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapValuesCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapValuesWithPagingPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapValuesWithPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapAddEntryListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapAddEntryListenerToKeyCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapClearCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapContainsEntryCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapContainsKeyCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapContainsValueCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapDeleteCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapEntrySetCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapForceUnlockCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapGetCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapIsLockedCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapKeySetCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapLockCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapPutAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapPutCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapRemoveEntryCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapRemoveEntryListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapTryLockCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapUnlockCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapValueCountCodec;
import uk.org.hecaton.client.impl.protocol.codec.MultiMapValuesCodec;
import uk.org.hecaton.client.impl.protocol.codec.PNCounterAddCodec;
import uk.org.hecaton.client.impl.protocol.codec.PNCounterGetCodec;
import uk.org.hecaton.client.impl.protocol.codec.PNCounterGetConfiguredReplicaCountCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueAddAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueAddListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueClearCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueCompareAndRemoveAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueCompareAndRetainAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueContainsAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueContainsCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueDrainToCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueDrainToMaxSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueIsEmptyCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueIteratorCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueOfferCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueuePeekCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueuePollCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueuePutCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueRemainingCapacityCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueRemoveListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.QueueTakeCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapAddEntryListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapAddEntryListenerToKeyCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapAddEntryListenerToKeyWithPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapAddEntryListenerWithPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapAddNearCacheEntryListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapClearCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapContainsKeyCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapContainsValueCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapEndEntryViewIterationCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapEntrySetCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapFetchEntryViewsCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapGetCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapIsEmptyCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapKeySetCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapPutAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapPutAllWithMetadataCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapPutCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapRemoveEntryListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.ReplicatedMapValuesCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferAddAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferAddCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferCapacityCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferHeadSequenceCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferReadManyCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferReadOneCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferRemainingCapacityCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferTailSequenceCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorCancelFromMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorCancelFromPartitionCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorDisposeFromMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorDisposeFromPartitionCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorGetAllScheduledFuturesCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorGetDelayFromMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorGetDelayFromPartitionCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorGetResultFromMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorGetResultFromPartitionCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorGetStatsFromMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorGetStatsFromPartitionCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorIsCancelledFromMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorIsCancelledFromPartitionCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorIsDoneFromMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorIsDoneFromPartitionCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorShutdownCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorSubmitToMemberCodec;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorSubmitToPartitionCodec;
import uk.org.hecaton.client.impl.protocol.codec.SetAddAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.SetAddCodec;
import uk.org.hecaton.client.impl.protocol.codec.SetAddListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.SetClearCodec;
import uk.org.hecaton.client.impl.protocol.codec.SetCompareAndRemoveAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.SetCompareAndRetainAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.SetContainsAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.SetContainsCodec;
import uk.org.hecaton.client.impl.protocol.codec.SetGetAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.SetIsEmptyCodec;
import uk.org.hecaton.client.impl.protocol.codec.SetRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.SetRemoveListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.SetSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.SqlCloseCodec;
import uk.org.hecaton.client.impl.protocol.codec.SqlExecuteCodec;
import uk.org.hecaton.client.impl.protocol.codec.SqlFetchCodec;
import uk.org.hecaton.client.impl.protocol.codec.SqlMappingDdlCodec;
import uk.org.hecaton.client.impl.protocol.codec.TopicAddMessageListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.TopicPublishAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.TopicPublishCodec;
import uk.org.hecaton.client.impl.protocol.codec.TopicRemoveMessageListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionCommitCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionCreateCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionRollbackCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalListAddCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalListRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalListSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapContainsKeyCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapDeleteCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapGetCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapGetForUpdateCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapIsEmptyCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapKeySetCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapKeySetWithPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapPutCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapPutIfAbsentCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapRemoveIfSameCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapReplaceCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapReplaceIfSameCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapSetCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapValuesCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMapValuesWithPredicateCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMultiMapGetCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMultiMapPutCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMultiMapRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMultiMapRemoveEntryCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMultiMapSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalMultiMapValueCountCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalQueueOfferCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalQueuePeekCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalQueuePollCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalQueueSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalQueueTakeCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalSetAddCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalSetRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalSetSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.XATransactionClearRemoteCodec;
import uk.org.hecaton.client.impl.protocol.codec.XATransactionCollectTransactionsCodec;
import uk.org.hecaton.client.impl.protocol.codec.XATransactionCommitCodec;
import uk.org.hecaton.client.impl.protocol.codec.XATransactionCreateCodec;
import uk.org.hecaton.client.impl.protocol.codec.XATransactionFinalizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.XATransactionPrepareCodec;
import uk.org.hecaton.client.impl.protocol.codec.XATransactionRollbackCodec;
import uk.org.hecaton.client.impl.protocol.task.AddBackupListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.AddClusterViewListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.AddDistributedObjectListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.AddMigrationListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.AddPartitionLostListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.AuthenticationCustomCredentialsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.AuthenticationMessageTask;
import uk.org.hecaton.client.impl.protocol.task.ClientStatisticsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.ClientTpcAuthenticationMessageTask;
import uk.org.hecaton.client.impl.protocol.task.CreateProxiesMessageTask;
import uk.org.hecaton.client.impl.protocol.task.CreateProxyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.DeployClassesMessageTask;
import uk.org.hecaton.client.impl.protocol.task.DestroyProxyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.GetDistributedObjectsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.PingMessageTask;
import uk.org.hecaton.client.impl.protocol.task.RemoveDistributedObjectListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.RemoveMigrationListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.RemovePartitionLostListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.TriggerPartitionAssignmentMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheAddEntryListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheAddNearCacheInvalidationListenerTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheAddPartitionLostListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheClearMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheContainsKeyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheCreateConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheDestroyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheEntryProcessorMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheEventJournalReadTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheEventJournalSubscribeTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheFetchNearCacheInvalidationMetadataTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheGetAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheGetAndRemoveMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheGetAndReplaceMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheGetConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheGetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheIterateEntriesMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheIterateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheListenerRegistrationMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheLoadAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheManagementConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CachePutAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CachePutIfAbsentMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CachePutMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheRemoveAllKeysMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheRemoveAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheRemoveEntryListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheRemoveInvalidationListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheRemoveMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheRemovePartitionLostListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheReplaceMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheSetExpiryPolicyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheSizeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cardinality.CardinalityEstimatorAddMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cardinality.CardinalityEstimatorEstimateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.crdt.pncounter.PNCounterAddMessageTask;
import uk.org.hecaton.client.impl.protocol.task.crdt.pncounter.PNCounterGetConfiguredReplicaCountMessageTask;
import uk.org.hecaton.client.impl.protocol.task.crdt.pncounter.PNCounterGetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddCacheConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddCardinalityEstimatorConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddDataConnectionConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.GetDiagnosticsConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.SetDiagnosticsConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddDurableExecutorConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddExecutorConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddFlakeIdGeneratorConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddListConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddMapConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddMultiMapConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddPNCounterConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddQueueConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddReliableTopicConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddReplicatedMapConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddRingbufferConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddScheduledExecutorConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddSetConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddTopicConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddUserCodeNamespaceConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.dynamicconfig.AddWanReplicationConfigTask;
import uk.org.hecaton.client.impl.protocol.task.executorservice.ExecutorServiceCancelOnAddressMessageTask;
import uk.org.hecaton.client.impl.protocol.task.executorservice.ExecutorServiceCancelOnPartitionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.executorservice.ExecutorServiceIsShutdownMessageTask;
import uk.org.hecaton.client.impl.protocol.task.executorservice.ExecutorServiceShutdownMessageTask;
import uk.org.hecaton.client.impl.protocol.task.executorservice.ExecutorServiceSubmitToAddressMessageTask;
import uk.org.hecaton.client.impl.protocol.task.executorservice.ExecutorServiceSubmitToPartitionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.executorservice.durable.DurableExecutorDisposeResultMessageTask;
import uk.org.hecaton.client.impl.protocol.task.executorservice.durable.DurableExecutorIsShutdownMessageTask;
import uk.org.hecaton.client.impl.protocol.task.executorservice.durable.DurableExecutorRetrieveAndDisposeResultMessageTask;
import uk.org.hecaton.client.impl.protocol.task.executorservice.durable.DurableExecutorRetrieveResultMessageTask;
import uk.org.hecaton.client.impl.protocol.task.executorservice.durable.DurableExecutorShutdownMessageTask;
import uk.org.hecaton.client.impl.protocol.task.executorservice.durable.DurableExecutorSubmitToPartitionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListAddAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListAddAllWithIndexMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListAddListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListAddMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListAddWithIndexMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListClearMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListCompareAndRemoveAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListCompareAndRetainAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListContainsAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListContainsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListGetAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListGetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListIndexOfMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListIsEmptyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListIteratorMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListLastIndexOfMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListListIteratorMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListRemoveListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListRemoveMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListRemoveWithIndexMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListSetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListSizeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.list.ListSubMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.AddWanBatchPublisherConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.ApplyClientFilteringConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.ChangeClusterStateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.ChangeClusterVersionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.ChangeWanReplicationStateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.CheckWanConsistencyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.ClearWanQueuesMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.DemoteDataMemberMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.GetClusterMetadataMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.GetMapConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.GetMemberConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.GetSystemPropertiesMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.GetThreadDumpMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.GetTimedMemberStateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.HotRestartInterruptBackupMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.HotRestartTriggerBackupMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.HotRestartTriggerForceStartMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.HotRestartTriggerPartialStartMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.MatchClientFilteringConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.PollMCEventsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.PromoteLiteMemberMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.QueueResetAgeStatisticsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.ReloadConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.RunConsoleCommandMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.RunGcMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.RunScriptMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.ShutdownClusterMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.ShutdownMemberMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.UpdateConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.UpdateMapConfigMessageTask;
import uk.org.hecaton.client.impl.protocol.task.management.WanSyncMapMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapAddEntryListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapAddEntryListenerToKeyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapAddEntryListenerToKeyWithPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapAddEntryListenerWithPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapAddIndexMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapAddInterceptorMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapAddListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapAddNearCacheInvalidationListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapAddPartitionLostListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapAggregateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapAggregateWithPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapClearMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapContainsKeyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapContainsValueMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapDeleteMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapDestroyCacheMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapEntriesWithPagingPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapEntriesWithPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapEntrySetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapEventJournalReadTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapEventJournalSubscribeTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapEvictAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapEvictMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapExecuteOnAllKeysMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapExecuteOnKeyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapExecuteOnKeysMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapExecuteWithPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapFetchEntriesMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapFetchKeysMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapFetchNearCacheInvalidationMetadataTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapFetchWithQueryMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapFlushMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapForceUnlockMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapGetAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapGetEntryViewMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapGetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapIsEmptyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapIsLockedMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapKeySetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapKeySetWithPagingPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapKeySetWithPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapLoadAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapLoadGivenKeysMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapLockMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapMadePublishableMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapProjectionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapProjectionWithPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapPublisherCreateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapPublisherCreateWithValueMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapPutAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapPutAllWithMetadataMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapPutIfAbsentMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapPutIfAbsentWithMaxIdleMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapPutMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapPutTransientMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapPutTransientWithMaxIdleMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapPutWithMaxIdleMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapRemoveAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapRemoveEntryListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapRemoveIfSameMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapRemoveInterceptorMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapRemoveMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapRemovePartitionLostListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapReplaceAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapReplaceIfSameMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapReplaceMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapSetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapSetReadCursorMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapSetTtlMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapSetWithMaxIdleMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapSizeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapSubmitToKeyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapTryLockMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapTryPutMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapTryRemoveMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapUnlockMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapValuesMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapValuesWithPagingPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapValuesWithPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.metrics.ReadMetricsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapAddEntryListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapAddEntryListenerToKeyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapClearMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapContainsEntryMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapContainsKeyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapContainsValueMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapDeleteMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapEntrySetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapForceUnlockMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapGetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapIsLockedMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapKeySetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapLockMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapPutAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapPutMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapRemoveEntryListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapRemoveEntryMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapRemoveMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapSizeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapTryLockMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapUnlockMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapValueCountMessageTask;
import uk.org.hecaton.client.impl.protocol.task.multimap.MultiMapValuesMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueAddAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueAddListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueClearMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueCompareAndRemoveAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueCompareAndRetainAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueContainsAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueContainsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueDrainMaxSizeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueDrainMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueIsEmptyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueIteratorMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueOfferMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueuePeekMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueuePollMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueuePutMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueRemainingCapacityMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueRemoveListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueRemoveMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueSizeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.queue.QueueTakeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerToKeyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerToKeyWithPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapAddEntryListenerWithPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapAddNearCacheListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapClearMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapContainsKeyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapContainsValueMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapEndEntryViewIterationMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapEntrySetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapFetchEntryViewsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapGetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapIsEmptyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapKeySetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapPutAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapPutAllWithMetadataMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapPutMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapRemoveEntryListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapRemoveMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapSizeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.replicatedmap.ReplicatedMapValuesMessageTask;
import uk.org.hecaton.client.impl.protocol.task.ringbuffer.RingbufferAddAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.ringbuffer.RingbufferAddMessageTask;
import uk.org.hecaton.client.impl.protocol.task.ringbuffer.RingbufferCapacityMessageTask;
import uk.org.hecaton.client.impl.protocol.task.ringbuffer.RingbufferHeadSequenceMessageTask;
import uk.org.hecaton.client.impl.protocol.task.ringbuffer.RingbufferReadManyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.ringbuffer.RingbufferReadOneMessageTask;
import uk.org.hecaton.client.impl.protocol.task.ringbuffer.RingbufferRemainingCapacityMessageTask;
import uk.org.hecaton.client.impl.protocol.task.ringbuffer.RingbufferSizeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.ringbuffer.RingbufferTailSequenceMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorGetAllScheduledMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorShutdownMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorSubmitToPartitionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorSubmitToTargetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskCancelFromPartitionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskCancelFromTargetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskDisposeFromPartitionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskDisposeFromTargetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetDelayFromPartitionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetDelayFromTargetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetResultFromPartitionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetResultFromTargetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetStatisticsFromPartitionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskGetStatisticsFromTargetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskIsCancelledFromPartitionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskIsCancelledFromTargetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskIsDoneFromPartitionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.scheduledexecutor.ScheduledExecutorTaskIsDoneFromTargetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.schema.FetchSchemaMessageTask;
import uk.org.hecaton.client.impl.protocol.task.schema.SendAllSchemasMessageTask;
import uk.org.hecaton.client.impl.protocol.task.schema.SendSchemaMessageTask;
import uk.org.hecaton.client.impl.protocol.task.set.SetAddAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.set.SetAddListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.set.SetAddMessageTask;
import uk.org.hecaton.client.impl.protocol.task.set.SetClearMessageTask;
import uk.org.hecaton.client.impl.protocol.task.set.SetCompareAndRemoveAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.set.SetCompareAndRetainAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.set.SetContainsAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.set.SetContainsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.set.SetGetAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.set.SetIsEmptyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.set.SetRemoveListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.set.SetRemoveMessageTask;
import uk.org.hecaton.client.impl.protocol.task.set.SetSizeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.topic.TopicAddMessageListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.topic.TopicPublishAllMessageTask;
import uk.org.hecaton.client.impl.protocol.task.topic.TopicPublishMessageTask;
import uk.org.hecaton.client.impl.protocol.task.topic.TopicRemoveMessageListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transaction.TransactionCommitMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transaction.TransactionCreateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transaction.TransactionRollbackMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transaction.XAClearRemoteTransactionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transaction.XACollectTransactionsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transaction.XAFinalizeTransactionMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transaction.XATransactionCommitMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transaction.XATransactionCreateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transaction.XATransactionPrepareMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transaction.XATransactionRollbackMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionallist.TransactionalListAddMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionallist.TransactionalListRemoveMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionallist.TransactionalListSizeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapContainsKeyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapDeleteMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapGetForUpdateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapGetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapIsEmptyMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapKeySetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapKeySetWithPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapPutIfAbsentMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapPutMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapRemoveIfSameMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapRemoveMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapReplaceIfSameMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapReplaceMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapSetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapSizeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapValuesMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmap.TransactionalMapValuesWithPredicateMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapGetMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapPutMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapRemoveEntryMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapRemoveMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapSizeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalmultimap.TransactionalMultiMapValueCountMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalqueue.TransactionalQueueOfferMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalqueue.TransactionalQueuePeekMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalqueue.TransactionalQueuePollMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalqueue.TransactionalQueueSizeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalqueue.TransactionalQueueTakeMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalset.TransactionalSetAddMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalset.TransactionalSetRemoveMessageTask;
import uk.org.hecaton.client.impl.protocol.task.transactionalset.TransactionalSetSizeMessageTask;
import uk.org.hecaton.flakeidgen.impl.client.NewIdBatchMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterAddAndGetCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterDecrementAndGetCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterGetAndAddCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterGetAndIncrementCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterGetAndSetCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterGetCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterIncrementAndGetCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterSetCodec;
import uk.org.hecaton.internal.longregister.client.task.LongRegisterAddAndGetMessageTask;
import uk.org.hecaton.internal.longregister.client.task.LongRegisterDecrementAndGetMessageTask;
import uk.org.hecaton.internal.longregister.client.task.LongRegisterGetAndAddMessageTask;
import uk.org.hecaton.internal.longregister.client.task.LongRegisterGetAndIncrementMessageTask;
import uk.org.hecaton.internal.longregister.client.task.LongRegisterGetAndSetMessageTask;
import uk.org.hecaton.internal.longregister.client.task.LongRegisterGetMessageTask;
import uk.org.hecaton.internal.longregister.client.task.LongRegisterIncrementAndGetMessageTask;
import uk.org.hecaton.internal.longregister.client.task.LongRegisterSetMessageTask;
import uk.org.hecaton.internal.util.collection.Int2ObjectHashMap;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.sql.impl.client.SqlCloseMessageTask;
import uk.org.hecaton.sql.impl.client.SqlExecuteMessageTask;
import uk.org.hecaton.sql.impl.client.SqlFetchMessageTask;
import uk.org.hecaton.sql.impl.client.SqlMappingDdlTask;

import static uk.org.hecaton.internal.util.MapUtil.createInt2ObjectHashMap;

@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class DefaultMessageTaskFactoryProvider implements MessageTaskFactoryProvider {
    private static final int MESSAGE_TASK_PROVIDER_INITIAL_CAPACITY = 500;

    private final Int2ObjectHashMap<MessageTaskFactory> factories;

    private final Node node;

    public DefaultMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = nodeEngine.getNode();
        this.factories = createInt2ObjectHashMap(MESSAGE_TASK_PROVIDER_INITIAL_CAPACITY);
        initFactories();
    }

    public void initFactories() {
        initializeSetTaskFactories();
        initializeRingBufferTaskFactories();
        initializeCacheTaskFactories();
        initializeReplicatedMapTaskFactories();
        initializeLongRegisterClientTaskFactories();
        initializeTransactionalListTaskFactories();
        initializeTransactionalMultiMapTaskFactories();
        initializeListTaskFactories();
        initializeTransactionalQueueTaskFactories();
        initializeMultiMapTaskFactories();
        initializeTopicTaskFactories();
        initializeTransactionalMapTaskFactories();
        initializeExecutorServiceTaskFactories();
        initializeDurableExecutorTaskFactories();
        initializeTransactionTaskFactories();
        initializeTransactionalSetTaskFactories();
        initializeMapTaskFactories();
        initializeGeneralTaskFactories();
        initializeQueueTaskFactories();
        initializeCardinalityTaskFactories();
        initializeScheduledExecutorTaskFactories();
        initializeContinuousMapQueryOperations();
        initializeDynamicConfigTaskFactories();
        initializeFlakeIdGeneratorTaskFactories();
        initializePnCounterTaskFactories();
        initializeManagementCenterTaskFactories();
        initializeSqlTaskFactories();
        initializeSchemaFactories();
    }

    private void initializeSetTaskFactories() {
        factories.put(SetRemoveListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetRemoveListenerMessageTask(cm, node, con));
        factories.put(SetClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetClearMessageTask(cm, node, con));
        factories.put(SetCompareAndRemoveAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetCompareAndRemoveAllMessageTask(cm, node, con));
        factories.put(SetContainsAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetContainsAllMessageTask(cm, node, con));
        factories.put(SetIsEmptyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetIsEmptyMessageTask(cm, node, con));
        factories.put(SetAddAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetAddAllMessageTask(cm, node, con));
        factories.put(SetAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetAddMessageTask(cm, node, con));
        factories.put(SetCompareAndRetainAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetCompareAndRetainAllMessageTask(cm, node, con));
        factories.put(SetGetAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetGetAllMessageTask(cm, node, con));
        factories.put(SetRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetRemoveMessageTask(cm, node, con));
        factories.put(SetAddListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetAddListenerMessageTask(cm, node, con));
        factories.put(SetContainsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetContainsMessageTask(cm, node, con));
        factories.put(SetSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetSizeMessageTask(cm, node, con));
    }

    private void initializeRingBufferTaskFactories() {
        factories.put(RingbufferReadOneCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferReadOneMessageTask(cm, node, con));
        factories.put(RingbufferAddAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferAddAllMessageTask(cm, node, con));
        factories.put(RingbufferCapacityCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferCapacityMessageTask(cm, node, con));
        factories.put(RingbufferTailSequenceCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferTailSequenceMessageTask(cm, node, con));
        factories.put(RingbufferAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferAddMessageTask(cm, node, con));
        factories.put(RingbufferRemainingCapacityCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferRemainingCapacityMessageTask(cm, node, con));
        factories.put(RingbufferReadManyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferReadManyMessageTask(cm, node, con));
        factories.put(RingbufferHeadSequenceCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferHeadSequenceMessageTask(cm, node, con));
        factories.put(RingbufferSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RingbufferSizeMessageTask(cm, node, con));
    }

    @SuppressWarnings("MethodLength")
    private void initializeCacheTaskFactories() {
        factories.put(CacheClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheClearMessageTask(cm, node, con));
        factories.put(CacheFetchNearCacheInvalidationMetadataCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheFetchNearCacheInvalidationMetadataTask(cm, node, con));
        factories.put(CacheReplaceCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheReplaceMessageTask(cm, node, con));
        factories.put(CacheContainsKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheContainsKeyMessageTask(cm, node, con));
        factories.put(CacheCreateConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheCreateConfigMessageTask(cm, node, con));
        factories.put(CacheGetAndReplaceCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheGetAndReplaceMessageTask(cm, node, con));
        factories.put(CacheGetAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheGetAllMessageTask(cm, node, con));
        factories.put(CachePutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CachePutMessageTask(cm, node, con));
        factories.put(CacheAddNearCacheInvalidationListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheAddNearCacheInvalidationListenerTask(cm, node, con));
        factories.put(CachePutAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CachePutAllMessageTask(cm, node, con));
        factories.put(CacheSetExpiryPolicyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheSetExpiryPolicyMessageTask(cm, node, con));
        factories.put(CacheLoadAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheLoadAllMessageTask(cm, node, con));
        factories.put(CacheListenerRegistrationCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheListenerRegistrationMessageTask(cm, node, con));
        factories.put(CacheAddEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheAddEntryListenerMessageTask(cm, node, con));
        factories.put(CacheRemoveEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheRemoveEntryListenerMessageTask(cm, node, con));
        factories.put(CacheRemoveInvalidationListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheRemoveInvalidationListenerMessageTask(cm, node, con));
        factories.put(CacheDestroyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheDestroyMessageTask(cm, node, con));
        factories.put(CacheRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheRemoveMessageTask(cm, node, con));
        factories.put(CacheEntryProcessorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheEntryProcessorMessageTask(cm, node, con));
        factories.put(CacheGetAndRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheGetAndRemoveMessageTask(cm, node, con));
        factories.put(CacheManagementConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheManagementConfigMessageTask(cm, node, con));
        factories.put(CachePutIfAbsentCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CachePutIfAbsentMessageTask(cm, node, con));
        factories.put(CacheRemoveAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheRemoveAllMessageTask(cm, node, con));
        factories.put(CacheRemoveAllKeysCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheRemoveAllKeysMessageTask(cm, node, con));
        factories.put(CacheIterateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheIterateMessageTask(cm, node, con));
        factories.put(CacheAddPartitionLostListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheAddPartitionLostListenerMessageTask(cm, node, con));
        factories.put(CacheGetConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheGetConfigMessageTask(cm, node, con));
        factories.put(CacheGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheGetMessageTask(cm, node, con));
        factories.put(CacheRemovePartitionLostListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheRemovePartitionLostListenerMessageTask(cm, node, con));
        factories.put(CacheSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheSizeMessageTask(cm, node, con));
        factories.put(CacheIterateEntriesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheIterateEntriesMessageTask(cm, node, con));
        factories.put(CacheEventJournalSubscribeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheEventJournalSubscribeTask(cm, node, con));
        factories.put(CacheEventJournalReadCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CacheEventJournalReadTask<>(cm, node, con));
    }

    private void initializeReplicatedMapTaskFactories() {
        factories.put(ReplicatedMapSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapSizeMessageTask(cm, node, con));
        factories.put(ReplicatedMapRemoveEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapRemoveEntryListenerMessageTask(cm, node, con));
        factories.put(ReplicatedMapAddEntryListenerToKeyWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapAddEntryListenerToKeyWithPredicateMessageTask(cm, node, con));
        factories.put(ReplicatedMapIsEmptyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapIsEmptyMessageTask(cm, node, con));
        factories.put(ReplicatedMapPutAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapPutAllMessageTask(cm, node, con));
        factories.put(ReplicatedMapContainsKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapContainsKeyMessageTask(cm, node, con));
        factories.put(ReplicatedMapContainsValueCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapContainsValueMessageTask(cm, node, con));
        factories.put(ReplicatedMapAddNearCacheEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapAddNearCacheListenerMessageTask(cm, node, con));
        factories.put(ReplicatedMapGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapGetMessageTask(cm, node, con));
        factories.put(ReplicatedMapAddEntryListenerWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapAddEntryListenerWithPredicateMessageTask(cm, node, con));
        factories.put(ReplicatedMapAddEntryListenerToKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapAddEntryListenerToKeyMessageTask(cm, node, con));
        factories.put(ReplicatedMapRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapRemoveMessageTask(cm, node, con));
        factories.put(ReplicatedMapClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapClearMessageTask(cm, node, con));
        factories.put(ReplicatedMapValuesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapValuesMessageTask(cm, node, con));
        factories.put(ReplicatedMapEntrySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapEntrySetMessageTask(cm, node, con));
        factories.put(ReplicatedMapPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapPutMessageTask(cm, node, con));
        factories.put(ReplicatedMapAddEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapAddEntryListenerMessageTask(cm, node, con));
        factories.put(ReplicatedMapKeySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapKeySetMessageTask(cm, node, con));
        factories.put(ReplicatedMapPutAllWithMetadataCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapPutAllWithMetadataMessageTask(cm, node, con));
        factories.put(ReplicatedMapFetchEntryViewsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapFetchEntryViewsMessageTask(cm, node, con));
        factories.put(ReplicatedMapEndEntryViewIterationCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReplicatedMapEndEntryViewIterationMessageTask(cm, node, con));
    }

    private void initializeLongRegisterClientTaskFactories() {
        factories.put(LongRegisterDecrementAndGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterDecrementAndGetMessageTask(cm, node, con));
        factories.put(LongRegisterGetAndAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterGetAndAddMessageTask(cm, node, con));
        factories.put(LongRegisterAddAndGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterAddAndGetMessageTask(cm, node, con));
        factories.put(LongRegisterGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterGetMessageTask(cm, node, con));
        factories.put(LongRegisterSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterSetMessageTask(cm, node, con));
        factories.put(LongRegisterIncrementAndGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterIncrementAndGetMessageTask(cm, node, con));
        factories.put(LongRegisterGetAndSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterGetAndSetMessageTask(cm, node, con));
        factories.put(LongRegisterGetAndIncrementCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LongRegisterGetAndIncrementMessageTask(cm, node, con));
    }

    private void initializeTransactionalListTaskFactories() {
        factories.put(TransactionalListSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalListSizeMessageTask(cm, node, con));
        factories.put(TransactionalListRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalListRemoveMessageTask(cm, node, con));
        factories.put(TransactionalListAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalListAddMessageTask(cm, node, con));
    }

    private void initializeTransactionalMultiMapTaskFactories() {
        factories.put(TransactionalMultiMapPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMultiMapPutMessageTask(cm, node, con));
        factories.put(TransactionalMultiMapRemoveEntryCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMultiMapRemoveEntryMessageTask(cm, node, con));
        factories.put(TransactionalMultiMapGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMultiMapGetMessageTask(cm, node, con));
        factories.put(TransactionalMultiMapRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMultiMapRemoveMessageTask(cm, node, con));
        factories.put(TransactionalMultiMapSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMultiMapSizeMessageTask(cm, node, con));
        factories.put(TransactionalMultiMapValueCountCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMultiMapValueCountMessageTask(cm, node, con));
    }

    private void initializeListTaskFactories() {
        factories.put(ListGetAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListGetAllMessageTask(cm, node, con));
        factories.put(ListListIteratorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListListIteratorMessageTask(cm, node, con));
        factories.put(ListSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListSetMessageTask(cm, node, con));
        factories.put(ListAddAllWithIndexCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListAddAllWithIndexMessageTask(cm, node, con));
        factories.put(ListCompareAndRemoveAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListCompareAndRemoveAllMessageTask(cm, node, con));
        factories.put(ListGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListGetMessageTask(cm, node, con));
        factories.put(ListRemoveListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListRemoveListenerMessageTask(cm, node, con));
        factories.put(ListRemoveWithIndexCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListRemoveWithIndexMessageTask(cm, node, con));
        factories.put(ListAddListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListAddListenerMessageTask(cm, node, con));
        factories.put(ListIteratorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListIteratorMessageTask(cm, node, con));
        factories.put(ListClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListClearMessageTask(cm, node, con));
        factories.put(ListAddAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListAddAllMessageTask(cm, node, con));
        factories.put(ListAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListAddMessageTask(cm, node, con));
        factories.put(ListAddWithIndexCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListAddWithIndexMessageTask(cm, node, con));
        factories.put(ListLastIndexOfCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListLastIndexOfMessageTask(cm, node, con));
        factories.put(ListRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListRemoveMessageTask(cm, node, con));
        factories.put(ListSubCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListSubMessageTask(cm, node, con));
        factories.put(ListContainsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListContainsMessageTask(cm, node, con));
        factories.put(ListIndexOfCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListIndexOfMessageTask(cm, node, con));
        factories.put(ListSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListSizeMessageTask(cm, node, con));
        factories.put(ListContainsAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListContainsAllMessageTask(cm, node, con));
        factories.put(ListIsEmptyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListIsEmptyMessageTask(cm, node, con));
        factories.put(ListCompareAndRetainAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ListCompareAndRetainAllMessageTask(cm, node, con));
    }

    private void initializeTransactionalQueueTaskFactories() {
        factories.put(TransactionalQueueSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalQueueSizeMessageTask(cm, node, con));
        factories.put(TransactionalQueueOfferCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalQueueOfferMessageTask(cm, node, con));
        factories.put(TransactionalQueuePeekCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalQueuePeekMessageTask(cm, node, con));
        factories.put(TransactionalQueuePollCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalQueuePollMessageTask(cm, node, con));
        factories.put(TransactionalQueueTakeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalQueueTakeMessageTask(cm, node, con));
    }

    private void initializeMultiMapTaskFactories() {
        factories.put(MultiMapClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapClearMessageTask(cm, node, con));
        factories.put(MultiMapGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapGetMessageTask(cm, node, con));
        factories.put(MultiMapRemoveEntryCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapRemoveEntryMessageTask(cm, node, con));
        factories.put(MultiMapContainsKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapContainsKeyMessageTask(cm, node, con));
        factories.put(MultiMapSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapSizeMessageTask(cm, node, con));
        factories.put(MultiMapAddEntryListenerToKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapAddEntryListenerToKeyMessageTask(cm, node, con));
        factories.put(MultiMapAddEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapAddEntryListenerMessageTask(cm, node, con));
        factories.put(MultiMapRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapRemoveMessageTask(cm, node, con));
        factories.put(MultiMapTryLockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapTryLockMessageTask(cm, node, con));
        factories.put(MultiMapIsLockedCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapIsLockedMessageTask(cm, node, con));
        factories.put(MultiMapContainsValueCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapContainsValueMessageTask(cm, node, con));
        factories.put(MultiMapKeySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapKeySetMessageTask(cm, node, con));
        factories.put(MultiMapPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapPutMessageTask(cm, node, con));
        factories.put(MultiMapPutAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapPutAllMessageTask(cm, node, con));
        factories.put(MultiMapEntrySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapEntrySetMessageTask(cm, node, con));
        factories.put(MultiMapValueCountCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapValueCountMessageTask(cm, node, con));
        factories.put(MultiMapUnlockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapUnlockMessageTask(cm, node, con));
        factories.put(MultiMapLockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapLockMessageTask(cm, node, con));
        factories.put(MultiMapRemoveEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapRemoveEntryListenerMessageTask(cm, node, con));
        factories.put(MultiMapContainsEntryCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapContainsEntryMessageTask(cm, node, con));
        factories.put(MultiMapForceUnlockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapForceUnlockMessageTask(cm, node, con));
        factories.put(MultiMapValuesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapValuesMessageTask(cm, node, con));
        factories.put(MultiMapDeleteCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MultiMapDeleteMessageTask(cm, node, con));
    }

    private void initializeTopicTaskFactories() {
        factories.put(TopicPublishCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TopicPublishMessageTask(cm, node, con));
        factories.put(TopicPublishAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TopicPublishAllMessageTask(cm, node, con));
        factories.put(TopicAddMessageListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TopicAddMessageListenerMessageTask(cm, node, con));
        factories.put(TopicRemoveMessageListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TopicRemoveMessageListenerMessageTask(cm, node, con));
    }

    private void initializeTransactionalMapTaskFactories() {
        factories.put(TransactionalMapValuesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapValuesMessageTask(cm, node, con));
        factories.put(TransactionalMapSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapSizeMessageTask(cm, node, con));
        factories.put(TransactionalMapPutIfAbsentCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapPutIfAbsentMessageTask(cm, node, con));
        factories.put(TransactionalMapRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapRemoveMessageTask(cm, node, con));
        factories.put(TransactionalMapGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapGetMessageTask(cm, node, con));
        factories.put(TransactionalMapGetForUpdateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapGetForUpdateMessageTask(cm, node, con));
        factories.put(TransactionalMapIsEmptyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapIsEmptyMessageTask(cm, node, con));
        factories.put(TransactionalMapKeySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapKeySetMessageTask(cm, node, con));
        factories.put(TransactionalMapKeySetWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapKeySetWithPredicateMessageTask(cm, node, con));
        factories.put(TransactionalMapReplaceIfSameCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapReplaceIfSameMessageTask(cm, node, con));
        factories.put(TransactionalMapContainsKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapContainsKeyMessageTask(cm, node, con));
        factories.put(TransactionalMapRemoveIfSameCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapRemoveIfSameMessageTask(cm, node, con));
        factories.put(TransactionalMapSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapSetMessageTask(cm, node, con));
        factories.put(TransactionalMapReplaceCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapReplaceMessageTask(cm, node, con));
        factories.put(TransactionalMapPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapPutMessageTask(cm, node, con));
        factories.put(TransactionalMapDeleteCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapDeleteMessageTask(cm, node, con));
        factories.put(TransactionalMapValuesWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalMapValuesWithPredicateMessageTask(cm, node, con));
    }

    private void initializeExecutorServiceTaskFactories() {
        factories.put(ExecutorServiceCancelOnPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ExecutorServiceCancelOnPartitionMessageTask(cm, node, con));
        factories.put(ExecutorServiceSubmitToPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ExecutorServiceSubmitToPartitionMessageTask(cm, node, con));
        factories.put(ExecutorServiceCancelOnMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ExecutorServiceCancelOnAddressMessageTask(cm, node, con));
        factories.put(ExecutorServiceIsShutdownCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ExecutorServiceIsShutdownMessageTask(cm, node, con));
        factories.put(ExecutorServiceShutdownCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ExecutorServiceShutdownMessageTask(cm, node, con));
        factories.put(ExecutorServiceSubmitToMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ExecutorServiceSubmitToAddressMessageTask(cm, node, con));
    }

    private void initializeDurableExecutorTaskFactories() {
        factories.put(DurableExecutorSubmitToPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DurableExecutorSubmitToPartitionMessageTask(cm, node, con));
        factories.put(DurableExecutorIsShutdownCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DurableExecutorIsShutdownMessageTask(cm, node, con));
        factories.put(DurableExecutorShutdownCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DurableExecutorShutdownMessageTask(cm, node, con));
        factories.put(DurableExecutorRetrieveResultCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DurableExecutorRetrieveResultMessageTask(cm, node, con));
        factories.put(DurableExecutorDisposeResultCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DurableExecutorDisposeResultMessageTask(cm, node, con));
        factories.put(DurableExecutorRetrieveAndDisposeResultCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DurableExecutorRetrieveAndDisposeResultMessageTask(cm, node, con));
    }

    private void initializeTransactionTaskFactories() {
        factories.put(TransactionCreateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionCreateMessageTask(cm, node, con));
        factories.put(XATransactionClearRemoteCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XAClearRemoteTransactionMessageTask(cm, node, con));
        factories.put(XATransactionFinalizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XAFinalizeTransactionMessageTask(cm, node, con));
        factories.put(TransactionCommitCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionCommitMessageTask(cm, node, con));
        factories.put(XATransactionCollectTransactionsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XACollectTransactionsMessageTask(cm, node, con));
        factories.put(XATransactionPrepareCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XATransactionPrepareMessageTask(cm, node, con));
        factories.put(XATransactionCreateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XATransactionCreateMessageTask(cm, node, con));
        factories.put(TransactionRollbackCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionRollbackMessageTask(cm, node, con));
        factories.put(XATransactionCommitCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XATransactionCommitMessageTask(cm, node, con));
        factories.put(XATransactionRollbackCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new XATransactionRollbackMessageTask(cm, node, con));
    }

    private void initializeTransactionalSetTaskFactories() {
        factories.put(TransactionalSetSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalSetSizeMessageTask(cm, node, con));
        factories.put(TransactionalSetAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalSetAddMessageTask(cm, node, con));
        factories.put(TransactionalSetRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TransactionalSetRemoveMessageTask(cm, node, con));
    }

    @SuppressWarnings("MethodLength")
    private void initializeMapTaskFactories() {
        factories.put(MapEntriesWithPagingPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEntriesWithPagingPredicateMessageTask(cm, node, con));
        factories.put(MapAddEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddEntryListenerMessageTask(cm, node, con));
        factories.put(MapFetchNearCacheInvalidationMetadataCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapFetchNearCacheInvalidationMetadataTask(cm, node, con));
        factories.put(MapRemoveIfSameCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapRemoveIfSameMessageTask(cm, node, con));
        factories.put(MapAddInterceptorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddInterceptorMessageTask(cm, node, con));
        factories.put(MapEntriesWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEntriesWithPredicateMessageTask(cm, node, con));
        factories.put(MapPutTransientCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutTransientMessageTask(cm, node, con));
        factories.put(MapContainsValueCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapContainsValueMessageTask(cm, node, con));
        factories.put(MapIsEmptyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapIsEmptyMessageTask(cm, node, con));
        factories.put(MapReplaceCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapReplaceMessageTask(cm, node, con));
        factories.put(MapRemoveInterceptorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapRemoveInterceptorMessageTask(cm, node, con));
        factories.put(MapAddNearCacheInvalidationListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddNearCacheInvalidationListenerMessageTask(cm, node, con));
        factories.put(MapExecuteOnAllKeysCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapExecuteOnAllKeysMessageTask(cm, node, con));
        factories.put(MapFlushCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapFlushMessageTask(cm, node, con));
        factories.put(MapSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapSetMessageTask(cm, node, con));
        factories.put(MapTryLockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapTryLockMessageTask(cm, node, con));
        factories.put(MapAddEntryListenerToKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddEntryListenerToKeyMessageTask(cm, node, con));
        factories.put(MapEntrySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEntrySetMessageTask(cm, node, con));
        factories.put(MapClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapClearMessageTask(cm, node, con));
        factories.put(MapLockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapLockMessageTask(cm, node, con));
        factories.put(MapGetEntryViewCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapGetEntryViewMessageTask(cm, node, con));
        factories.put(MapRemovePartitionLostListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapRemovePartitionLostListenerMessageTask(cm, node, con));
        factories.put(MapLoadGivenKeysCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapLoadGivenKeysMessageTask(cm, node, con));
        factories.put(MapExecuteWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapExecuteWithPredicateMessageTask(cm, node, con));
        factories.put(MapRemoveAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapRemoveAllMessageTask(cm, node, con));
        factories.put(MapPutIfAbsentCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutIfAbsentMessageTask(cm, node, con));
        factories.put(MapTryRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapTryRemoveMessageTask(cm, node, con));
        factories.put(MapPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutMessageTask(cm, node, con));
        factories.put(MapUnlockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapUnlockMessageTask(cm, node, con));
        factories.put(MapSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapSizeMessageTask(cm, node, con));
        factories.put(MapValuesWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapValuesWithPredicateMessageTask(cm, node, con));
        factories.put(MapAddEntryListenerToKeyWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddEntryListenerToKeyWithPredicateMessageTask(cm, node, con));
        factories.put(MapEvictCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEvictMessageTask(cm, node, con));
        factories.put(MapGetAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapGetAllMessageTask(cm, node, con));
        factories.put(MapReplaceAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapReplaceAllMessageTask(cm, node, con));
        factories.put(MapForceUnlockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapForceUnlockMessageTask(cm, node, con));
        factories.put(MapLoadAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapLoadAllMessageTask(cm, node, con));
        factories.put(MapAddIndexCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddIndexMessageTask(cm, node, con));
        factories.put(MapExecuteOnKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapExecuteOnKeyMessageTask(cm, node, con));
        factories.put(MapKeySetWithPagingPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapKeySetWithPagingPredicateMessageTask(cm, node, con));
        factories.put(MapRemoveEntryListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapRemoveEntryListenerMessageTask(cm, node, con));
        factories.put(MapIsLockedCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapIsLockedMessageTask(cm, node, con));
        factories.put(MapEvictAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEvictAllMessageTask(cm, node, con));
        factories.put(MapSubmitToKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapSubmitToKeyMessageTask(cm, node, con));
        factories.put(MapValuesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapValuesMessageTask(cm, node, con));
        factories.put(MapAddEntryListenerWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddEntryListenerWithPredicateMessageTask(cm, node, con));
        factories.put(MapDeleteCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapDeleteMessageTask(cm, node, con));
        factories.put(MapAddPartitionLostListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddPartitionLostListenerMessageTask(cm, node, con));
        factories.put(MapPutAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutAllMessageTask(cm, node, con));
        factories.put(MapRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapRemoveMessageTask(cm, node, con));
        factories.put(MapKeySetWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapKeySetWithPredicateMessageTask(cm, node, con));
        factories.put(MapExecuteOnKeysCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapExecuteOnKeysMessageTask(cm, node, con));
        factories.put(MapReplaceIfSameCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapReplaceIfSameMessageTask(cm, node, con));
        factories.put(MapContainsKeyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapContainsKeyMessageTask(cm, node, con));
        factories.put(MapTryPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapTryPutMessageTask(cm, node, con));
        factories.put(MapValuesWithPagingPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapValuesWithPagingPredicateMessageTask(cm, node, con));
        factories.put(MapGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapGetMessageTask(cm, node, con));
        factories.put(MapKeySetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapKeySetMessageTask(cm, node, con));
        factories.put(MapFetchKeysCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapFetchKeysMessageTask(cm, node, con));
        factories.put(MapFetchEntriesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapFetchEntriesMessageTask(cm, node, con));
        factories.put(MapAggregateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAggregateMessageTask(cm, node, con));
        factories.put(MapAggregateWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAggregateWithPredicateMessageTask(cm, node, con));
        factories.put(MapProjectCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapProjectionMessageTask(cm, node, con));
        factories.put(MapProjectWithPredicateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapProjectionWithPredicateMessageTask(cm, node, con));
        factories.put(MapFetchWithQueryCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapFetchWithQueryMessageTask(cm, node, con));
        factories.put(MapEventJournalSubscribeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEventJournalSubscribeTask(cm, node, con));
        factories.put(MapEventJournalReadCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapEventJournalReadTask<>(cm, node, con));
        factories.put(MapSetTtlCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapSetTtlMessageTask(cm, node, con));
        factories.put(MapSetWithMaxIdleCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapSetWithMaxIdleMessageTask(cm, node, con));
        factories.put(MapPutWithMaxIdleCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutWithMaxIdleMessageTask(cm, node, con));
        factories.put(MapPutIfAbsentWithMaxIdleCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutIfAbsentWithMaxIdleMessageTask(cm, node, con));
        factories.put(MapPutTransientWithMaxIdleCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutTransientWithMaxIdleMessageTask(cm, node, con));
        factories.put(MapPutAllWithMetadataCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPutAllWithMetadataMessageTask(cm, node, con));
    }

    private void initializeGeneralTaskFactories() {
        factories.put(ClientAddPartitionLostListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddPartitionLostListenerMessageTask(cm, node, con));
        factories.put(ClientRemovePartitionLostListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RemovePartitionLostListenerMessageTask(cm, node, con));
        factories.put(ClientAddMigrationListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddMigrationListenerMessageTask(cm, node, con));
        factories.put(ClientRemoveMigrationListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RemoveMigrationListenerMessageTask(cm, node, con));
        factories.put(ClientCreateProxyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CreateProxyMessageTask(cm, node, con));
        factories.put(ClientGetDistributedObjectsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetDistributedObjectsMessageTask(cm, node, con));
        factories.put(ClientAddDistributedObjectListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddDistributedObjectListenerMessageTask(cm, node, con));
        factories.put(ClientDestroyProxyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DestroyProxyMessageTask(cm, node, con));
        factories.put(ClientPingCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PingMessageTask(cm, node, con));
        factories.put(ClientAddClusterViewListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddClusterViewListenerMessageTask(cm, node, con));
        factories.put(ClientAuthenticationCustomCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AuthenticationCustomCredentialsMessageTask(cm, node, con));
        factories.put(ClientRemoveDistributedObjectListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RemoveDistributedObjectListenerMessageTask(cm, node, con));
        factories.put(ClientAuthenticationCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AuthenticationMessageTask(cm, node, con));
        factories.put(ClientStatisticsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ClientStatisticsMessageTask(cm, node, con));
        factories.put(ClientDeployClassesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DeployClassesMessageTask(cm, node, con));
        factories.put(ClientCreateProxiesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CreateProxiesMessageTask(cm, node, con));
        factories.put(ClientLocalBackupListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddBackupListenerMessageTask(cm, node, con));
        factories.put(ClientTriggerPartitionAssignmentCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TriggerPartitionAssignmentMessageTask(cm, node, con));
        factories.put(ClientTpcAuthenticationCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ClientTpcAuthenticationMessageTask(cm, node, con));
    }

    private void initializeQueueTaskFactories() {
        factories.put(QueueCompareAndRemoveAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueCompareAndRemoveAllMessageTask(cm, node, con));
        factories.put(QueueContainsAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueContainsAllMessageTask(cm, node, con));
        factories.put(QueueAddAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueAddAllMessageTask(cm, node, con));
        factories.put(QueueTakeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueTakeMessageTask(cm, node, con));
        factories.put(QueueAddListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueAddListenerMessageTask(cm, node, con));
        factories.put(QueueCompareAndRetainAllCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueCompareAndRetainAllMessageTask(cm, node, con));
        factories.put(QueueOfferCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueOfferMessageTask(cm, node, con));
        factories.put(QueuePeekCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueuePeekMessageTask(cm, node, con));
        factories.put(QueueRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueRemoveMessageTask(cm, node, con));
        factories.put(QueueIsEmptyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueIsEmptyMessageTask(cm, node, con));
        factories.put(QueueIteratorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueIteratorMessageTask(cm, node, con));
        factories.put(QueueSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueSizeMessageTask(cm, node, con));
        factories.put(QueuePutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueuePutMessageTask(cm, node, con));
        factories.put(QueueContainsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueContainsMessageTask(cm, node, con));
        factories.put(QueuePollCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueuePollMessageTask(cm, node, con));
        factories.put(QueueDrainToCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueDrainMessageTask(cm, node, con));
        factories.put(QueueRemoveListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueRemoveListenerMessageTask(cm, node, con));
        factories.put(QueueRemainingCapacityCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueRemainingCapacityMessageTask(cm, node, con));
        factories.put(QueueClearCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueClearMessageTask(cm, node, con));
        factories.put(QueueDrainToMaxSizeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueDrainMaxSizeMessageTask(cm, node, con));
    }

    private void initializeCardinalityTaskFactories() {
        factories.put(CardinalityEstimatorAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CardinalityEstimatorAddMessageTask(cm, node, con));
        factories.put(CardinalityEstimatorEstimateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CardinalityEstimatorEstimateMessageTask(cm, node, con));
    }

    private void initializeScheduledExecutorTaskFactories() {
        factories.put(ScheduledExecutorSubmitToPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorSubmitToPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorSubmitToMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorSubmitToTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorShutdownCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorShutdownMessageTask(cm, node, con));
        factories.put(ScheduledExecutorDisposeFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskDisposeFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorDisposeFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskDisposeFromTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorCancelFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskCancelFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorCancelFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskCancelFromTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorIsDoneFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskIsDoneFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorIsDoneFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskIsDoneFromTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetDelayFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskGetDelayFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetDelayFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskGetDelayFromTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetStatsFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskGetStatisticsFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetStatsFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskGetStatisticsFromTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetResultFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskGetResultFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetResultFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskGetResultFromTargetMessageTask(cm, node, con));
        factories.put(ScheduledExecutorGetAllScheduledFuturesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorGetAllScheduledMessageTask(cm, node, con));
        factories.put(ScheduledExecutorIsCancelledFromPartitionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskIsCancelledFromPartitionMessageTask(cm, node, con));
        factories.put(ScheduledExecutorIsCancelledFromMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ScheduledExecutorTaskIsCancelledFromTargetMessageTask(cm, node, con));
    }

    private void initializeContinuousMapQueryOperations() {
        factories.put(ContinuousQueryDestroyCacheCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapDestroyCacheMessageTask(cm, node, con));
        factories.put(ContinuousQueryPublisherCreateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPublisherCreateMessageTask(cm, node, con));
        factories.put(ContinuousQuerySetReadCursorCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapSetReadCursorMessageTask(cm, node, con));
        factories.put(ContinuousQueryAddListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapAddListenerMessageTask(cm, node, con));
        factories.put(ContinuousQueryMadePublishableCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapMadePublishableMessageTask(cm, node, con));
        factories.put(ContinuousQueryPublisherCreateWithValueCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MapPublisherCreateWithValueMessageTask(cm, node, con));
    }

    private void initializeDynamicConfigTaskFactories() {
        factories.put(DynamicConfigAddMultiMapConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddMultiMapConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddCardinalityEstimatorConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddCardinalityEstimatorConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddExecutorConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddExecutorConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddDurableExecutorConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddDurableExecutorConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddScheduledExecutorConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddScheduledExecutorConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddRingbufferConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddRingbufferConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddListConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddListConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddSetConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddSetConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddTopicConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddTopicConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddReplicatedMapConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddReplicatedMapConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddQueueConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddQueueConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddMapConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddMapConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddReliableTopicConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddReliableTopicConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddCacheConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddCacheConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddFlakeIdGeneratorConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddFlakeIdGeneratorConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddPNCounterConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddPNCounterConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddDataConnectionConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddDataConnectionConfigMessageTask(cm, node, con));
        factories.put(DynamicConfigAddWanReplicationConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddWanReplicationConfigTask(cm, node, con));
        factories.put(DynamicConfigAddUserCodeNamespaceConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddUserCodeNamespaceConfigMessageTask(cm, node, con));
    }

    private void initializeFlakeIdGeneratorTaskFactories() {
        factories.put(FlakeIdGeneratorNewIdBatchCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new NewIdBatchMessageTask(cm, node, con));
    }

    private void initializePnCounterTaskFactories() {
        factories.put(PNCounterGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PNCounterGetMessageTask(cm, node, con));
        factories.put(PNCounterAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PNCounterAddMessageTask(cm, node, con));
        factories.put(PNCounterGetConfiguredReplicaCountCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PNCounterGetConfiguredReplicaCountMessageTask(cm, node, con));
    }

    private void initializeManagementCenterTaskFactories() {
        factories.put(MCReadMetricsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReadMetricsMessageTask(cm, node, con));
        factories.put(MCChangeClusterStateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ChangeClusterStateMessageTask(cm, node, con));
        factories.put(MCGetMapConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetMapConfigMessageTask(cm, node, con));
        factories.put(MCUpdateMapConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new UpdateMapConfigMessageTask(cm, node, con));
        factories.put(MCGetMemberConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetMemberConfigMessageTask(cm, node, con));
        factories.put(MCRunGcCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RunGcMessageTask(cm, node, con));
        factories.put(MCGetThreadDumpCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetThreadDumpMessageTask(cm, node, con));
        factories.put(MCShutdownMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ShutdownMemberMessageTask(cm, node, con));
        factories.put(MCPromoteLiteMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PromoteLiteMemberMessageTask(cm, node, con));
        factories.put(MCGetSystemPropertiesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetSystemPropertiesMessageTask(cm, node, con));
        factories.put(MCGetTimedMemberStateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetTimedMemberStateMessageTask(cm, node, con));
        factories.put(MCMatchMCConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new MatchClientFilteringConfigMessageTask(cm, node, con));
        factories.put(MCApplyMCConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ApplyClientFilteringConfigMessageTask(cm, node, con));
        factories.put(MCGetClusterMetadataCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetClusterMetadataMessageTask(cm, node, con));
        factories.put(MCShutdownClusterCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ShutdownClusterMessageTask(cm, node, con));
        factories.put(MCChangeClusterVersionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ChangeClusterVersionMessageTask(cm, node, con));
        factories.put(MCRunScriptCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RunScriptMessageTask(cm, node, con));
        factories.put(MCRunConsoleCommandCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RunConsoleCommandMessageTask(cm, node, con));
        factories.put(uk.org.hecaton.client.impl.protocol.codec.MCChangeWanReplicationStateCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ChangeWanReplicationStateMessageTask(cm, node, con));
        factories.put(uk.org.hecaton.client.impl.protocol.codec.MCClearWanQueuesCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ClearWanQueuesMessageTask(cm, node, con));
        factories.put(uk.org.hecaton.client.impl.protocol.codec.MCAddWanBatchPublisherConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddWanBatchPublisherConfigMessageTask(cm, node, con));
        factories.put(uk.org.hecaton.client.impl.protocol.codec.MCWanSyncMapCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new WanSyncMapMessageTask(cm, node, con));
        factories.put(uk.org.hecaton.client.impl.protocol.codec.MCCheckWanConsistencyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CheckWanConsistencyMessageTask(cm, node, con));
        factories.put(MCPollMCEventsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PollMCEventsMessageTask(cm, node, con));
        factories.put(MCTriggerPartialStartCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new HotRestartTriggerPartialStartMessageTask(cm, node, con));
        factories.put(MCTriggerForceStartCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new HotRestartTriggerForceStartMessageTask(cm, node, con));
        factories.put(MCTriggerHotRestartBackupCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new HotRestartTriggerBackupMessageTask(cm, node, con));
        factories.put(MCInterruptHotRestartBackupCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new HotRestartInterruptBackupMessageTask(cm, node, con));
        factories.put(MCResetQueueAgeStatisticsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new QueueResetAgeStatisticsMessageTask(cm, node, con));
        factories.put(MCReloadConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReloadConfigMessageTask(cm, node, con));
        factories.put(MCUpdateConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new UpdateConfigMessageTask(cm, node, con));
        factories.put(MCDemoteDataMemberCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DemoteDataMemberMessageTask(cm, node, con));
        factories.put(MCGetDiagnosticsConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetDiagnosticsConfigMessageTask(cm, node, con));
        factories.put(MCSetDiagnosticsConfigCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetDiagnosticsConfigMessageTask(cm, node, con));
    }

    private void initializeSqlTaskFactories() {
        factories.put(SqlExecuteCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SqlExecuteMessageTask(cm, node, con));
        factories.put(SqlFetchCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SqlFetchMessageTask(cm, node, con));
        factories.put(SqlCloseCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SqlCloseMessageTask(cm, node, con));
        factories.put(SqlMappingDdlCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SqlMappingDdlTask(cm, node, con));
    }

    private void initializeSchemaFactories() {
        factories.put(ClientSendSchemaCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SendSchemaMessageTask(cm, node, con));
        factories.put(ClientFetchSchemaCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new FetchSchemaMessageTask(cm, node, con));
        factories.put(ClientSendAllSchemasCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SendAllSchemasMessageTask(cm, node, con));
    }

    @Override
    public Int2ObjectHashMap<MessageTaskFactory> getFactories() {
        return factories;
    }
}
