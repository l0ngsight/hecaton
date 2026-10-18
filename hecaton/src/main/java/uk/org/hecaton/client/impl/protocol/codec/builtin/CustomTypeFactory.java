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

package uk.org.hecaton.client.impl.protocol.codec.builtin;

import uk.org.hecaton.cache.CacheEventType;
import uk.org.hecaton.cache.impl.CacheEventDataImpl;
import uk.org.hecaton.client.impl.protocol.codec.holder.VectorPairHolder;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.BTreeIndexConfig;
import uk.org.hecaton.config.BitmapIndexOptions;
import uk.org.hecaton.config.BitmapIndexOptions.UniqueKeyTransformation;
import uk.org.hecaton.config.CacheSimpleEntryListenerConfig;
import uk.org.hecaton.config.DataPersistenceConfig;
import uk.org.hecaton.config.DiskTierConfig;
import uk.org.hecaton.config.EventJournalConfig;
import uk.org.hecaton.config.HotRestartConfig;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.MemoryTierConfig;
import uk.org.hecaton.config.MerkleTreeConfig;
import uk.org.hecaton.config.NearCachePreloaderConfig;
import uk.org.hecaton.config.TieredStoreConfig;
import uk.org.hecaton.config.vector.Metric;
import uk.org.hecaton.config.vector.VectorIndexConfig;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonJsonValue;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.ProtocolType;
import uk.org.hecaton.internal.management.dto.ClientBwListEntryDTO;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.impl.compact.FieldDescriptor;
import uk.org.hecaton.internal.serialization.impl.compact.Schema;
import uk.org.hecaton.jet.core.JobStatus;
import uk.org.hecaton.jet.impl.JobAndSqlSummaryIds;
import uk.org.hecaton.jet.impl.SqlSummary;
import uk.org.hecaton.map.impl.SimpleEntryView;
import uk.org.hecaton.map.impl.querycache.event.DefaultQueryCacheEventData;
import uk.org.hecaton.memory.Capacity;
import uk.org.hecaton.memory.MemoryUnit;
import uk.org.hecaton.nio.serialization.FieldKind;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedMapEntryView;
import uk.org.hecaton.sql.SqlColumnMetadata;
import uk.org.hecaton.sql.SqlColumnType;
import uk.org.hecaton.vector.SearchOptions;
import uk.org.hecaton.vector.VectorValues;
import uk.org.hecaton.vector.impl.DataSearchResult;
import uk.org.hecaton.vector.impl.DataVectorDocument;
import uk.org.hecaton.version.Version;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static uk.org.hecaton.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.DurationConfig;
import static uk.org.hecaton.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig;
import static uk.org.hecaton.config.CacheSimpleConfig.ExpiryPolicyFactoryConfig.TimedExpiryPolicyFactoryConfig.ExpiryPolicyType;

@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public final class CustomTypeFactory {

    private CustomTypeFactory() {
    }

    public static Address createAddress(String host, int port) {
        return Address.createUnresolvedAddress(host, port);
    }

    public static CacheEventDataImpl createCacheEventData(String name, int cacheEventType, Data dataKey,
                                                          Data dataValue, Data dataOldValue, boolean oldValueAvailable) {
        return new CacheEventDataImpl(name, CacheEventType.getByType(cacheEventType), dataKey, dataValue,
                dataOldValue, oldValueAvailable);
    }

    public static TimedExpiryPolicyFactoryConfig createTimedExpiryPolicyFactoryConfig(int expiryPolicyType,
                                                                                      DurationConfig durationConfig) {
        return new TimedExpiryPolicyFactoryConfig(ExpiryPolicyType.getById(expiryPolicyType), durationConfig);
    }

    public static CacheSimpleEntryListenerConfig createCacheSimpleEntryListenerConfig(boolean oldValueRequired,
                                                                                      boolean synchronous,
                                                                                      String cacheEntryListenerFactory,
                                                                                      String cacheEntryEventFilterFactory) {
        CacheSimpleEntryListenerConfig config = new CacheSimpleEntryListenerConfig();
        config.setOldValueRequired(oldValueRequired);
        config.setSynchronous(synchronous);
        config.setCacheEntryListenerFactory(cacheEntryListenerFactory);
        config.setCacheEntryEventFilterFactory(cacheEntryEventFilterFactory);
        return config;
    }

    public static EventJournalConfig createEventJournalConfig(boolean enabled, int capacity, int timeToLiveSeconds) {
        EventJournalConfig config = new EventJournalConfig();
        config.setEnabled(enabled);
        config.setCapacity(capacity);
        config.setTimeToLiveSeconds(timeToLiveSeconds);
        return config;
    }

    public static HotRestartConfig createHotRestartConfig(boolean enabled, boolean fsync) {
        HotRestartConfig config = new HotRestartConfig();
        config.setEnabled(enabled);
        config.setFsync(fsync);
        return config;
    }

    public static DataPersistenceConfig createDataPersistenceConfig(boolean enabled, boolean fsync) {
        DataPersistenceConfig config = new DataPersistenceConfig();
        config.setEnabled(enabled);
        config.setFsync(fsync);
        return config;
    }

    public static MerkleTreeConfig createMerkleTreeConfig(boolean enabled, int depth,
                                                          boolean isEnabledSetExists, boolean isEnabledSet) {
        MerkleTreeConfig config = new MerkleTreeConfig();
        if (!isEnabledSetExists || isEnabledSet) {
            config.setEnabled(enabled);
        }
        config.setDepth(depth);
        return config;
    }

    public static NearCachePreloaderConfig createNearCachePreloaderConfig(boolean enabled, String directory,
                                                                          int storeInitialDelaySeconds,
                                                                          int storeIntervalSeconds) {
        NearCachePreloaderConfig config = new NearCachePreloaderConfig();
        config.setEnabled(enabled);
        config.setDirectory(directory);
        config.setStoreInitialDelaySeconds(storeInitialDelaySeconds);
        config.setStoreIntervalSeconds(storeIntervalSeconds);
        return config;
    }

    public static SimpleEntryView<Data, Data> createSimpleEntryView(Data key, Data value, long cost, long creationTime,
                                                                    long expirationTime, long hits, long lastAccessTime,
                                                                    long lastStoredTime, long lastUpdateTime, long version,
                                                                    long ttl, long maxIdle) {
        SimpleEntryView<Data, Data> entryView = new SimpleEntryView<>();
        entryView.setKey(key);
        entryView.setValue(value);
        entryView.setCost(cost);
        entryView.setCreationTime(creationTime);
        entryView.setExpirationTime(expirationTime);
        entryView.setHits(hits);
        entryView.setLastAccessTime(lastAccessTime);
        entryView.setLastStoredTime(lastStoredTime);
        entryView.setLastUpdateTime(lastUpdateTime);
        entryView.setVersion(version);
        entryView.setTtl(ttl);
        entryView.setMaxIdle(maxIdle);
        return entryView;
    }

    public static ReplicatedMapEntryView<Data, Data> createReplicatedMapEntryView(Data key, Data value, long creationTime,
                                                                           long hits, long lastAccessTime, long lastUpdateTime,
                                                                           long ttl) {
        ReplicatedMapEntryView<Data, Data> entryView = new ReplicatedMapEntryView<>();
        entryView.setKey(key);
        entryView.setValue(value);
        entryView.setCreationTime(creationTime);
        entryView.setHits(hits);
        entryView.setLastAccessTime(lastAccessTime);
        entryView.setLastUpdateTime(lastUpdateTime);
        entryView.setTtl(ttl);
        return entryView;
    }

    public static DefaultQueryCacheEventData createQueryCacheEventData(Data dataKey, Data dataNewValue, long sequence,
                                                                       int eventType, int partitionId, boolean isMapNameExists,
                                                                       String mapName) {
        DefaultQueryCacheEventData eventData = new DefaultQueryCacheEventData();
        eventData.setDataKey(dataKey);
        eventData.setDataNewValue(dataNewValue);
        eventData.setSequence(sequence);
        eventData.setEventType(eventType);
        eventData.setPartitionId(partitionId);
        if (isMapNameExists) {
            eventData.setMapName(mapName);
        }

        return eventData;
    }

    @SuppressWarnings("MagicNumber")
    public static DurationConfig createDurationConfig(long durationAmount, int timeUnitId) {
        TimeUnit timeUnit;
        if (timeUnitId == 0) {
            timeUnit = TimeUnit.NANOSECONDS;
        } else if (timeUnitId == 1) {
            timeUnit = TimeUnit.MICROSECONDS;
        } else if (timeUnitId == 2) {
            timeUnit = TimeUnit.MILLISECONDS;
        } else if (timeUnitId == 3) {
            timeUnit = TimeUnit.SECONDS;
        } else if (timeUnitId == 4) {
            timeUnit = TimeUnit.MINUTES;
        } else if (timeUnitId == 5) {
            timeUnit = TimeUnit.HOURS;
        } else if (timeUnitId == 6) {
            timeUnit = TimeUnit.DAYS;
        } else {
            timeUnit = null;
        }
        return new DurationConfig(durationAmount, timeUnit);
    }

    public static IndexConfig createIndexConfig(String name, int type, List<String> attributes,
                                                BitmapIndexOptions bitmapIndexOptions,
                                                boolean bTreeConfigExists,
                                                BTreeIndexConfig bTreeIndexConfig) {
        IndexType type0 = IndexType.getById(type);

        return new IndexConfig()
                .setName(name)
                .setType(type0)
                .setAttributes(attributes)
                .setBitmapIndexOptions(bitmapIndexOptions)
                .setBTreeIndexConfig(bTreeConfigExists ? bTreeIndexConfig : new BTreeIndexConfig());
    }

    public static BitmapIndexOptions createBitmapIndexOptions(String uniqueKey, int uniqueKeyTransformation) {
        UniqueKeyTransformation resolvedUniqueKeyTransformation = UniqueKeyTransformation.fromId(uniqueKeyTransformation);
        return new BitmapIndexOptions().setUniqueKey(uniqueKey).setUniqueKeyTransformation(resolvedUniqueKeyTransformation);
    }

    public static BTreeIndexConfig createBTreeIndexConfig(Capacity pageSize, MemoryTierConfig memoryTierConfig) {
        return new BTreeIndexConfig().setPageSize(pageSize).setMemoryTierConfig(memoryTierConfig);
    }

    public static ClientBwListEntryDTO createClientBwListEntry(int type, String value) {
        ClientBwListEntryDTO.Type entryType = ClientBwListEntryDTO.Type.getById(type);
        if (entryType == null) {
            throw new HecatonException("Unexpected client B/W list entry type = [" + type + "]");
        }
        return new ClientBwListEntryDTO(entryType, value);
    }

    public static EndpointQualifier createEndpointQualifier(int type, String identifier) {
        ProtocolType protocolType = ProtocolType.getById(type);
        if (protocolType == null) {
            throw new HecatonException("Unexpected protocol type = [" + type + "]");
        }
        return EndpointQualifier.resolve(protocolType, identifier);
    }

    public static SqlColumnMetadata createSqlColumnMetadata(String name, int type, boolean isNullableExists, boolean nullability) {
        SqlColumnType sqlColumnType = SqlColumnType.getById(type);

        if (sqlColumnType == null) {
            throw new HecatonException("Unexpected SQL column type = [" + type + "]");
        }

        if (isNullableExists) {
            return new SqlColumnMetadata(name, sqlColumnType, nullability);
        }

        return new SqlColumnMetadata(name, sqlColumnType, true);
    }

    public static FieldDescriptor createFieldDescriptor(@Nonnull String fieldName, int id) {
        FieldKind fieldKind = FieldKind.get(id);
        return new FieldDescriptor(fieldName, fieldKind);
    }

    public static Schema createSchema(String typeName, List<FieldDescriptor> fields) {
        return new Schema(typeName, fields);
    }

    public static HecatonJsonValue createHecatonJsonValue(String value) {
        return new HecatonJsonValue(value);
    }

    public static Capacity createCapacity(long value, int unit) {
        MemoryUnit memoryUnit = MemoryUnit.getById(unit);
        return new Capacity(value, memoryUnit);
    }

    public static MemoryTierConfig createMemoryTierConfig(Capacity capacity) {
        MemoryTierConfig config = new MemoryTierConfig();
        config.setCapacity(capacity);
        return config;
    }

    public static DiskTierConfig createDiskTierConfig(boolean enabled, String deviceName) {
        DiskTierConfig config = new DiskTierConfig();
        config.setEnabled(enabled);
        config.setDeviceName(deviceName);
        return config;
    }

    public static TieredStoreConfig createTieredStoreConfig(
            boolean enabled,
            MemoryTierConfig memoryTierConfig,
            DiskTierConfig diskTierConfig
    ) {
        TieredStoreConfig config = new TieredStoreConfig();
        config.setEnabled(enabled);
        config.setMemoryTierConfig(memoryTierConfig);
        config.setDiskTierConfig(diskTierConfig);
        return config;
    }

    public static SqlSummary createSqlSummary(String query, boolean unbounded) {
        return new SqlSummary(query, unbounded);
    }

    public static JobAndSqlSummaryIds createJobAndSqlSummary(
            boolean lightJob,
            long jobId,
            long executionId,
            String nameOrId,
            int jobStatus,
            long submissionTime,
            long completionTime,
            String failureText,
            SqlSummary sqlSummary,
            boolean isSuspensionCauseExists,
            String suspensionCause,
            boolean isUserCancelledExists,
            boolean userCancelled
    ) {
        return new JobAndSqlSummaryIds(lightJob, jobId, executionId, nameOrId, JobStatus.getById(jobStatus), submissionTime,
                completionTime, failureText, sqlSummary, isSuspensionCauseExists ? suspensionCause : null,
                isUserCancelledExists ? userCancelled : false);
    }

    public static VectorIndexConfig createVectorIndexConfig(
            String name,
            int metric,
            int dimension,
            int maxDegree,
            int efConstruction,
            boolean useDeduplication
    ) {
        var vectorConfig = new VectorIndexConfig()
                .setMetric(Metric.getById(metric))
                .setDimension(dimension)
                .setMaxDegree(maxDegree)
                .setEfConstruction(efConstruction)
                .setUseDeduplication(useDeduplication);
        if (name != null) {
            vectorConfig.setName(name);
        }
        return vectorConfig;
    }

    public static VectorValues toVectorValues(List<VectorPairHolder> vectors) {
        if (vectors == null) {
            return null;
        }
        if (vectors.size() == 1 && VectorPairHolder.SINGLE_VECTOR_NAME.equals(vectors.get(0).getName())) {
            VectorPairHolder pair = vectors.get(0);
            if (pair.getType() == VectorPairHolder.DENSE_FLOAT_VECTOR) {
                return VectorValues.of(pair.getVector());
            } else {
                throw new IllegalArgumentException("Unsupported vector type: " + pair.getType());
            }
        } else {
            var map = vectors.stream().collect(Collectors.toMap(VectorPairHolder::getName, pair -> {
                if (pair.getType() == VectorPairHolder.DENSE_FLOAT_VECTOR) {
                    return pair.getVector();
                } else {
                    throw new IllegalArgumentException("Unsupported vector type: " + pair.getType());
                }
            }));
            return VectorValues.of(map);
        }
    }

    public static DataVectorDocument createVectorDocument(Data value, List<VectorPairHolder> vectors) {
        return new DataVectorDocument(value, toVectorValues(vectors));
    }

    public static SearchOptions createVectorSearchOptions(boolean includeValue, boolean includeVectors, int limit,
                                                          Map<String, String> hints) {
        var builder = SearchOptions.builder()
                .setIncludeValue(includeValue)
                .setIncludeVectors(includeVectors)
                .limit(limit);
        if (hints != null) {
            builder.hints(hints);
        }
        return builder.build();
    }

    public static DataSearchResult createVectorSearchResult(Data key, Data value, float score, List<VectorPairHolder> vectors) {
        return new DataSearchResult(key, value, score, toVectorValues(vectors));
    }

    public static Version createVersion(byte major, byte minor) {
        return Version.of(major, minor);
    }
}
