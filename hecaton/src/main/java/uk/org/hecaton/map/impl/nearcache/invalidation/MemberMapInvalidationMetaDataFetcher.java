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

package uk.org.hecaton.map.impl.nearcache.invalidation;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.operation.MapGetInvalidationMetaDataOperation;
import uk.org.hecaton.map.impl.operation.MapGetInvalidationMetaDataOperation.MetaDataResponse;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;

import java.util.Collection;
import java.util.List;

import static uk.org.hecaton.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * {@code InvalidationMetaDataFetcher} for member side usage
 */
public class MemberMapInvalidationMetaDataFetcher extends InvalidationMetaDataFetcher {

    private final ClusterService clusterService;
    private final OperationService operationService;

    public MemberMapInvalidationMetaDataFetcher(ClusterService clusterService,
                                                OperationService operationService, ILogger logger) {
        super(logger);
        this.clusterService = clusterService;
        this.operationService = operationService;
    }

    @Override
    protected Collection<Member> getDataMembers() {
        return clusterService.getMembers(DATA_MEMBER_SELECTOR);
    }

    @Override
    protected InternalCompletableFuture fetchMetadataOf(Member member, List<String> names) {
        Operation operation = new MapGetInvalidationMetaDataOperation(names);
        return operationService.invokeOnTarget(SERVICE_NAME, operation, member.getAddress());
    }

    @Override
    protected void extractMemberMetadata(Member member,
                                         InternalCompletableFuture future,
                                         MetadataHolder metadataHolder) throws Exception {

        MetaDataResponse response = (MetaDataResponse) future.get(ASYNC_RESULT_WAIT_TIMEOUT_MINUTES, MINUTES);
        metadataHolder.setMetadata(response.getPartitionUuidList().entrySet(),
                response.getNamePartitionSequenceList().entrySet());
    }
}
