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

package uk.org.hecaton.client.cache.impl.nearcache.invalidation;

import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheFetchNearCacheInvalidationMetadataCodec.ResponseParameters;
import uk.org.hecaton.client.impl.spi.ClientClusterService;
import uk.org.hecaton.client.impl.spi.ClientContext;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;

import java.util.Collection;
import java.util.List;

import static uk.org.hecaton.client.impl.protocol.codec.CacheFetchNearCacheInvalidationMetadataCodec.decodeResponse;
import static uk.org.hecaton.client.impl.protocol.codec.CacheFetchNearCacheInvalidationMetadataCodec.encodeRequest;
import static uk.org.hecaton.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * {@code InvalidationMetaDataFetcher} for client side usage
 */
public class ClientCacheInvalidationMetaDataFetcher extends InvalidationMetaDataFetcher {

    private final ClientClusterService clusterService;
    private final HecatonClientInstanceImpl clientImpl;

    public ClientCacheInvalidationMetaDataFetcher(ClientContext clientContext) {
        super(clientContext.getLoggingService().getLogger(ClientCacheInvalidationMetaDataFetcher.class));
        this.clusterService = clientContext.getClusterService();
        this.clientImpl = (HecatonClientInstanceImpl) clientContext.getHecatonInstance();
    }

    @Override
    protected Collection<Member> getDataMembers() {
        return clusterService.getMembers(DATA_MEMBER_SELECTOR);
    }

    @Override
    protected InternalCompletableFuture fetchMetadataOf(Member member, List<String> names) {
        ClientMessage message = encodeRequest(names, member.getUuid());
        ClientInvocation invocation = new ClientInvocation(clientImpl, message, null, member.getUuid());
        return invocation.invoke();
    }

    @Override
    protected void extractMemberMetadata(Member member,
                                         InternalCompletableFuture future,
                                         MetadataHolder metadataHolder) throws Exception {

        ClientMessage message = ((ClientMessage) future.get(ASYNC_RESULT_WAIT_TIMEOUT_MINUTES, MINUTES));
        ResponseParameters response = decodeResponse(message);

        metadataHolder.setMetadata(response.partitionUuidList, response.namePartitionSequenceList);
    }
}
