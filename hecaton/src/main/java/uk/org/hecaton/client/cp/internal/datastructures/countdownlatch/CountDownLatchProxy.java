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

package uk.org.hecaton.client.cp.internal.datastructures.countdownlatch;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import uk.org.hecaton.client.impl.protocol.codec.CountDownLatchAwaitCodec;
import uk.org.hecaton.client.impl.protocol.codec.CountDownLatchCountDownCodec;
import uk.org.hecaton.client.impl.protocol.codec.CountDownLatchGetCountCodec;
import uk.org.hecaton.client.impl.protocol.codec.CountDownLatchGetRoundCodec;
import uk.org.hecaton.client.impl.protocol.codec.CountDownLatchTrySetCountCodec;
import uk.org.hecaton.client.impl.spi.ClientContext;
import uk.org.hecaton.client.impl.spi.ClientProxy;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.ICountDownLatch;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import uk.org.hecaton.internal.util.EmptyStatement;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static uk.org.hecaton.internal.util.Preconditions.checkPositive;
import static uk.org.hecaton.internal.util.UuidUtil.newUnsecureUUID;

/**
 * Client-side Raft-based proxy implementation of {@link ICountDownLatch}
 */
public class CountDownLatchProxy extends ClientProxy implements ICountDownLatch {

    private final RaftGroupId groupId;
    private final String objectName;

    public CountDownLatchProxy(ClientContext context, RaftGroupId groupId, String proxyName, String objectName) {
        super(CountDownLatchService.SERVICE_NAME, proxyName, context);
        this.groupId = groupId;
        this.objectName = objectName;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) {
        checkNotNull(unit);

        long timeoutMillis = Math.max(0, unit.toMillis(timeout));
        ClientMessage request = CountDownLatchAwaitCodec.encodeRequest(groupId, objectName, newUnsecureUUID(), timeoutMillis);
        ClientMessage response = new ClientInvocation(getClient(), request, name).invoke().joinInternal();

        return CountDownLatchAwaitCodec.decodeResponse(response);
    }

    @Override
    public void countDown() {
        int round = getRound();
        UUID invocationUid = newUnsecureUUID();
        for (;;) {
            try {
                countDown(round, invocationUid);
                return;
            } catch (OperationTimeoutException e) {
                EmptyStatement.ignore(e);
                // I can retry safely because my retry would be idempotent...
            }
        }
    }

    private int getRound() {
        ClientMessage request = CountDownLatchGetRoundCodec.encodeRequest(groupId, objectName);
        ClientMessage response = new ClientInvocation(getClient(), request, name).invoke().joinInternal();

        return CountDownLatchGetRoundCodec.decodeResponse(response);
    }

    private void countDown(int round, UUID invocationUid) {
        ClientMessage request = CountDownLatchCountDownCodec.encodeRequest(groupId, objectName, invocationUid, round);

        new ClientInvocation(getClient(), request, name).invoke().joinInternal();
    }

    @Override
    public int getCount() {
        ClientMessage request = CountDownLatchGetCountCodec.encodeRequest(groupId, objectName);
        ClientMessage response = new ClientInvocation(getClient(), request, name).invoke().joinInternal();

        return CountDownLatchGetCountCodec.decodeResponse(response);
    }

    @Override
    public boolean trySetCount(int count) {
        checkPositive(count, "Count must be positive!");

        ClientMessage request = CountDownLatchTrySetCountCodec.encodeRequest(groupId, objectName, count);
        ClientMessage response = new ClientInvocation(getClient(), request, name).invoke().joinInternal();

        return CountDownLatchTrySetCountCodec.decodeResponse(response);
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    public CPGroupId getGroupId() {
        return groupId;
    }

    @Override
    public void onDestroy() {
        ClientMessage request = CPGroupDestroyCPObjectCodec.encodeRequest(groupId, getServiceName(), objectName);
        new ClientInvocation(getClient(), request, name).invoke().joinInternal();
    }

}
