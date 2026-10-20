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

package uk.org.hecaton.client.impl.spi.impl;

import uk.org.hecaton.client.HecatonClientNotActiveException;
import uk.org.hecaton.client.HecatonClientOfflineException;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientConnectionStrategyConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.connection.ClientConnection;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.impl.proxy.txn.TransactionContextProxy;
import uk.org.hecaton.client.impl.proxy.txn.xa.XATransactionContextProxy;
import uk.org.hecaton.client.impl.spi.ClientClusterService;
import uk.org.hecaton.client.impl.spi.ClientTransactionManagerService;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionException;
import uk.org.hecaton.transaction.TransactionOptions;
import uk.org.hecaton.transaction.TransactionalTask;

import javax.annotation.Nonnull;
import javax.transaction.xa.Xid;
import java.util.Collection;

import static uk.org.hecaton.internal.util.Clock.currentTimeMillis;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static uk.org.hecaton.internal.util.StringUtil.timeToString;

public class ClientTransactionManagerServiceImpl implements ClientTransactionManagerService {

    private final HecatonClientInstanceImpl client;

    public ClientTransactionManagerServiceImpl(HecatonClientInstanceImpl client) {
        this.client = client;
    }

    public HecatonClientInstanceImpl getClient() {
        return client;
    }

    @Override
    public TransactionContext newTransactionContext() {
        return newTransactionContext(TransactionOptions.getDefault());
    }

    @Override
    public TransactionContext newTransactionContext(@Nonnull TransactionOptions options) {
        return new TransactionContextProxy(this, options);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionalTask<T> task) throws TransactionException {
        return executeTransaction(TransactionOptions.getDefault(), task);
    }

    @Override
    public <T> T executeTransaction(@Nonnull TransactionOptions options,
                                    @Nonnull TransactionalTask<T> task) throws TransactionException {
        checkNotNull(options, "TransactionOptions must not be null!");
        checkNotNull(task, "TransactionalTask is required!");
        final TransactionContext context = newTransactionContext(options);
        context.beginTransaction();
        try {
            final T value = task.execute(context);
            context.commitTransaction();
            return value;
        } catch (Throwable e) {
            context.rollbackTransaction();
            if (e instanceof TransactionException exception) {
                throw exception;
            }
            if (e.getCause() instanceof TransactionException transactionException) {
                throw transactionException;
            }
            if (e instanceof RuntimeException exception) {
                throw exception;
            }
            throw new TransactionException(e);
        }
    }

    @Override
    public TransactionContext newXATransactionContext(Xid xid, int timeoutInSeconds) {
        return new XATransactionContextProxy(this, xid, timeoutInSeconds);
    }

    public void shutdown() {
    }

    @Override
    public String getClusterName() {
        return client.getClientConfig().getClusterName();
    }

    public ClientConnection connect() throws Exception {
        ClientInvocationServiceImpl invocationService = (ClientInvocationServiceImpl) client.getInvocationService();
        long startTimeMillis = System.currentTimeMillis();
        long invocationTimeoutMillis = invocationService.getInvocationTimeoutMillis();

        while (client.getLifecycleService().isRunning()) {
            try {
                ClientConnection connection = client.getConnectionManager().getRandomConnection();
                if (connection == null) {
                    throw throwException();
                }
                return connection;
            } catch (Exception e) {
                if (e instanceof HecatonClientOfflineException) {
                    throw e;
                }
                if (System.currentTimeMillis() - startTimeMillis > invocationTimeoutMillis) {
                    throw newOperationTimeoutException(e, invocationTimeoutMillis, startTimeMillis);
                }
            }
            Thread.sleep(invocationService.getInvocationRetryPauseMillis());
        }
        throw new HecatonClientNotActiveException();
    }

    private Exception newOperationTimeoutException(Throwable e, long invocationTimeoutMillis, long startTimeMillis) {
        StringBuilder sb = new StringBuilder();
        sb.append("Creating transaction context timed out because exception occurred after client invocation timeout ");
        sb.append(invocationTimeoutMillis).append(" ms. ");
        sb.append("Current time: ").append(timeToString(currentTimeMillis())).append(". ");
        sb.append("Start time: ").append(timeToString(startTimeMillis)).append(". ");
        sb.append("Total elapsed time: ").append(currentTimeMillis() - startTimeMillis).append(" ms. ");
        String msg = sb.toString();
        return new OperationTimeoutException(msg, e);
    }

    private RuntimeException throwException() {
        ClientConfig clientConfig = client.getClientConfig();
        ClientConnectionStrategyConfig connectionStrategyConfig = clientConfig.getConnectionStrategyConfig();
        ClientConnectionStrategyConfig.ReconnectMode reconnectMode = connectionStrategyConfig.getReconnectMode();
        if (reconnectMode == ClientConnectionStrategyConfig.ReconnectMode.ASYNC) {
            throw new HecatonClientOfflineException();
        }
        String msg = getExceptionMsgByRoutingMode();
        throw new IllegalStateException(msg);
    }

    private String getExceptionMsgByRoutingMode() {
        RoutingMode routingMode = client.getConnectionManager().getRoutingMode();
        return switch (routingMode) {
            case ALL_MEMBERS -> toAllMembersModeExceptionMsg();
            case SINGLE_MEMBER -> "No active connection is found";
            case MULTI_MEMBER -> toMultiMemberModeExceptionMsg();
            default -> throw new IllegalStateException("Unsupported RoutingMode: " + routingMode);
        };
    }

    private String toAllMembersModeExceptionMsg() {
        Collection<Member> members = client.getClientClusterService().getEffectiveMemberList();
        if (members.isEmpty()) {
            return "No address was returned by the LoadBalancer since there are no members in the cluster";
        } else {
            return "No address was returned by the LoadBalancer. "
                    + "But the cluster contains the following members:" + members;
        }
    }

    private String toMultiMemberModeExceptionMsg() {
        ClientClusterService clientClusterService = client.getClientClusterService();
        Collection<Member> effectiveMembers = clientClusterService.getEffectiveMemberList();
        Collection<Member> allMembers = clientClusterService.getMemberList();

        if (effectiveMembers.isEmpty() && allMembers.isEmpty()) {
            return "No address was returned by the LoadBalancer since there is no member "
                    + "in subset and in the cluster as well";
        }

        if (effectiveMembers.isEmpty()) {
            return "No address was returned by the LoadBalancer since there is no member "
                    + "in subset but the cluster has these members:" + allMembers;
        }

        return "No address was returned by the LoadBalancer. "
                + "But the effective members list contains the following:" + effectiveMembers
                + ", while the cluster has these members:" + allMembers;
    }
}
