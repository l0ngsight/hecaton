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

package uk.org.hecaton.client.impl.proxy.txn;

import uk.org.hecaton.client.impl.connection.ClientConnection;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.TransactionCommitCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionCreateCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionRollbackCodec;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.transaction.TransactionNotActiveException;
import uk.org.hecaton.transaction.TransactionOptions;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.internal.util.ThreadUtil;
import uk.org.hecaton.transaction.TransactionTimedOutException;

import java.util.UUID;

import javax.annotation.Nonnull;

import static uk.org.hecaton.transaction.impl.Transaction.State;
import static uk.org.hecaton.transaction.impl.Transaction.State.ACTIVE;
import static uk.org.hecaton.transaction.impl.Transaction.State.COMMITTED;
import static uk.org.hecaton.transaction.impl.Transaction.State.COMMITTING;
import static uk.org.hecaton.transaction.impl.Transaction.State.COMMIT_FAILED;
import static uk.org.hecaton.transaction.impl.Transaction.State.NO_TXN;
import static uk.org.hecaton.transaction.impl.Transaction.State.ROLLED_BACK;
import static uk.org.hecaton.transaction.impl.Transaction.State.ROLLING_BACK;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;

final class TransactionProxy {

    private static final ThreadLocal<Boolean> TRANSACTION_EXISTS = new ThreadLocal<>();

    private final TransactionOptions options;
    private final HecatonClientInstanceImpl client;
    private final long threadId = ThreadUtil.getThreadId();
    private final ClientConnection connection;
    private final ILogger logger;

    private UUID txnId;
    private State state = NO_TXN;
    private long startTime;

    TransactionProxy(HecatonClientInstanceImpl client,
                     @Nonnull TransactionOptions options, ClientConnection connection) {
        this.options = options;
        this.client = client;
        this.connection = connection;
        this.logger = client.getLoggingService().getLogger(TransactionProxy.class);
    }

    public UUID getTxnId() {
        return txnId;
    }

    public State getState() {
        return state;
    }

    void begin() {
        try {
            if (state == ACTIVE) {
                throw new IllegalStateException("Transaction is already active");
            }
            checkThread();
            if (TRANSACTION_EXISTS.get() != null) {
                throw new IllegalStateException("Nested transactions are not allowed!");
            }
            TRANSACTION_EXISTS.set(Boolean.TRUE);
            startTime = Clock.currentTimeMillis();
            ClientMessage request = TransactionCreateCodec.encodeRequest(options.getTimeoutMillis(),
                    options.getDurability(), options.getTransactionType().id(), threadId);
            ClientMessage response = ClientTransactionUtil.invoke(request, getTxnId(), client, connection);
            txnId = TransactionCreateCodec.decodeResponse(response);
            state = ACTIVE;
        } catch (Exception e) {
            TRANSACTION_EXISTS.remove();
            throw rethrow(e);
        }
    }

    void commit() {
        try {
            if (state != ACTIVE) {
                throw new TransactionNotActiveException("Transaction is not active");
            }
            state = COMMITTING;
            checkThread();
            checkTimeout();
            ClientMessage request = TransactionCommitCodec.encodeRequest(txnId, threadId);
            ClientTransactionUtil.invoke(request, getTxnId(), client, connection);
            state = COMMITTED;
        } catch (Exception e) {
            state = COMMIT_FAILED;
            throw rethrow(e);
        } finally {
            TRANSACTION_EXISTS.remove();
        }
    }

    void rollback() {
        try {
            if (state == NO_TXN || state == ROLLED_BACK) {
                throw new IllegalStateException("Transaction is not active");
            }
            state = ROLLING_BACK;
            checkThread();
            try {
                ClientMessage request = TransactionRollbackCodec.encodeRequest(txnId, threadId);
                ClientTransactionUtil.invoke(request, getTxnId(), client, connection);
            } catch (Exception exception) {
                logger.warning("Exception while rolling back the transaction", exception);
            }
            state = ROLLED_BACK;
        } finally {
            TRANSACTION_EXISTS.remove();
        }
    }

    private void checkThread() {
        if (threadId != Thread.currentThread().getId()) {
            throw new IllegalStateException("Transaction cannot span multiple threads!");
        }
    }

    private void checkTimeout() {
        if (startTime + options.getTimeoutMillis() < Clock.currentTimeMillis()) {
            throw new TransactionTimedOutException("Transaction is timed-out!");
        }
    }

}
