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

package uk.org.hecaton.client.impl.proxy.txn.xa;

import uk.org.hecaton.client.impl.connection.ClientConnection;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.XATransactionCommitCodec;
import uk.org.hecaton.client.impl.protocol.codec.XATransactionCreateCodec;
import uk.org.hecaton.client.impl.protocol.codec.XATransactionPrepareCodec;
import uk.org.hecaton.client.impl.protocol.codec.XATransactionRollbackCodec;
import uk.org.hecaton.client.impl.proxy.txn.ClientTransactionUtil;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.transaction.TransactionException;
import uk.org.hecaton.transaction.TransactionNotActiveException;
import uk.org.hecaton.transaction.impl.Transaction;
import uk.org.hecaton.transaction.impl.xa.SerializableXID;
import uk.org.hecaton.internal.util.Clock;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.transaction.impl.Transaction.State.ACTIVE;
import static uk.org.hecaton.transaction.impl.Transaction.State.COMMITTED;
import static uk.org.hecaton.transaction.impl.Transaction.State.COMMITTING;
import static uk.org.hecaton.transaction.impl.Transaction.State.COMMIT_FAILED;
import static uk.org.hecaton.transaction.impl.Transaction.State.NO_TXN;
import static uk.org.hecaton.transaction.impl.Transaction.State.PREPARED;
import static uk.org.hecaton.transaction.impl.Transaction.State.ROLLED_BACK;
import static uk.org.hecaton.transaction.impl.Transaction.State.ROLLING_BACK;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.internal.util.ExceptionUtil.sneakyThrow;

/**
 * This class does not need to be thread-safe, it is only used via XAResource
 * All visibility guarantees handled by XAResource
 */
public class XATransactionProxy {

    private final HecatonClientInstanceImpl client;
    private final ClientConnection connection;
    private final SerializableXID xid;
    private final int timeout;
    private final ILogger logger;

    private Transaction.State state = NO_TXN;
    private volatile UUID txnId;
    private long startTime;

    public XATransactionProxy(HecatonClientInstanceImpl client, ClientConnection connection, Xid xid, int timeout) {
        this.client = client;
        this.connection = connection;
        this.timeout = timeout;
        this.xid = new SerializableXID(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
        logger = client.getLoggingService().getLogger(XATransactionProxy.class);
    }

    void begin() {
        try {
            startTime = Clock.currentTimeMillis();
            ClientMessage request = XATransactionCreateCodec.encodeRequest(xid, timeout);
            ClientMessage response = ClientTransactionUtil.invoke(request, txnId, client, connection);
            txnId = XATransactionCreateCodec.decodeResponse(response);
            state = ACTIVE;
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    void prepare() {
        checkTimeout();
        try {
            if (state != ACTIVE) {
                throw new TransactionNotActiveException("Transaction is not active");
            }
            ClientMessage request = XATransactionPrepareCodec.encodeRequest(txnId);
            ClientTransactionUtil.invoke(request, txnId, client, connection);
            state = PREPARED;
        } catch (Exception e) {
            state = ROLLING_BACK;
            throw rethrow(e);
        }
    }

    void commit(boolean onePhase) {
        checkTimeout();
        try {
            if (onePhase && state != ACTIVE) {
                throw new TransactionException("Transaction is not active");
            }
            if (!onePhase && state != PREPARED) {
                throw new TransactionException("Transaction is not prepared");
            }
            state = COMMITTING;
            ClientMessage request = XATransactionCommitCodec.encodeRequest(txnId, onePhase);
            ClientTransactionUtil.invoke(request, txnId, client, connection);
            state = COMMITTED;
        } catch (Exception e) {
            state = COMMIT_FAILED;
            throw rethrow(e);
        }
    }

    void rollback() {
        state = ROLLING_BACK;
        try {
            ClientMessage request = XATransactionRollbackCodec.encodeRequest(txnId);
            ClientTransactionUtil.invoke(request, txnId, client, connection);
        } catch (Exception exception) {
            logger.warning("Exception while rolling back the transaction", exception);
        }
        state = ROLLED_BACK;
    }

    public UUID getTxnId() {
        return txnId;
    }

    public Transaction.State getState() {
        return state;
    }

    private void checkTimeout() {
        long timeoutMillis = TimeUnit.SECONDS.toMillis(timeout);
        if (startTime + timeoutMillis < Clock.currentTimeMillis()) {
            sneakyThrow(new XAException(XAException.XA_RBTIMEOUT));
        }
    }

}
