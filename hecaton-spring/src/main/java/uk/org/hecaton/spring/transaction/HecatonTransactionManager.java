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

package uk.org.hecaton.spring.transaction;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.transaction.TransactionContext;
import uk.org.hecaton.transaction.TransactionOptions;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.concurrent.TimeUnit;

/**
 * {@link org.springframework.transaction.PlatformTransactionManager} implementation
 * for a single {@link HecatonInstance}. Binds a Hecaton {@link TransactionContext}
 * from the instance to the thread (as it is already bounded by Hecaton itself) and makes it available for access.
 * <p>
 * <i>Note:</i> This transaction manager doesn't supports nested transactions, since Hecaton doesn't support them either.
 *
 * @author Balint Krivan
 * @see #getTransactionContext(HecatonInstance)
 * @see #getTransactionContext()
 */
public class HecatonTransactionManager extends AbstractPlatformTransactionManager implements ResourceTransactionManager {

    private HecatonInstance hecatonInstance;

    public HecatonTransactionManager(HecatonInstance hecatonInstance) {
        this.hecatonInstance = hecatonInstance;
    }

    /**
     * Returns the transaction context for the given Hecaton instance bounded to the current thread.
     *
     * @throws NoTransactionException if the transaction context cannot be found
     */
    public static TransactionContext getTransactionContext(HecatonInstance hecatonInstance) {
        TransactionContextHolder transactionContextHolder =
                (TransactionContextHolder) TransactionSynchronizationManager.getResource(hecatonInstance);
        if (transactionContextHolder == null) {
            throw new NoTransactionException("No TransactionContext with actual transaction available for current thread");
        }
        return transactionContextHolder.getContext();
    }

    /**
     * Returns the transaction context
     *
     * @throws NoTransactionException if the transaction context cannot be found
     */
    public TransactionContext getTransactionContext() {
        return getTransactionContext(this.hecatonInstance);
    }

    @Override
    public Object getResourceFactory() {
        return hecatonInstance;
    }

    @Override
    protected Object doGetTransaction() throws TransactionException {
        HecatonTransactionObject txObject = new HecatonTransactionObject();

        TransactionContextHolder transactionContextHolder =
                (TransactionContextHolder) TransactionSynchronizationManager.getResource(hecatonInstance);
        if (transactionContextHolder != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Found thread-bound TransactionContext [" + transactionContextHolder.getContext() + "]");
            }
            txObject.setTransactionContextHolder(transactionContextHolder, false);
        }

        return txObject;
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) throws TransactionException {
        return ((HecatonTransactionObject) transaction).hasTransaction();
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        HecatonTransactionObject txObject = (HecatonTransactionObject) transaction;

        try {
            if (txObject.getTransactionContextHolder() == null) {
                TransactionContext transactionContext = getTransactionContext(definition);
                txObject.setTransactionContextHolder(new TransactionContextHolder(transactionContext), true);
            }

            txObject.getTransactionContextHolder().beginTransaction();

            if (txObject.isNewTransactionContextHolder()) {
                TransactionSynchronizationManager.bindResource(hecatonInstance, txObject.getTransactionContextHolder());
            }
        } catch (Throwable ex) {
            closeTransactionContextAfterFailedBegin(txObject);
            throw new CannotCreateTransactionException("Could not begin Hecaton transaction", ex);
        }
    }

    private TransactionContext getTransactionContext(TransactionDefinition definition) {
        TransactionOptions options = TransactionOptions.getDefault();
        if (definition.getTimeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
            options.setTimeout(definition.getTimeout(), TimeUnit.SECONDS);
        } else if (getDefaultTimeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
            options.setTimeout(getDefaultTimeout(), TimeUnit.SECONDS);
        }
        TransactionContext transactionContext = hecatonInstance.newTransactionContext(options);
        if (logger.isDebugEnabled()) {
            logger.debug("Opened new TransactionContext [" + transactionContext + "]");
        }
        return transactionContext;
    }

    private void closeTransactionContextAfterFailedBegin(HecatonTransactionObject txObject) {
        if (txObject.isNewTransactionContextHolder()) {
            TransactionContext context = txObject.getTransactionContextHolder().getContext();
            try {
                context.rollbackTransaction();
            } catch (Throwable ex) {
                logger.debug("Could not rollback Hecaton transaction after failed transaction begin", ex);
            }
            txObject.setTransactionContextHolder(null, false);
        }
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        HecatonTransactionObject txObject = (HecatonTransactionObject) status.getTransaction();
        if (status.isDebug()) {
            logger.debug("Committing Hecaton transaction on TransactionContext ["
                    + txObject.getTransactionContextHolder().getContext() + "]");
        }

        try {
            txObject.getTransactionContextHolder().getContext().commitTransaction();
        } catch (uk.org.hecaton.transaction.TransactionException ex) {
            throw new TransactionSystemException("Could not commit Hecaton transaction", ex);
        }
    }

    @Override
    protected void doSetRollbackOnly(DefaultTransactionStatus status) throws TransactionException {
        HecatonTransactionObject txObject = (HecatonTransactionObject) status.getTransaction();
        txObject.setRollbackOnly(true);
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        HecatonTransactionObject txObject = (HecatonTransactionObject) status.getTransaction();
        if (status.isDebug()) {
            logger.debug("Rolling back Hecaton transaction on TransactionContext ["
                    + txObject.getTransactionContextHolder().getContext() + "]");
        }

        TransactionContext tx = txObject.getTransactionContextHolder().getContext();
        tx.rollbackTransaction();
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        HecatonTransactionObject txObject = (HecatonTransactionObject) transaction;

        if (txObject.isNewTransactionContextHolder()) {
            TransactionSynchronizationManager.unbindResourceIfPossible(hecatonInstance);
        }

        txObject.getTransactionContextHolder().clear();
    }

    private static class HecatonTransactionObject implements SmartTransactionObject {

        private TransactionContextHolder transactionContextHolder;
        private boolean newTransactionContextHolder;
        private boolean rollbackOnly;

        void setRollbackOnly(boolean rollbackOnly) {
            this.rollbackOnly = rollbackOnly;
        }

        void setTransactionContextHolder(TransactionContextHolder transactionContextHolder,
                                         boolean newTransactionContextHolder) {
            this.transactionContextHolder = transactionContextHolder;
            this.newTransactionContextHolder = newTransactionContextHolder;
        }

        TransactionContextHolder getTransactionContextHolder() {
            return transactionContextHolder;
        }

        boolean isNewTransactionContextHolder() {
            return newTransactionContextHolder;
        }

        boolean hasTransaction() {
            return this.transactionContextHolder != null && this.transactionContextHolder.isTransactionActive();
        }

        @Override
        public boolean isRollbackOnly() {
            return rollbackOnly;
        }

        @Override
        public void flush() {
            // do nothing here
        }
    }
}
