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

package uk.org.hecaton.transaction.impl;

import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static uk.org.hecaton.test.Accessors.getOperationService;
import static uk.org.hecaton.transaction.TransactionOptions.TransactionType.ONE_PHASE;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests some basic behavior that doesn't rely too much on the type of transaction
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class TransactionImplTest extends HecatonTestSupport {

    private OperationServiceImpl operationService;
    private ILogger logger;
    private TransactionManagerServiceImpl txManagerService;
    private NodeEngine nodeEngine;
    private TransactionOptions options;

    @Before
    public void setup() {
        HecatonInstance hz = createHecatonInstance();
        operationService = getOperationService(hz);
        logger = mock(ILogger.class);

        txManagerService = mock(TransactionManagerServiceImpl.class);

        nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getOperationService()).thenReturn(operationService);
        when(nodeEngine.getLocalMember()).thenReturn(new MemberImpl());
        when(nodeEngine.getLogger(TransactionImpl.class)).thenReturn(logger);
        options = new TransactionOptions().setTransactionType(ONE_PHASE);
    }

    @Test
    public void getTimeoutMillis() {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        assertEquals(options.getTimeoutMillis(), tx.getTimeoutMillis());
    }

    @Test
    public void testToString() throws Exception {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, UUID.randomUUID());
        assertEquals(format("Transaction{txnId='%s', state=%s, txType=%s, timeoutMillis=%s}",
                tx.getTxnId(), tx.getState(), options.getTransactionType(), options.getTimeoutMillis()), tx.toString());
    }

    @Test
    public void getOwnerUUID() {
        UUID ownerUUID = UUID.randomUUID();
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, ownerUUID);
        assertEquals(ownerUUID, tx.getOwnerUuid());
    }

}
