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

package uk.org.hecaton.jet.impl.connector;

import uk.org.hecaton.jet.core.Outbox;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.Watermark;
import uk.org.hecaton.jet.core.test.TestInbox;
import uk.org.hecaton.jet.core.test.TestProcessorContext;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.jet.core.processor.DiagnosticProcessors.writeLoggerP;
import static uk.org.hecaton.jet.core.test.TestSupport.supplierFrom;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteLoggerPTest {

    @Test
    public void test() throws Exception {
        // Given
        Processor p = supplierFrom(writeLoggerP()).get();
        TestInbox inbox = new TestInbox();
        Outbox outbox = mock(Outbox.class);
        ILogger logger = mock(ILogger.class);
        p.init(outbox, new TestProcessorContext().setLogger(logger));

        // When
        inbox.add(1);
        p.process(0, inbox);
        Watermark wm = new Watermark(2);
        p.tryProcessWatermark(wm);

        // Then
        verifyNoInteractions(outbox);
        verify(logger).info("1");
        verify(logger).fine(wm.toString());
        verifyNoMoreInteractions(logger);
    }
}
