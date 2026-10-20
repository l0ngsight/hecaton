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

package uk.org.hecaton.logging;

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;

import java.util.logging.Level;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Slf4jLoggerTest extends AbstractLoggerTest {

    private Logger mockLogger;
    private ILogger hecatonLogger;

    @Before
    public void setUp() {
        mockLogger = mock(Logger.class);
        hecatonLogger = new Slf4jFactory.Slf4jLogger(mockLogger);
    }

    @Test
    public void isLoggable_whenLevelOff_shouldReturnFalse() {
        assertFalse(hecatonLogger.isLoggable(Level.OFF));
    }

    @Test
    public void logWithThrowable_shouldCallLogWithThrowable() {
        hecatonLogger.warning(MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).warn(MESSAGE, THROWABLE);
    }

    @Test
    public void logAtLevelOff_shouldNotLog() {
        hecatonLogger.log(Level.OFF, MESSAGE);
        verifyNoInteractions(mockLogger);
    }

    @Test
    public void logAtLevelOff_shouldNotLog_withThrowable() {
        hecatonLogger.log(Level.OFF, MESSAGE, THROWABLE);
        verifyNoInteractions(mockLogger);
    }

    @Test
    public void logAtLevelAll_shouldLogInfo() {
        hecatonLogger.log(Level.ALL, MESSAGE);
        verify(mockLogger, times(1)).info(MESSAGE);
    }

    @Test
    public void logAtLevelAll_shouldLogInfo_withThrowable() {
        hecatonLogger.log(Level.ALL, MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).info(MESSAGE, THROWABLE);
    }

    @Test
    public void logFinest_shouldLogTrace() {
        hecatonLogger.finest(MESSAGE);
        verify(mockLogger, times(1)).trace(MESSAGE);
    }

    @Test
    public void logFinest_shouldLogTrace_withThrowable() {
        hecatonLogger.finest(MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).trace(MESSAGE, THROWABLE);
    }

    @Test
    public void logFine_shouldLogDebug() {
        hecatonLogger.fine(MESSAGE);
        verify(mockLogger, times(1)).debug(MESSAGE);
    }

    @Test
    public void logFine_shouldLogDebug_withThrowable() {
        hecatonLogger.fine(MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).debug(MESSAGE, THROWABLE);
    }

    @Test
    public void logInfo_shouldLogInfo() {
        hecatonLogger.info(MESSAGE);
        verify(mockLogger, times(1)).info(MESSAGE);
    }

    @Test
    public void logInfo_shouldLogInfo_withThrowable() {
        hecatonLogger.log(Level.INFO, MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).info(MESSAGE, THROWABLE);
    }

    @Test
    public void logWarning_shouldLogWarn() {
        hecatonLogger.warning(MESSAGE);
        verify(mockLogger, times(1)).warn(MESSAGE);
    }

    @Test
    public void logWarning_shouldLogWarn_withThrowable() {
        hecatonLogger.warning(MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).warn(MESSAGE, THROWABLE);
    }

    @Test
    public void logSevere_shouldLogError() {
        hecatonLogger.severe(MESSAGE);
        verify(mockLogger, times(1)).error(MESSAGE);
    }

    @Test
    public void logSevere_shouldLogError_withThrowable() {
        hecatonLogger.severe(MESSAGE, THROWABLE);
        verify(mockLogger, times(1)).error(MESSAGE, THROWABLE);
    }

    @Test
    public void logEvent_shouldLogWithCorrectLevel() {
        hecatonLogger.log(LOG_EVENT);
        verify(mockLogger, times(1)).warn(MESSAGE, THROWABLE);
    }
}
