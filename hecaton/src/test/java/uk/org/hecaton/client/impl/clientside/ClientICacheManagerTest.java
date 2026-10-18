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

package uk.org.hecaton.client.impl.clientside;

import uk.org.hecaton.client.impl.spi.impl.ClientServiceNotFoundException;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.exception.ServiceNotFoundException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test ClientICacheManager.getCache exception handling
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientICacheManagerTest {

    @Test
    public void getCache_when_hecatonExceptionIsThrown_then_isRethrown() {
        // when a HecatonException occurs whose cause is not a ServiceNotFoundException
        HecatonInstance hzInstance = mock(HecatonInstance.class);
        when(hzInstance.getDistributedObject(anyString(), anyString())).thenThrow(new HecatonException("mock exception"));

        ClientICacheManager clientCacheManager = new ClientICacheManager(hzInstance);
        // then the exception is rethrown
        assertThrows(HecatonException.class, () -> clientCacheManager.getCache("any-cache"));
    }

    @Test
    public void getCache_when_serviceNotFoundExceptionIsThrown_then_illegalStateExceptionIsThrown() {
        // when HecatonException with ServiceNotFoundException cause was thrown by hzInstance.getDistributedObject
        // (i.e. cache support is not available server-side)
        HecatonInstance hzInstance = mock(HecatonInstance.class);
        HecatonException hzException = new HecatonException("mock exception",
                new ServiceNotFoundException("mock exception"));
        when(hzInstance.getDistributedObject(anyString(), anyString())).thenThrow(hzException);

        ClientICacheManager clientCacheManager = new ClientICacheManager(hzInstance);
        // then an IllegalStateException will be thrown by getCache
        assertThrows(IllegalStateException.class, () -> clientCacheManager.getCache("any-cache"));
    }

    @Test
    public void getCache_when_clientServiceNotFoundExceptionIsThrown_then_illegalStateExceptionIsThrown() {
        // when ClientServiceNotFoundException was thrown by hzInstance.getDistributedObject
        // (i.e. cache support is not available on the client-side)
        HecatonInstance hzInstance = mock(HecatonInstance.class);
        when(hzInstance.getDistributedObject(anyString(), anyString()))
                .thenThrow(new ClientServiceNotFoundException("mock exception"));

        ClientICacheManager clientCacheManager = new ClientICacheManager(hzInstance);
        // then an IllegalStateException will be thrown by getCache
        assertThrows(IllegalStateException.class, () -> clientCacheManager.getCache("any-cache"));
    }

}
