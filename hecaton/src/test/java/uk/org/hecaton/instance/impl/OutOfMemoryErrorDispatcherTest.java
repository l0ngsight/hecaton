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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.OutOfMemoryHandler;
import uk.org.hecaton.memory.MemoryUnit;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.internal.util.MemoryInfoAccessor;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.verification.VerificationMode;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class OutOfMemoryErrorDispatcherTest extends HecatonTestSupport {

    @Before
    public void before() {
        OutOfMemoryErrorDispatcher.clearServers();
    }

    @Test
    public void onOutOfMemory() {
        OutOfMemoryError oome = new OutOfMemoryError();
        OutOfMemoryHandler handler = mock(OutOfMemoryHandler.class);
        when(handler.shouldHandle(oome)).thenReturn(Boolean.TRUE);

        HecatonInstance hz1 = mock(HecatonInstance.class);

        OutOfMemoryErrorDispatcher.registerServer(hz1);
        OutOfMemoryErrorDispatcher.setServerHandler(handler);

        HecatonInstance[] registeredInstances = OutOfMemoryErrorDispatcher.current();

        OutOfMemoryErrorDispatcher.onOutOfMemory(oome);

        //make sure the handler is called
        verify(handler).onOutOfMemory(oome, registeredInstances);
        //make sure that the registered instances are removed.
        assertArrayEquals(new HecatonInstance[]{}, OutOfMemoryErrorDispatcher.current());
    }

    @Test
    public void register() {
        HecatonInstance hz1 = mock(HecatonInstance.class);
        HecatonInstance hz2 = mock(HecatonInstance.class);

        OutOfMemoryErrorDispatcher.registerServer(hz1);
        assertArrayEquals(new HecatonInstance[]{hz1}, OutOfMemoryErrorDispatcher.current());

        OutOfMemoryErrorDispatcher.registerServer(hz2);
        assertArrayEquals(new HecatonInstance[]{hz1, hz2}, OutOfMemoryErrorDispatcher.current());
    }

    @Test(expected = IllegalArgumentException.class)
    public void register_whenNull() {
        OutOfMemoryErrorDispatcher.registerServer(null);
    }

    @Test
    public void deregister_Existing() {
        HecatonInstance hz1 = mock(HecatonInstance.class);
        HecatonInstance hz2 = mock(HecatonInstance.class);
        HecatonInstance hz3 = mock(HecatonInstance.class);
        OutOfMemoryErrorDispatcher.registerServer(hz1);
        OutOfMemoryErrorDispatcher.registerServer(hz2);
        OutOfMemoryErrorDispatcher.registerServer(hz3);

        OutOfMemoryErrorDispatcher.deregisterServer(hz2);
        assertArrayEquals(new HecatonInstance[]{hz1, hz3}, OutOfMemoryErrorDispatcher.current());

        OutOfMemoryErrorDispatcher.deregisterServer(hz1);
        assertArrayEquals(new HecatonInstance[]{hz3}, OutOfMemoryErrorDispatcher.current());

        OutOfMemoryErrorDispatcher.deregisterServer(hz3);
        assertArrayEquals(new HecatonInstance[]{}, OutOfMemoryErrorDispatcher.current());
    }

    @Test
    public void deregister_nonExisting() {
        HecatonInstance instance = mock(HecatonInstance.class);
        OutOfMemoryErrorDispatcher.deregisterServer(instance);
    }

    @Test(expected = IllegalArgumentException.class)
    public void deregister_null() {
        OutOfMemoryErrorDispatcher.deregisterServer(null);
    }

    @Test
    public void test_OutOfMemoryHandler_shouldHandle_true() {
        test_OutOfMemoryHandler_with_shouldHandle(Boolean.TRUE, times(1));
    }

    @Test
    public void test_OutOfMemoryHandler_shouldHandle_false() {
        test_OutOfMemoryHandler_with_shouldHandle(Boolean.FALSE, never());
    }

    private void test_OutOfMemoryHandler_with_shouldHandle(Boolean answer, VerificationMode verificationMode) {
        OutOfMemoryError oome = new OutOfMemoryError();

        HecatonInstance hz = mock(HecatonInstance.class);
        OutOfMemoryErrorDispatcher.registerServer(hz);

        OutOfMemoryHandler handler = mock(OutOfMemoryHandler.class);
        when(handler.shouldHandle(oome)).thenReturn(answer);
        OutOfMemoryErrorDispatcher.setServerHandler(handler);

        OutOfMemoryErrorDispatcher.onOutOfMemory(oome);
        verify(handler, verificationMode).onOutOfMemory(oome, new HecatonInstance[]{hz});
    }

    @Test
    public void test_DefaultOutOfMemoryHandler_whenGcOverheadLimitExceeded() {
        OutOfMemoryError oome = new OutOfMemoryError(DefaultOutOfMemoryHandler.GC_OVERHEAD_LIMIT_EXCEEDED);

        HecatonInstance hz = mock(HecatonInstance.class);
        OutOfMemoryErrorDispatcher.registerServer(hz);

        OutOfMemoryHandler handler = spy(new DefaultOutOfMemoryHandler());
        when(handler.shouldHandle(oome)).thenCallRealMethod();
        OutOfMemoryErrorDispatcher.setServerHandler(handler);

        OutOfMemoryErrorDispatcher.onOutOfMemory(oome);
        verify(handler).onOutOfMemory(oome, new HecatonInstance[]{hz});
    }

    @Test
    public void test_DefaultOutOfMemoryHandler_total_smallerThan_max() {
        test_DefaultOutOfMemoryHandler_using_accessor(new MemoryInfoAccessor() {
            @Override
            public long getMaxMemory() {
                return MemoryUnit.MEGABYTES.toBytes(100);
            }

            @Override
            public long getTotalMemory() {
                return MemoryUnit.MEGABYTES.toBytes(80);
            }

            @Override
            public long getFreeMemory() {
                return MemoryUnit.MEGABYTES.toBytes(10);
            }
        }, never());
    }

    @Test
    public void test_DefaultOutOfMemoryHandler_total_equalTo_max() {
        test_DefaultOutOfMemoryHandler_using_accessor(new MemoryInfoAccessor() {
            @Override
            public long getMaxMemory() {
                return MemoryUnit.MEGABYTES.toBytes(100);
            }

            @Override
            public long getTotalMemory() {
                return MemoryUnit.MEGABYTES.toBytes(100);
            }

            @Override
            public long getFreeMemory() {
                return MemoryUnit.MEGABYTES.toBytes(20);
            }
        }, never());
    }

    @Test
    public void test_DefaultOutOfMemoryHandler_not_enough_memory() {
        test_DefaultOutOfMemoryHandler_using_accessor(new MemoryInfoAccessor() {
            @Override
            public long getMaxMemory() {
                return MemoryUnit.MEGABYTES.toBytes(100);
            }

            @Override
            public long getTotalMemory() {
                return MemoryUnit.MEGABYTES.toBytes(100);
            }

            @Override
            public long getFreeMemory() {
                return MemoryUnit.MEGABYTES.toBytes(5);
            }
        }, times(1));
    }

    private void test_DefaultOutOfMemoryHandler_using_accessor(MemoryInfoAccessor memoryInfoAccessor,
                                                               VerificationMode verificationMode) {
        OutOfMemoryError oome = new OutOfMemoryError();

        HecatonInstance hz = mock(HecatonInstance.class);
        OutOfMemoryErrorDispatcher.registerServer(hz);

        OutOfMemoryHandler handler = spy(new DefaultOutOfMemoryHandler(0.1d, memoryInfoAccessor));
        OutOfMemoryErrorDispatcher.setServerHandler(handler);

        OutOfMemoryErrorDispatcher.onOutOfMemory(oome);
        verify(handler, verificationMode).onOutOfMemory(oome, new HecatonInstance[]{hz});
    }
}
