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

package uk.org.hecaton.map;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category(NightlyTest.class)
public class MapPutDestroyTest extends HecatonTestSupport {

    @Test
    public void testConcurrentPutDestroy_doesNotCauseNPE() {
        final HecatonInstance instance = createHecatonInstance(getConfig());
        final String name = randomString();
        final AtomicReference<Throwable> error = new AtomicReference<>(null);

        final AtomicBoolean stop = new AtomicBoolean();

        Thread t1 = new Thread(
                () -> {
                    try {
                        while (!stop.get()) {
                            IMap<Object, Object> map = instance.getMap(name);
                            map.put(System.currentTimeMillis(), Boolean.TRUE);
                        }
                    } catch (Throwable e) {
                        error.set(e);
                    }
                }
        );

        Thread t2 = new Thread(
                () -> {
                    try {
                        while (!stop.get()) {
                            IMap<Object, Object> map = instance.getMap(name);
                            map.destroy();
                        }
                    } catch (Throwable e) {
                        error.set(e);
                    }
                }
        );

        t1.start();
        t2.start();

        sleepSeconds(10);
        stop.set(true);

        try {
            t1.join();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        try {
            t2.join();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        Throwable object = error.get();

        if (object != null) {
            object.printStackTrace(System.out);
            fail(object.getMessage());
        }
    }
}
