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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static uk.org.hecaton.internal.util.EmptyStatement.ignore;
import static uk.org.hecaton.internal.util.Preconditions.isNotNull;
import static java.lang.System.arraycopy;

public final class OutOfMemoryErrorDispatcher {

    private static final int MAX_REGISTERED_INSTANCES = 50;
    private static final HecatonInstance[] EMPTY_INSTANCES = new HecatonInstance[0];

    private static final AtomicReference<HecatonInstance[]> SERVER_INSTANCES_REF =
            new AtomicReference<>(EMPTY_INSTANCES);

    private static final AtomicReference<HecatonInstance[]> CLIENT_INSTANCES_REF =
            new AtomicReference<>(EMPTY_INSTANCES);

    private static volatile OutOfMemoryHandler handler = new DefaultOutOfMemoryHandler();

    private static volatile OutOfMemoryHandler clientHandler = new EmptyOutOfMemoryHandler();

    private static final AtomicInteger OUT_OF_MEMORY_ERROR_COUNT = new AtomicInteger();

    private OutOfMemoryErrorDispatcher() {
    }

    //for testing only
    static HecatonInstance[] current() {
        return SERVER_INSTANCES_REF.get();
    }

    /**
     * Gets the number of OutOfMemoryErrors that have been reported.
     *
     * @return the number of OutOfMemoryErrors.
     */
    public static int getOutOfMemoryErrorCount() {
        return OUT_OF_MEMORY_ERROR_COUNT.get();
    }

    public static void setServerHandler(OutOfMemoryHandler outOfMemoryHandler) {
        handler = outOfMemoryHandler;
    }

    public static void setClientHandler(OutOfMemoryHandler outOfMemoryHandler) {
        clientHandler = outOfMemoryHandler;
    }

    public static void registerServer(HecatonInstance instance) {
       register(SERVER_INSTANCES_REF, instance);
    }

    public static void registerClient(HecatonInstance instance) {
        register(CLIENT_INSTANCES_REF, instance);
    }

    private static void register(AtomicReference<HecatonInstance[]> ref, HecatonInstance instance) {
        isNotNull(instance, "instance");

        for (;;) {
            HecatonInstance[] oldInstances = ref.get();
            if (oldInstances.length == MAX_REGISTERED_INSTANCES) {
                return;
            }

            HecatonInstance[] newInstances = new HecatonInstance[oldInstances.length + 1];
            arraycopy(oldInstances, 0, newInstances, 0, oldInstances.length);
            newInstances[oldInstances.length] = instance;

            if (ref.compareAndSet(oldInstances, newInstances)) {
                return;
            }
        }
    }

    public static void deregisterServer(HecatonInstance instance) {
        deregister(SERVER_INSTANCES_REF, instance);
    }

    public static void deregisterClient(HecatonInstance instance) {
        deregister(CLIENT_INSTANCES_REF, instance);
    }

    private static void deregister(AtomicReference<HecatonInstance[]> ref, HecatonInstance instance) {
        isNotNull(instance, "instance");

        for (;;) {
            HecatonInstance[] oldInstances = ref.get();
            int indexOf = indexOf(oldInstances, instance);
            if (indexOf == -1) {
                return;
            }

            HecatonInstance[] newInstances;
            if (oldInstances.length == 1) {
                newInstances = EMPTY_INSTANCES;
            } else {
                newInstances = new HecatonInstance[oldInstances.length - 1];
                arraycopy(oldInstances, 0, newInstances, 0, indexOf);
                if (indexOf < newInstances.length) {
                    arraycopy(oldInstances, indexOf + 1, newInstances, indexOf, newInstances.length - indexOf);
                }
            }

            if (ref.compareAndSet(oldInstances, newInstances)) {
                return;
            }
        }
    }

    private static int indexOf(HecatonInstance[] instances, HecatonInstance instance) {
        for (int k = 0; k < instances.length; k++) {
            if (instance == instances[k]) {
                return k;
            }
        }
        return -1;
    }

    public static void clearServers() {
        SERVER_INSTANCES_REF.set(EMPTY_INSTANCES);
    }

    public static void clearClients() {
        CLIENT_INSTANCES_REF.set(EMPTY_INSTANCES);
    }

    public static void inspectOutOfMemoryError(Throwable throwable) {
        if (throwable == null) {
            return;
        }

        if (throwable instanceof OutOfMemoryError error) {
            onOutOfMemory(error);
        }
    }

    /**
     * Signals the OutOfMemoryErrorDispatcher that an OutOfMemoryError happened.
     * <p>
     * If there are any registered instances, they are automatically unregistered. This is done to prevent creating
     * new objects during the deregistration process while the system is suffering from a shortage of memory.
     *
     * @param outOfMemoryError the out of memory error
     */
    public static void onOutOfMemory(OutOfMemoryError outOfMemoryError) {
        isNotNull(outOfMemoryError, "outOfMemoryError");

        OUT_OF_MEMORY_ERROR_COUNT.incrementAndGet();

        OutOfMemoryHandler h = clientHandler;
        if (h != null && h.shouldHandle(outOfMemoryError)) {
            try {
                HecatonInstance[] clients = removeRegisteredClients();
                h.onOutOfMemory(outOfMemoryError, clients);
            } catch (Throwable ignored) {
                ignore(ignored);
            }
        }

        h = handler;
        if (h != null && h.shouldHandle(outOfMemoryError)) {
            try {
                HecatonInstance[] instances = removeRegisteredServers();
                h.onOutOfMemory(outOfMemoryError, instances);
            } catch (Throwable ignored) {
                ignore(ignored);
            }
        }
    }

    private static HecatonInstance[] removeRegisteredServers() {
        return removeRegisteredInstances(SERVER_INSTANCES_REF);
    }

    private static HecatonInstance[] removeRegisteredClients() {
        return removeRegisteredInstances(CLIENT_INSTANCES_REF);
    }

    private static HecatonInstance[] removeRegisteredInstances(AtomicReference<HecatonInstance[]> ref) {
        for (;;) {
            HecatonInstance[] instances = ref.get();
            if (ref.compareAndSet(instances, EMPTY_INSTANCES)) {
                return instances;
            }
        }
    }

    private static class EmptyOutOfMemoryHandler extends OutOfMemoryHandler {
        @Override
        public void onOutOfMemory(OutOfMemoryError oome, HecatonInstance[] hecatonInstances) {
        }

        @Override
        public boolean shouldHandle(OutOfMemoryError oome) {
            return false;
        }
    }

    // Only used for testing purposes
    public static AtomicReference<HecatonInstance[]> getClientInstancesRef() {
        return CLIENT_INSTANCES_REF;
    }
}
