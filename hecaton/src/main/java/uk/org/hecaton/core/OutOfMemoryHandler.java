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

package uk.org.hecaton.core;

import uk.org.hecaton.instance.impl.OutOfMemoryHandlerHelper;

/**
 * Handler for <code>OutOfMemoryError</code>.
 * <p>
 * When an <code>OutOfMemoryError</code> is caught by Hecaton threads,
 * <code>OutOfMemoryHandler</code> is called for ALL <code>HecatonInstance</code>s
 * known by the current JVM (actually ClassLoader).
 * </p>
 *
 * <p>
 * <b>Warning: </b> <code>OutOfMemoryHandler</code> may not be called even if JVM throws
 * <code>OutOfMemoryError</code>
 * because the error may be thrown from an external (user) thread, so
 * Hecaton may not be informed about <code>OutOfMemoryError</code>.
 * </p>
 *
 * @see OutOfMemoryError
 * @see Hecaton#setOutOfMemoryHandler(OutOfMemoryHandler)
 *
 */
public abstract class OutOfMemoryHandler {

    /**
     * When an <code>OutOfMemoryError</code> is caught by Hecaton threads,
     * this method is called for ALL <code>HecatonInstance</code>s
     * knows by current JVM (actually ClassLoader).
     *
     * <p>
     * User can shutdown the <code>HecatonInstance</code>, call <code>System.exit()</code>,
     * just log the error, etc.
     * The default handler tries to close socket connections to other nodes and shutdown the
     * <code>HecatonInstance</code>.
     * </p>
     *
     * <p>
     * <b>Warning: </b> <code>OutOfMemoryHandler</code> may not be called even if JVM throws
     * <code>OutOfMemoryError</code>
     * because the error may be thrown from an external (user) thread
     * and Hecaton may not be informed about <code>OutOfMemoryError</code>.
     * </p>
     *
     * @see OutOfMemoryHandler#tryCloseConnections(HecatonInstance)
     * @see OutOfMemoryHandler#tryShutdown(HecatonInstance)
     *
     * @param oome OutOfMemoryError thrown by JVM
     * @param hecatonInstances All HecatonInstances known by JVM,
     *                           can include inactive or NULL instances.
     */
    public abstract void onOutOfMemory(OutOfMemoryError oome, HecatonInstance[] hecatonInstances);

    /**
     * Decides if the OutOfMemoryError thrown will be handled by this OutOfMemoryHandler or not.
     * If <code>true</code> is return, {@link #onOutOfMemory(OutOfMemoryError, HecatonInstance[])} will be called
     * to handle error, otherwise OutOfMemoryError will be ignored.
     *
     * @param oome OutOfMemoryError thrown by JVM
     * @return true if OutOfMemoryError will be handled, false otherwise
     */
    public boolean shouldHandle(OutOfMemoryError oome) {
        return true;
    }

    /**
     * Tries to close the server socket and connections to other <code>HecatonInstance</code>s.
     *
     * @param hecatonInstance the Hecaton instance to close server socket
     */
    protected final void tryCloseConnections(HecatonInstance hecatonInstance) {
        OutOfMemoryHandlerHelper.tryCloseConnections(hecatonInstance);
    }

    /**
     * Tries to shut down <code>HecatonInstance</code> forcefully;
     * including closing sockets and connections, stopping threads, etc.
     *
     * @param hecatonInstance the Hecaton instance to shut down
     */
    protected final void tryShutdown(final HecatonInstance hecatonInstance) {
        OutOfMemoryHandlerHelper.tryShutdown(hecatonInstance);
    }
}
