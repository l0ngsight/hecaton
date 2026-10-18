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

package uk.org.hecaton.internal.util.executor;

import uk.org.hecaton.instance.impl.OutOfMemoryErrorDispatcher;
import uk.org.hecaton.internal.util.ThreadAffinity;
import uk.org.hecaton.internal.util.ThreadAffinityHelper;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;

import java.util.BitSet;

/**
 * Base class for all Hecaton threads to manage them from a single point.
 * Concrete classes can customize their behaviours by overriding
 * {@link uk.org.hecaton.internal.util.executor.HecatonManagedThread#beforeRun},
 * {@link uk.org.hecaton.internal.util.executor.HecatonManagedThread#executeRun} and
 * {@link uk.org.hecaton.internal.util.executor.HecatonManagedThread#afterRun} methods.
 */
public class HecatonManagedThread extends Thread {

    private BitSet allowedCpus;

    public HecatonManagedThread() {
    }

    public HecatonManagedThread(Runnable target) {
        super(target);
    }

    public HecatonManagedThread(String name) {
        super(name);
    }

    public HecatonManagedThread(Runnable target, String name) {
        super(target, name);
    }

    public void setThreadAffinity(ThreadAffinity threadAffinity) {
        this.allowedCpus = threadAffinity.nextAllowedCpus();
    }

    @Override
    public void setContextClassLoader(ClassLoader cl) {
        // Set only if specified classloader is not empty, otherwise go one with current
        if (cl != null) {
            super.setContextClassLoader(cl);
        }
    }

    /**
     * Called before inner run method and can be overridden to customize.
     */
    protected void beforeRun() {

    }

    /**
     * Does the actual run and can be overridden to customize.
     */
    protected void executeRun() {
        super.run();
    }

    /**
     * Called after inner run method and can be overridden to customize.
     */
    protected void afterRun() {

    }

    protected void beforeStart() {
    }

    @Override
    public synchronized void start() {
        beforeStart();
        super.start();
    }

    @Override
    public final void run() {
        if (allowedCpus != null) {
            ThreadAffinityHelper.setAffinity(allowedCpus);
            BitSet actualCpus = ThreadAffinityHelper.getAffinity();
            ILogger logger = Logger.getLogger(HecatonManagedThread.class);
            if (!actualCpus.equals(allowedCpus)) {
                logger.warning(getName() + " affinity was not applied successfully. "
                        + "Expected CPUs:" + allowedCpus + ". Actual CPUs:" + actualCpus);
            } else {
                logger.info(getName() + " has affinity for CPUs:" + allowedCpus);
            }
        }

        try {
            beforeRun();
            executeRun();
        } catch (OutOfMemoryError e) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(e);
        } finally {
            afterRun();
        }
    }
}

