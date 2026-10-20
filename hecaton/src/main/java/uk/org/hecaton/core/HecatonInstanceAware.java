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

/**
 * Used to get HecatonInstance reference when submitting a Runnable/Callable using Hecaton ExecutorService.
 * Before executing the Runnable/Callable Hecaton will invoke
 * {@link HecatonInstanceAware#setHecatonInstance(HecatonInstance)}  method with the reference to HecatonInstance
 * that is executing. This way the implementer will have a chance to get the reference to HecatonInstance.
 */
public interface HecatonInstanceAware {

    /**
     * Gets the HecatonInstance reference when submitting a Runnable/Callable using Hecaton ExecutorService.
     *
     * @param hecatonInstance the HecatonInstance reference
     */
    void setHecatonInstance(HecatonInstance hecatonInstance);
}
