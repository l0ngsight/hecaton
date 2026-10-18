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

import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.core.LifecycleService;

import javax.annotation.Nonnull;
import java.util.UUID;

public final class TerminatedLifecycleService implements LifecycleService {

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void terminate() {
    }

    @Nonnull
    @Override
    public UUID addLifecycleListener(@Nonnull LifecycleListener lifecycleListener) {
        throw new HecatonInstanceNotActiveException();
    }

    @Override
    public boolean removeLifecycleListener(@Nonnull UUID registrationId) {
        throw new HecatonInstanceNotActiveException();
    }
}
