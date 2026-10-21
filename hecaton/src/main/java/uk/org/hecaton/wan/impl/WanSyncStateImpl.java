/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.wan.impl;

import uk.org.hecaton.internal.monitor.WanSyncState;

/** The WAN sync state of a member: whether a sync runs or failed, and of which publisher. */
record WanSyncStateImpl(WanSyncStatus status, String activeWanConfigName, String activePublisherName,
                        long creationTime) implements WanSyncState {

    WanSyncStateImpl(WanSyncStatus status, String activeWanConfigName, String activePublisherName) {
        this(status, activeWanConfigName, activePublisherName, System.currentTimeMillis());
    }

    @Override
    public WanSyncStatus getStatus() {
        return status;
    }

    @Override
    public String getActiveWanConfigName() {
        return activeWanConfigName;
    }

    @Override
    public String getActivePublisherName() {
        return activePublisherName;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }
}
