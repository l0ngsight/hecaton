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

package uk.org.hecaton.transaction;

import uk.org.hecaton.core.DistributedObject;

import javax.annotation.Nonnull;
import javax.transaction.xa.XAResource;

/**
 * Interface for providing Hecaton as an XAResource
 */
public interface HecatonXAResource extends XAResource, DistributedObject {

    /**
     * Returns the TransactionContext associated with the current thread.
     *
     * @return TransactionContext associated with the current thread
     * @throws IllegalStateException if no context found
     */
    @Nonnull TransactionContext getTransactionContext();


}
