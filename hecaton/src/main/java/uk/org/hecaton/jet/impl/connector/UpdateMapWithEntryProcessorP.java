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

package uk.org.hecaton.jet.impl.connector;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.internal.namespace.impl.NodeEngineThreadLocalContext;
import uk.org.hecaton.jet.core.Inbox;
import uk.org.hecaton.jet.core.JetDataSerializerHook;
import uk.org.hecaton.jet.core.Outbox;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.security.impl.InternalSecurityContext;
import uk.org.hecaton.spi.impl.NodeEngine;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serial;
import java.util.Map;
import java.util.Objects;

import static uk.org.hecaton.internal.namespace.NamespaceUtil.callWithNamespace;

public final class UpdateMapWithEntryProcessorP<T, K, V, R> extends AsyncHecatonWriterP {

    private final IMap<K, V> map;
    private final FunctionEx<? super T, ? extends K> toKeyFn;
    private final FunctionEx<? super T, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn;
    private String userCodeNamespace;

    public UpdateMapWithEntryProcessorP(
        @Nonnull HecatonInstance instance,
        int maxParallelAsyncOps,
        @Nonnull String name,
        @Nonnull FunctionEx<? super T, ? extends K> toKeyFn,
        @Nonnull FunctionEx<? super T, ? extends EntryProcessor<K, V, R>> toEntryProcessorFn) {
        super(instance, maxParallelAsyncOps);
        this.map = instance.getMap(name);
        this.toKeyFn = toKeyFn;
        this.toEntryProcessorFn = toEntryProcessorFn;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context)
            throws Exception {
        super.init(outbox, context);
        this.userCodeNamespace = context.jobConfig().getUserCodeNamespace();
    }

    @Override
    protected void processInternal(Inbox inbox) {
        int permits = tryAcquirePermits(inbox.size());
        for (Object object; permits > 0 && (object = inbox.peek()) != null; permits--) {
            @SuppressWarnings("unchecked") T item = (T) object;
            EntryProcessor<K, V, R> entryProcessor = toEntryProcessorFn.apply(item);
            EntryProcessor<K, V, R> entryProcessorForSubmission =
                    userCodeNamespace != null && isLocal() ? new NamespaceAwareEntryProcessor<>(entryProcessor,
                            userCodeNamespace) : entryProcessor;
            K key = toKeyFn.apply(item);
            setCallback(map.submitToKey(key, entryProcessorForSubmission));
            inbox.remove();
        }
    }

    public static class NamespaceAwareEntryProcessor<K, V, T>
            implements EntryProcessor<K, V, T>, IdentifiedDataSerializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private EntryProcessor<K, V, T> delegate;
        private String userCodeNamespace;

        public NamespaceAwareEntryProcessor() {
        }

        public NamespaceAwareEntryProcessor(EntryProcessor<K, V, T> delegate, @Nonnull String userCodeNamespace) {
            this.delegate = delegate;
            this.userCodeNamespace = Objects.requireNonNull(userCodeNamespace);
        }

        @Override
        public T process(Map.Entry<K, V> entry) {
            return delegate.process(entry);
        }

        @Nullable
        @Override
        public EntryProcessor<K, V, T> getBackupProcessor() {
            var backupDelegate = delegate.getBackupProcessor();
            if (backupDelegate == null) {
                // ReadOnly processor
                return null;
            }
            if (backupDelegate == delegate) {
                return this;
            }
            return new NamespaceAwareEntryProcessor<>(backupDelegate, userCodeNamespace);
        }

        @Serial
        private void readObject(java.io.ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            throw new UnsupportedOperationException("Java deserialization disabled for security reasons for" + getClass());
        }

        @Override
        public int getFactoryId() {
            return JetDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetDataSerializerHook.NAMESPACE_AWARE_ENTRY_PROCESSOR;
        }

        @Override
        public void writeData(ObjectDataOutput out)
                throws IOException {
            out.writeString(userCodeNamespace);
            out.writeObject(delegate);
        }

        @Override
        public void readData(ObjectDataInput in)
                throws IOException {
            NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
            InternalSecurityContext securityContext = engine.getNode().getNodeExtension().getSecurityContextOrNull();
            if (securityContext != null && securityContext.isThreadRunningClientTask()) {
                throw new IllegalArgumentException(
                        "Clients are not permitted to use " + getClass() + " directly when security is enabled.");
            }
            userCodeNamespace = in.readString();
            if (userCodeNamespace == null) {
                throw new NullPointerException("Unexpected null namespace read from stream");
            }
            delegate = callWithNamespace(engine, userCodeNamespace, in::readObject);
        }
    }
}
