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
package uk.org.hecaton.security.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.serialization.SerializableByConvention;
import uk.org.hecaton.internal.services.NodeAware;
import uk.org.hecaton.security.SecureCallable;

import javax.security.auth.Subject;
import java.util.concurrent.Callable;

/**
 * A client's task (executor service, scheduled executor) that runs on a member as the client:
 * while it runs, {@link SecurityContextImpl#isThreadRunningClientTask()} is {@code true}.
 */
// wraps the user's Callable, which executor services Java-serialize by convention
@SerializableByConvention
final class SecureCallableImpl<V> implements SecureCallable<V> {

    private static final long serialVersionUID = 1L;

    private final Subject subject;
    private final Callable<V> callable;
    private transient Node node;

    SecureCallableImpl(Subject subject, Callable<V> callable) {
        this.subject = subject;
        this.callable = callable;
    }

    @Override
    public V call() throws Exception {
        SecurityContextImpl securityContext = node != null && node.securityContext instanceof SecurityContextImpl s ? s : null;
        if (securityContext == null) {
            return callable.call();
        }
        return securityContext.runAsClientTask(subject, callable);
    }

    @Override
    public void setHecatonInstance(HecatonInstance instance) {
        if (callable instanceof HecatonInstanceAware aware) {
            aware.setHecatonInstance(instance);
        }
    }

    @Override
    public void setNode(Node node) {
        this.node = node;
        if (callable instanceof NodeAware aware) {
            aware.setNode(node);
        }
    }
}
