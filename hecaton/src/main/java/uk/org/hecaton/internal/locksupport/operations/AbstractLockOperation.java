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

package uk.org.hecaton.internal.locksupport.operations;

import uk.org.hecaton.internal.locksupport.LockDataSerializerHook;
import uk.org.hecaton.internal.locksupport.LockSupportServiceImpl;
import uk.org.hecaton.internal.locksupport.LockStoreImpl;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.internal.services.LockInterceptorService;
import uk.org.hecaton.spi.impl.operationservice.NamedOperation;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.internal.services.ServiceNamespaceAware;
import uk.org.hecaton.internal.tpcengine.util.ReflectionUtil;

import java.io.IOException;
import java.lang.invoke.VarHandle;

public abstract class AbstractLockOperation extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable, NamedOperation,
        ServiceNamespaceAware {

    public static final int ANY_THREAD = 0;

    private static final VarHandle REFERENCE_CALL_ID = ReflectionUtil.findVarHandle("referenceCallId", long.class);

    protected ObjectNamespace namespace;
    protected Data key;
    protected long threadId;
    protected long leaseTime = -1L;
    protected boolean isClient;
    protected transient Object response;
    private volatile long referenceCallId;
    private transient boolean asyncBackup;

    public AbstractLockOperation() {
    }

    protected AbstractLockOperation(ObjectNamespace namespace, Data key, long threadId) {
        this.namespace = namespace;
        this.key = key;
        this.threadId = threadId;
    }

    protected AbstractLockOperation(ObjectNamespace namespace, Data key, long threadId, long timeout) {
        this.namespace = namespace;
        this.key = key;
        this.threadId = threadId;
        setWaitTimeout(timeout);
    }

    public AbstractLockOperation(ObjectNamespace namespace, Data key, long threadId, long leaseTime, long timeout) {
        this.namespace = namespace;
        this.key = key;
        this.threadId = threadId;
        this.leaseTime = leaseTime;
        setWaitTimeout(timeout);
    }

    @Override
    public final Object getResponse() {
        return response;
    }

    protected final LockStoreImpl getLockStore() {
        LockSupportServiceImpl service = getService();
        return service.getLockStore(getPartitionId(), namespace);
    }

    public final int getSyncBackupCount() {
        if (asyncBackup) {
            return 0;
        } else {
            return getLockStore().getBackupCount();
        }
    }

    public final int getAsyncBackupCount() {
        LockStoreImpl lockStore = getLockStore();
        if (asyncBackup) {
            return lockStore.getBackupCount() + lockStore.getAsyncBackupCount();
        } else {
            return lockStore.getAsyncBackupCount();
        }
    }

    public final void setAsyncBackup(boolean asyncBackup) {
        this.asyncBackup = asyncBackup;
    }

    @Override
    protected void onSetCallId(long callId) {
        REFERENCE_CALL_ID.compareAndSet(this, 0, callId);
    }

    protected final long getReferenceCallId() {
        return referenceCallId != 0 ? referenceCallId : getCallId();
    }

    protected final void setReferenceCallId(long refCallId) {
        this.referenceCallId = refCallId;
    }

    protected final void interceptLockOperation() {
        // if service is a LockInterceptorService, notify it a key is about to be locked
        Object targetService = getNodeEngine().getService(namespace.getServiceName());
        if (targetService instanceof LockInterceptorService service) {
            service.onBeforeLock(namespace.getObjectName(), key);
        }
    }

    @Override
    public String getServiceName() {
        return LockSupportServiceImpl.SERVICE_NAME;
    }

    public final Data getKey() {
        return key;
    }

    @Override
    public String getName() {
        return namespace.getObjectName();
    }

    @Override
    public ServiceNamespace getServiceNamespace() {
        return namespace;
    }

    @Override
    public final int getFactoryId() {
        return LockDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(namespace);
        IOUtil.writeData(out, key);
        out.writeLong(threadId);
        out.writeLong(leaseTime);
        out.writeLong(referenceCallId);
        out.writeBoolean(isClient);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        namespace = in.readObject();
        key = IOUtil.readData(in);
        threadId = in.readLong();
        leaseTime = in.readLong();
        referenceCallId = in.readLong();
        isClient = in.readBoolean();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", namespace=").append(namespace);
        sb.append(", threadId=").append(threadId);
    }
}
