/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.client.cp.internal.datastructures.proxy;

import uk.org.hecaton.client.cp.internal.datastructures.atomiclong.AtomicLongProxy;
import uk.org.hecaton.client.cp.internal.datastructures.atomicref.AtomicRefProxy;
import uk.org.hecaton.client.cp.internal.datastructures.countdownlatch.CountDownLatchProxy;
import uk.org.hecaton.client.cp.internal.datastructures.cpmap.CPMapProxy;
import uk.org.hecaton.client.cp.internal.datastructures.lock.FencedLockProxy;
import uk.org.hecaton.client.cp.internal.datastructures.semaphore.SessionAwareSemaphoreProxy;
import uk.org.hecaton.client.cp.internal.datastructures.semaphore.SessionlessSemaphoreProxy;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CPGroupCreateCPGroupCodec;
import uk.org.hecaton.client.impl.protocol.codec.SemaphoreGetSemaphoreTypeCodec;
import uk.org.hecaton.client.impl.spi.ClientContext;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.cp.ISemaphore;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.datastructures.atomiclong.AtomicLongService;
import uk.org.hecaton.cp.internal.datastructures.atomicref.AtomicRefService;
import uk.org.hecaton.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import uk.org.hecaton.cp.internal.datastructures.cpmap.CPMapService;
import uk.org.hecaton.cp.internal.datastructures.lock.LockService;
import uk.org.hecaton.cp.internal.datastructures.semaphore.SemaphoreService;
import uk.org.hecaton.cp.lock.FencedLock;

import javax.annotation.Nonnull;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.cp.internal.RaftService.getObjectNameForProxy;
import static uk.org.hecaton.cp.internal.RaftService.withoutDefaultGroupName;

/**
 * Creates client-side proxies of the CP data structures
 */
public class ClientRaftProxyFactory {

    protected ClientContext context;
    private final HecatonClientInstanceImpl client;
    private final ConcurrentMap<String, FencedLockProxy> lockProxies
            = new ConcurrentHashMap<String, FencedLockProxy>();

    public ClientRaftProxyFactory(HecatonClientInstanceImpl client) {
        this.client = client;
    }

    public void init(ClientContext context) {
        this.context = context;
    }

    public @Nonnull
    <T extends DistributedObject> T createProxy(String serviceName, String proxyName) {
        proxyName = withoutDefaultGroupName(proxyName);
        String objectName = getObjectNameForProxy(proxyName);


        RaftGroupId groupId = getGroupId(proxyName, objectName);

        if (serviceName.equals(AtomicLongService.SERVICE_NAME)) {
            return (T) new AtomicLongProxy(context, groupId, proxyName, objectName);
        } else if (serviceName.equals(AtomicRefService.SERVICE_NAME)) {
            return (T) new AtomicRefProxy(context, groupId, proxyName, objectName);
        } else if (serviceName.equals(CountDownLatchService.SERVICE_NAME)) {
            return (T) new CountDownLatchProxy(context, groupId, proxyName, objectName);
        } else if (serviceName.equals(LockService.SERVICE_NAME)) {
            return (T) createFencedLock(groupId, proxyName, objectName);
        } else if (serviceName.equals(SemaphoreService.SERVICE_NAME)) {
            return (T) createSemaphore(groupId, proxyName, objectName);
        } else if (serviceName.equals(CPMapService.SERVICE_NAME)) {
            return (T) new CPMapProxy<>(context, groupId, proxyName, objectName);
        } else {
            throw new IllegalArgumentException();
        }
    }

    private FencedLock createFencedLock(RaftGroupId groupId, String proxyName, String objectName) {
        while (true) {
            FencedLockProxy proxy = lockProxies.get(proxyName);
            if (proxy != null) {
                if (!proxy.getGroupId().equals(groupId)) {
                    lockProxies.remove(proxyName, proxy);
                } else {
                    return proxy;
                }
            }

            proxy = new FencedLockProxy(context, groupId, proxyName, objectName);
            FencedLockProxy existing = lockProxies.putIfAbsent(proxyName, proxy);
            if (existing == null) {
                return proxy;
            } else if (existing.getGroupId().equals(groupId)) {
                return existing;
            }

            groupId = getGroupId(proxyName, objectName);
        }
    }

    private ISemaphore createSemaphore(RaftGroupId groupId, String proxyName, String objectName) {
        ClientMessage request = SemaphoreGetSemaphoreTypeCodec.encodeRequest(proxyName);
        ClientMessage response = new ClientInvocation(client, request, objectName).invoke().join();
        boolean jdkCompatible = SemaphoreGetSemaphoreTypeCodec.decodeResponse(response);

        return jdkCompatible
                ? new SessionlessSemaphoreProxy(context, groupId, proxyName, objectName)
                : new SessionAwareSemaphoreProxy(context, groupId, proxyName, objectName);
    }

    protected RaftGroupId getGroupId(String proxyName, String objectName) {
        ClientMessage request = CPGroupCreateCPGroupCodec.encodeRequest(proxyName);
        ClientMessage response = new ClientInvocation(client, request, objectName).invoke().joinInternal();
        return CPGroupCreateCPGroupCodec.decodeResponse(response);
    }

}
