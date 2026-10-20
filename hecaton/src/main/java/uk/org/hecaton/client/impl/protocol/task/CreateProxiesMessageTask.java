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

package uk.org.hecaton.client.impl.protocol.task;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.ClientCreateProxiesCodec;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityContext;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.proxyservice.ProxyService;
import uk.org.hecaton.spi.impl.proxyservice.impl.ProxyInfo;
import uk.org.hecaton.spi.impl.proxyservice.impl.operations.PostJoinProxyOperation;

import java.security.AccessControlException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;


public class CreateProxiesMessageTask extends AbstractMultiTargetMessageTask<List<Map.Entry<String, String>>>
        implements Supplier<Operation> {

    private List<Map.Entry<String, String>> filteredProxies;

    public CreateProxiesMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Supplier<Operation> createOperationSupplier() {
        return this;
    }

    @Override
    public Operation get() {
        List<ProxyInfo> proxyInfos = new ArrayList<>(filteredProxies.size());
        for (Map.Entry<String, String> proxy : filteredProxies) {
            proxyInfos.add(new ProxyInfo(proxy.getValue(), proxy.getKey(), endpoint.getUuid()));
        }
        return new PostJoinProxyOperation(proxyInfos);
    }

    @Override
    protected Object reduce(Map<Member, Object> map) throws Throwable {
        for (Object result : map.values()) {
            if (result instanceof Throwable throwable && !(result instanceof MemberLeftException)) {
                throw throwable;
            }
        }
        return null;
    }

    @Override
    public Collection<Member> getTargets() {
        return nodeEngine.getClusterService().getMembers();
    }

    @Override
    protected List<Map.Entry<String, String>> decodeClientMessage(ClientMessage clientMessage) {
        return ClientCreateProxiesCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ClientCreateProxiesCodec.encodeResponse();
    }

    /**
     *@see #beforeProcess()
     */
    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    protected void beforeProcess() {
        // replacement for getRequiredPermission-based checks, we have to check multiple permission
        SecurityContext securityContext = clientEngine.getSecurityContext();
        if (securityContext != null) {
            filteredProxies = new ArrayList<>(parameters.size());
            ProxyService proxyService = clientEngine.getProxyService();
            for (Map.Entry<String, String> proxy : parameters) {
                String objectName = proxy.getKey();
                String serviceName = proxy.getValue();
                if (proxyService.existsDistributedObject(serviceName, objectName)) {
                    continue;
                }
                try {
                    Permission permission = ActionConstants.getPermission(objectName, serviceName,
                            ActionConstants.ACTION_CREATE);
                    securityContext.checkPermission(endpoint.getSubject(), permission);
                    filteredProxies.add(proxy);
                } catch (AccessControlException ace) {
                    logger.info("Insufficient client permissions. Proxy won't be created for type '" + serviceName + "': "
                            + objectName);
                    if (logger.isFineEnabled()) {
                        logger.fine("Skipping proxy creation due to AccessControlException", ace);
                    }
                } catch (Exception e) {
                    // unknown serviceName or another unexpected issue
                    logger.warning("Proxy won't be created for type '" + serviceName + "': " + objectName, e);
                }
            }
        } else {
            filteredProxies = parameters;
        }
        super.beforeProcess();
    }

    @Override
    public String getServiceName() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.CREATE_PROXIES;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters};
    }
}
