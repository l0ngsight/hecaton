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

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.PermissionConfig;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.management.operation.UpdatePermissionConfigOperation;
import uk.org.hecaton.security.SecurityService;
import uk.org.hecaton.spi.impl.operationservice.OperationService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;

/**
 * Changes the client permissions of the whole cluster at runtime (dynamic configuration): every
 * member's {@link SecurityContextImpl} gets the new permissions.
 */
public class SecurityServiceImpl implements SecurityService {

    private final Node node;

    public SecurityServiceImpl(Node node) {
        this.node = node;
    }

    @Override
    public void refreshClientPermissions(Set<PermissionConfig> permissionConfigs) {
        Set<PermissionConfig> copy = new HashSet<>(permissionConfigs);
        OperationService operationService = node.getNodeEngine().getOperationService();
        List<CompletableFuture<Object>> futures = new ArrayList<>();
        for (Member member : node.getClusterService().getMembers()) {
            futures.add(operationService.invokeOnTarget(null, new UpdatePermissionConfigOperation(copy),
                    member.getAddress()).toCompletableFuture());
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public Set<PermissionConfig> getClientPermissionConfigs() {
        return ((SecurityContextImpl) node.securityContext).getClientPermissions();
    }
}
