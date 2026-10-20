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

import uk.org.hecaton.config.OnJoinPermissionOperationName;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.management.operation.UpdatePermissionConfigOperation;
import uk.org.hecaton.internal.services.PostJoinAwareService;
import uk.org.hecaton.spi.impl.operationservice.Operation;

/**
 * Keeps the client permissions the same on all members when members join, according to
 * {@code on-join-operation}. Post-join operations travel both ways: the master's to the joining
 * member, the joining member's back to the master (which passes them on), and a member applies
 * permissions it receives only if its own setting is {@code RECEIVE}. So:
 * <ul>
 *     <li>the master contributes the cluster's permissions, which a joining member set to
 *     {@code RECEIVE} (the default) applies;</li>
 *     <li>a joining member set to {@code SEND} contributes its own permissions, which the
 *     members set to {@code RECEIVE} apply;</li>
 *     <li>with {@code NONE} a member neither applies nor sends anything.</li>
 * </ul>
 */
public class PermissionSyncService implements PostJoinAwareService {

    /** Service name. */
    public static final String SERVICE_NAME = "hc:impl:securityPermissionSync";

    private final Node node;

    public PermissionSyncService(Node node) {
        this.node = node;
    }

    @Override
    public Operation getPostJoinOperation() {
        if (!(node.securityContext instanceof SecurityContextImpl context)) {
            return null;
        }
        boolean master = node.isMaster();
        boolean send = node.getConfig().getSecurityConfig().getOnJoinPermissionOperation()
                == OnJoinPermissionOperationName.SEND;
        return master || send ? new UpdatePermissionConfigOperation(context.getClientPermissions()) : null;
    }
}
