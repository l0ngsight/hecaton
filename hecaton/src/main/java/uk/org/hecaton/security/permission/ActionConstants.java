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

package uk.org.hecaton.security.permission;

import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.cardinality.impl.CardinalityEstimatorService;
import uk.org.hecaton.collection.impl.list.ListService;
import uk.org.hecaton.collection.impl.queue.QueueService;
import uk.org.hecaton.collection.impl.set.SetService;
import uk.org.hecaton.cp.internal.datastructures.atomiclong.AtomicLongServiceUtil;
import uk.org.hecaton.cp.internal.datastructures.atomicref.AtomicRefServiceUtil;
import uk.org.hecaton.cp.internal.datastructures.countdownlatch.CountDownLatchServiceUtil;
import uk.org.hecaton.cp.internal.datastructures.cpmap.CPMapServiceUtil;
import uk.org.hecaton.cp.internal.datastructures.lock.LockServiceUtil;
import uk.org.hecaton.cp.internal.datastructures.semaphore.SemaphoreServiceUtil;
import uk.org.hecaton.durableexecutor.impl.DistributedDurableExecutorService;
import uk.org.hecaton.executor.impl.DistributedExecutorService;
import uk.org.hecaton.flakeidgen.impl.FlakeIdGeneratorService;
import uk.org.hecaton.internal.crdt.pncounter.PNCounterService;
import uk.org.hecaton.internal.locksupport.LockSupportService;
import uk.org.hecaton.internal.namespace.UserCodeNamespaceService;
import uk.org.hecaton.internal.usercodedeployment.UserCodeDeploymentService;
import uk.org.hecaton.jet.impl.JetServiceBackend;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.multimap.impl.MultiMapService;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService;
import uk.org.hecaton.sql.impl.InternalSqlService;
import uk.org.hecaton.topic.impl.TopicService;
import uk.org.hecaton.topic.impl.reliable.ReliableTopicService;
import uk.org.hecaton.vector.impl.VectorCollectionServiceUtil;

import java.security.Permission;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"checkstyle:executablestatementcount", "ClassDataAbstractionCoupling"})
public final class ActionConstants {

    public static final String ACTION_ALL = "all";
    public static final String ACTION_CREATE = "create";
    public static final String ACTION_DESTROY = "destroy";
    public static final String ACTION_MODIFY = "modify";
    public static final String ACTION_READ = "read";
    public static final String ACTION_REMOVE = "remove";
    public static final String ACTION_LOCK = "lock";
    public static final String ACTION_LISTEN = "listen";
    public static final String ACTION_RELEASE = "release";
    public static final String ACTION_ACQUIRE = "acquire";
    public static final String ACTION_PUT = "put";
    public static final String ACTION_ADD = "add";
    public static final String ACTION_INDEX = "index";
    public static final String ACTION_INTERCEPT = "intercept";
    public static final String ACTION_PUBLISH = "publish";
    public static final String ACTION_AGGREGATE = "aggregate";
    public static final String ACTION_PROJECTION = "projection";
    public static final String ACTION_USER_CODE_DEPLOY = "deploy";
    public static final String ACTION_USE = "use";
    public static final String ACTION_OPTIMIZE = "optimize";

    public static final String ACTION_SUBMIT = "submit";
    public static final String ACTION_CANCEL = "cancel";
    public static final String ACTION_RESTART = "restart";
    public static final String ACTION_EXPORT_SNAPSHOT = "export-snapshot";
    public static final String ACTION_ADD_RESOURCES = "add-resources";
    public static final String ACTION_WRITE = "write";

    public static final String LISTENER_INSTANCE = "instance";
    public static final String LISTENER_MEMBER = "member";
    public static final String LISTENER_MIGRATION = "migration";

    // SQL-specific actions
    public static final String ACTION_VIEW_MAPPING = "view-mapping";
    public static final String ACTION_CREATE_VIEW = "create-view";
    public static final String ACTION_DROP_VIEW = "drop-view";
    public static final String ACTION_CREATE_TYPE = "create-type";
    public static final String ACTION_DROP_TYPE = "drop-type";
    public static final String ACTION_VIEW_DATACONNECTION = "view-dataconnection";
    public static final String ACTION_CREATE_DATACONNECTION = "create-dataconnection";
    public static final String ACTION_DROP_DATACONNECTION = "drop-dataconnection";

    private static final Map<String, PermissionFactory> PERMISSION_FACTORY_MAP = new HashMap<>();

    static {
        PERMISSION_FACTORY_MAP.put(QueueService.SERVICE_NAME, QueuePermission::new);
        PERMISSION_FACTORY_MAP.put(MapService.SERVICE_NAME, MapPermission::new);
        PERMISSION_FACTORY_MAP.put(MultiMapService.SERVICE_NAME, MultiMapPermission::new);
        PERMISSION_FACTORY_MAP.put(ListService.SERVICE_NAME, ListPermission::new);
        PERMISSION_FACTORY_MAP.put(SetService.SERVICE_NAME, SetPermission::new);
        PERMISSION_FACTORY_MAP.put(AtomicLongServiceUtil.SERVICE_NAME, AtomicLongPermission::new);
        PERMISSION_FACTORY_MAP.put(CountDownLatchServiceUtil.SERVICE_NAME, CountDownLatchPermission::new);
        PERMISSION_FACTORY_MAP.put(SemaphoreServiceUtil.SERVICE_NAME, SemaphorePermission::new);
        PERMISSION_FACTORY_MAP.put(TopicService.SERVICE_NAME, TopicPermission::new);
        PERMISSION_FACTORY_MAP.put(LockSupportService.SERVICE_NAME, LockPermission::new);
        PERMISSION_FACTORY_MAP.put(LockServiceUtil.SERVICE_NAME, LockPermission::new);
        PERMISSION_FACTORY_MAP.put(DistributedExecutorService.SERVICE_NAME, ExecutorServicePermission::new);
        PERMISSION_FACTORY_MAP.put(FlakeIdGeneratorService.SERVICE_NAME, FlakeIdGeneratorPermission::new);
        PERMISSION_FACTORY_MAP.put(ReplicatedMapService.SERVICE_NAME, ReplicatedMapPermission::new);
        PERMISSION_FACTORY_MAP.put(AtomicRefServiceUtil.SERVICE_NAME, AtomicReferencePermission::new);
        PERMISSION_FACTORY_MAP.put(ICacheService.SERVICE_NAME, CachePermission::new);
        PERMISSION_FACTORY_MAP.put(RingbufferService.SERVICE_NAME, RingBufferPermission::new);
        PERMISSION_FACTORY_MAP.put(DistributedDurableExecutorService.SERVICE_NAME, DurableExecutorServicePermission::new);
        PERMISSION_FACTORY_MAP.put(CardinalityEstimatorService.SERVICE_NAME, CardinalityEstimatorPermission::new);
        PERMISSION_FACTORY_MAP.put(UserCodeDeploymentService.SERVICE_NAME,
                (name, actions) -> new UserCodeDeploymentPermission(actions));
        PERMISSION_FACTORY_MAP.put(PNCounterService.SERVICE_NAME, PNCounterPermission::new);
        PERMISSION_FACTORY_MAP.put(ReliableTopicService.SERVICE_NAME, ReliableTopicPermission::new);
        PERMISSION_FACTORY_MAP.put(JetServiceBackend.SERVICE_NAME, (name, actions) -> new JobPermission(actions));
        PERMISSION_FACTORY_MAP.put(InternalSqlService.SERVICE_NAME, SqlPermission::new);
        PERMISSION_FACTORY_MAP.put(DistributedScheduledExecutorService.SERVICE_NAME, ScheduledExecutorPermission::new);
        PERMISSION_FACTORY_MAP.put(CPMapServiceUtil.SERVICE_NAME, CPMapPermission::new);
        PERMISSION_FACTORY_MAP.put(UserCodeNamespaceService.SERVICE_NAME, UserCodeNamespacePermission::new);
        PERMISSION_FACTORY_MAP.put(VectorCollectionServiceUtil.SERVICE_NAME, VectorCollectionPermission::new);
    }

    private ActionConstants() {
    }

    private interface PermissionFactory {
        Permission create(String name, String... actions);
    }

    /**
     * Creates a permission
     *
     * @param name        the permission name
     * @param serviceName the service name
     * @param actions     the actions
     * @return the created Permission
     * @throws java.lang.IllegalArgumentException if there is no service found with the given serviceName.
     */
    public static Permission getPermission(String name, String serviceName, String... actions) {
        PermissionFactory permissionFactory = PERMISSION_FACTORY_MAP.get(serviceName);
        if (permissionFactory == null) {
            throw new IllegalArgumentException("No permissions found for service: " + serviceName);
        }

        return permissionFactory.create(name, actions);
    }

}
