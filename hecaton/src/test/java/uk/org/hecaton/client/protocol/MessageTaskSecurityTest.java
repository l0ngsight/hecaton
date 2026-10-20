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

package uk.org.hecaton.client.protocol;

import uk.org.hecaton.client.impl.protocol.task.AbstractMessageTask;
import uk.org.hecaton.client.impl.protocol.task.AddBackupListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.AddClusterViewListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.AddDistributedObjectListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.AddMigrationListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.AddPartitionLostListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.AuthenticationCustomCredentialsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.AuthenticationMessageTask;
import uk.org.hecaton.client.impl.protocol.task.ClientStatisticsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.CreateProxiesMessageTask;
import uk.org.hecaton.client.impl.protocol.task.ClientTpcAuthenticationMessageTask;
import uk.org.hecaton.client.impl.protocol.task.GetDistributedObjectsMessageTask;
import uk.org.hecaton.client.impl.protocol.task.MessageTask;
import uk.org.hecaton.client.impl.protocol.task.NoSuchMessageTask;
import uk.org.hecaton.client.impl.protocol.task.PingMessageTask;
import uk.org.hecaton.client.impl.protocol.task.RemoveDistributedObjectListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.RemoveMigrationListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.RemovePartitionLostListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.TriggerPartitionAssignmentMessageTask;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheFetchNearCacheInvalidationMetadataTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapAddListenerMessageTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapFetchNearCacheInvalidationMetadataTask;
import uk.org.hecaton.client.impl.protocol.task.map.MapMadePublishableMessageTask;
import uk.org.hecaton.client.impl.protocol.task.schema.FetchSchemaMessageTask;
import uk.org.hecaton.client.impl.protocol.task.schema.SendAllSchemasMessageTask;
import uk.org.hecaton.client.impl.protocol.task.schema.SendSchemaMessageTask;
import uk.org.hecaton.sql.impl.client.SqlCloseMessageTask;
import uk.org.hecaton.sql.impl.client.SqlExecuteMessageTask;
import uk.org.hecaton.sql.impl.client.SqlFetchMessageTask;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import javassist.ClassPool;
import javassist.bytecode.ClassFile;
import javassist.bytecode.CodeAttribute;
import javassist.bytecode.CodeIterator;
import javassist.bytecode.MethodInfo;
import javassist.bytecode.Mnemonic;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Verifies the {@code getRequiredPermission()} method doesn't simply return null in client {@link MessageTask} instances.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class})
public class MessageTaskSecurityTest {

    private static final String[] RETURN_NULL_OPS = { "aconst_null", "areturn" };
    private static final Map<String, String> SKIP_CLASS_MAP = new ConcurrentHashMap<>();

    static {
        skip(AddBackupListenerMessageTask.class,
                "Adds listener called for every invocation for ALL_MEMBERS routing clients if backupAcks are enabled");
        skip(AddClusterViewListenerMessageTask.class, "Adds listener for listening to member list and partition table changes");
        skip(AddDistributedObjectListenerMessageTask.class, "Adds distributed object listener by user's request");
        skip(AddMigrationListenerMessageTask.class, "Adds an internal listener");
        skip(AddPartitionLostListenerMessageTask.class, "Adds an internal listener");
        skip(CacheFetchNearCacheInvalidationMetadataTask.class, "Internal task used by RepairingTask");
        skip(ClientStatisticsMessageTask.class, "Client statistics collection task");
        skip(GetDistributedObjectsMessageTask.class, "Gets proxies");
        skip(MapAddListenerMessageTask.class, "Permissions checked by subsequent MapPublisherCreate* tasks");
        skip(MapFetchNearCacheInvalidationMetadataTask.class, "Internal task used by RepairingTask");
        skip(MapMadePublishableMessageTask.class, "Internal task used by RepairingTask");
        skip(NoSuchMessageTask.class, "Fallback MessageTask type - no cluster action performed");
        skip(PingMessageTask.class, "Heart beat type - no cluster action performed");
        skip(RemoveDistributedObjectListenerMessageTask.class, "Removes distributed object listener by user's request");
        skip(RemoveMigrationListenerMessageTask.class, "Adds an internal listener");
        skip(RemovePartitionLostListenerMessageTask.class, "Adds an internal listener");
        skip(FetchSchemaMessageTask.class, "Fetch compact-serialization schema");
        skip(SendAllSchemasMessageTask.class, "Send compact-serialization schemas");
        skip(SendSchemaMessageTask.class, "Send a compact-serialization schema");
        skip(TriggerPartitionAssignmentMessageTask.class, "Triggers first partition arrangement");
        skip(SqlExecuteMessageTask.class, "Permissions for specific objects are checked based on parsed query text");
        skip(SqlCloseMessageTask.class, "Follow up SQL message where queryId is present");
        skip(SqlFetchMessageTask.class, "Follow up SQL message where queryId is present");
        skip(AuthenticationMessageTask.class, "Authentication message processing");
        skip(AuthenticationCustomCredentialsMessageTask.class, "Authentication message processing");
        skip(ClientTpcAuthenticationMessageTask.class, "Authentication message processing");
        skip(CreateProxiesMessageTask.class, "Permissions handled in beforeProcess() method");
    }

    @Test
    public void testGetRequiredPermissions() throws Exception {
        Reflections reflections = new Reflections("uk.org.hecaton");
        Set<Class<? extends AbstractMessageTask>> subTypes = reflections.getSubTypesOf(AbstractMessageTask.class);
        for (Class<?> clazz : subTypes) {
            if (!Modifier.isAbstract(clazz.getModifiers())) {
                assertGetRequiredPermission(clazz.getName());
            }
        }
    }

    @Test
    public void testCreateProxiesOverridesBeforeProcess() throws Exception {
        assertNotNull(CreateProxiesMessageTask.class.getDeclaredMethod("beforeProcess"));
    }

    private void assertGetRequiredPermission(String clsname) throws Exception {
        if (SKIP_CLASS_MAP.containsKey(clsname)) {
            return;
        }
        boolean returnsNull = doesGetRequiredPermissionSimpleReturnNull(clsname);
        assertFalse(clsname + " returns null in getRequiredPermission()", returnsNull);
    }

    private boolean doesGetRequiredPermissionSimpleReturnNull(String clsname) throws Exception {
        if (clsname == null) {
            fail("Class with getRequiredPermission() method implementation not found");
        }
        ClassPool cp = ClassPool.getDefault();
        ClassFile cf = cp.get(clsname).getClassFile();
        MethodInfo minfo = cf.getMethod("getRequiredPermission");
        if (minfo == null) {
            return doesGetRequiredPermissionSimpleReturnNull(cf.getSuperclass());
        }
        CodeAttribute ca = minfo.getCodeAttribute();
        CodeIterator ci = ca.iterator();
        String[] ops = new String[RETURN_NULL_OPS.length];
        int i = 0;
        while (ci.hasNext()) {
            i++;
            if (i > RETURN_NULL_OPS.length) {
                return false;
            }
            int index = ci.next();
            int op = ci.byteAt(index);
            ops[i - 1] = Mnemonic.OPCODE[op];
        }
        return Arrays.equals(RETURN_NULL_OPS, ops);
    }

    private static void skip(Class<?> classToSkip, String reason) {
        Assertions.assertThat(reason).isNotEmpty();
        SKIP_CLASS_MAP.put(classToSkip.getName(), reason);
    }
}
