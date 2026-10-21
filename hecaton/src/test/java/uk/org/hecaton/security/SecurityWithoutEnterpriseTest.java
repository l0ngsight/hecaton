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

package uk.org.hecaton.security;

import uk.org.hecaton.auditlog.impl.NoOpAuditlogService;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.SecurityConfig;
import uk.org.hecaton.config.SymmetricEncryptionConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getSerializationService;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SecurityWithoutEnterpriseTest extends HecatonTestSupport {

    @Test
    public void securityIsAvailable() {
        SecurityConfig securityConfig = new SecurityConfig()
                .setEnabled(true);

        Config config = smallInstanceConfig()
                .setSecurityConfig(securityConfig);

        HecatonInstance hz = createHecatonInstance(config);
        assertNotNull(getNode(hz).securityContext);
    }

    @Test
    public void testSymmetricEncryption() {
        SymmetricEncryptionConfig symmetricEncryptionConfig = new SymmetricEncryptionConfig()
                .setEnabled(true);
        Config config = new Config();
        config.getNetworkConfig().setSymmetricEncryptionConfig(symmetricEncryptionConfig);
        assertThrows(IllegalStateException.class, () -> createHecatonInstance(config));
    }

    @Test
    public void auditlogIsAvailable() {
        Config config = smallInstanceConfig();
        config.getAuditlogConfig().setEnabled(true);
        HecatonInstance hz = createHecatonInstance(config);
        assertFalse(getNode(hz).getNodeExtension().getAuditlogService() instanceof NoOpAuditlogService);
    }

    @Test
    public void testCredentialsSerialization() {
        HecatonInstance hz = createHecatonInstance(smallInstanceConfig());
        SerializationService serializationService = getSerializationService(hz);

        UsernamePasswordCredentials upc = new UsernamePasswordCredentials("admin", "secret");
        UsernamePasswordCredentials upc2 = serializationService.toObject(serializationService.toData(upc));
        assertEquals(upc.getName(), upc2.getName());
        assertEquals(upc.getPassword(), upc2.getPassword());

        SimpleTokenCredentials stc = new SimpleTokenCredentials(new byte[] { 1, 2, 3 });
        SimpleTokenCredentials stc2 = serializationService.toObject(serializationService.toData(stc));
        assertEquals(stc.getName(), stc2.getName());
        assertArrayEquals(stc.getToken(), stc2.getToken());
    }

}
