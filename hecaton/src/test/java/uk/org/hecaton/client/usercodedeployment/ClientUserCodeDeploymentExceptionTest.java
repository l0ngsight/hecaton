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

package uk.org.hecaton.client.usercodedeployment;

import uk.org.hecaton.client.HecatonClientOfflineException;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientUserCodeDeploymentConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.UserCodeUtil;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.internal.util.FilteringClassLoader;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import usercodedeployment.IncrementingEntryProcessor;

import java.io.FileNotFoundException;
import java.util.stream.Stream;

import static uk.org.hecaton.test.UserCodeUtil.pathRelativeToBinariesFolder;
import static java.util.Collections.singletonList;
import static org.junit.Assert.fail;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("removal")
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientUserCodeDeploymentExceptionTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testUserCodeDeploymentIsDisabledByDefaultOnClient() {
        // this test also validate the EP is filtered locally and has to be loaded from the other member
        ClientConfig clientConfig = new ClientConfig();
        Config config = createNodeConfig();
        config.getUserCodeDeploymentConfig().setEnabled(true);

        IncrementingEntryProcessor incrementingEntryProcessor = new IncrementingEntryProcessor();
        factory.newHecatonInstance(config);
        HecatonInstance client = factory.newHecatonClient(clientConfig);

        IMap<Integer, Integer> map = client.getMap(randomName());
        try {
            map.executeOnEntries(incrementingEntryProcessor);
            fail();
        } catch (HecatonSerializationException e) {
            assertEquals(ClassNotFoundException.class, e.getCause().getClass());
        }
    }

    private static Config createNodeConfig() {
        Config i2Config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        i2Config.setClassLoader(filteringCL);
        return i2Config;
    }


    private static ClientConfig createClientConfig() {
        ClientConfig config = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass("usercodedeployment.IncrementingEntryProcessor");
        config.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));
        return config;
    }

    @Test(expected = IllegalStateException.class)
    public void testUserCodeDeployment_serverIsNotEnabled() {
        ClientConfig clientConfig = createClientConfig();
        clientConfig.getUserCodeDeploymentConfig().setEnabled(true);
        Config config = createNodeConfig();

        factory.newHecatonInstance(config);
        factory.newHecatonClient(clientConfig);
    }

    /**
     * The two JARs (IncrementingEntryProcessor) and (IncrementingEntryProcessorConflicting) contain the same class
     * {@link IncrementingEntryProcessor} with different implementations
     */
    @Test(expected = IllegalStateException.class)
    public void testClientsWithConflictingClassRepresentations() {
        Config config = createNodeConfig();
        config.getUserCodeDeploymentConfig().setEnabled(true);
        factory.newHecatonInstance(config);

        Stream.of(
                new String[] {"IncrementingEntryProcessor",
                        UserCodeUtil.INSTANCE.getCompiledJARName("usercodedeployment-incrementing-entry-processor")},
                new String[] {"IncrementingEntryProcessorConflicting",
                        UserCodeUtil.INSTANCE.getCompiledJARName("usercodedeployment-incrementing-entry-processor-conflicting")})
                .forEach(path -> {
                    ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig1 = new ClientUserCodeDeploymentConfig()
                            .addJar(pathRelativeToBinariesFolder(path).toFile()).setEnabled(true);
                    ClientConfig clientConfig = new ClientConfig().setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig1);
                    factory.newHecatonClient(clientConfig);
                });
    }

    @Test
    public void testClientWithAsyncStart_ClientRetriesIndefinitely_WhenUserCodeDeploymentFails() {
        ClientConfig clientConfig = createClientConfig();
        clientConfig.getUserCodeDeploymentConfig().setEnabled(true);
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        Config config = createNodeConfig();

        factory.newHecatonInstance(config);
        HecatonInstance client = factory.newHecatonClient(clientConfig);

        String mapName = randomMapName();
        assertTrueAllTheTime(() -> {
            // Client state stays as INITIAL and since we are using async start, client will keep throwing this error
            assertThatThrownBy(() -> client.getMap(mapName)).isInstanceOf(HecatonClientOfflineException.class);
        }, 10);
    }

    @Test(expected = ClassNotFoundException.class)
    public void testClientsWith_wrongClassName() throws Throwable {
        Config config = createNodeConfig();
        config.getUserCodeDeploymentConfig().setEnabled(true);

        factory.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass("NonExisting.class").setEnabled(true);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig);
        try {
            factory.newHecatonClient(clientConfig);
        } catch (HecatonException e) {
            throw e.getCause();
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void testClientsWith_wrongJarPath() throws Throwable {
        Config config = createNodeConfig();
        config.getUserCodeDeploymentConfig().setEnabled(true);

        factory.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addJar("NonExisting.jar").setEnabled(true);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig);
        try {
            factory.newHecatonClient(clientConfig);
        } catch (HecatonException e) {
            throw e.getCause();
        }
    }
}
