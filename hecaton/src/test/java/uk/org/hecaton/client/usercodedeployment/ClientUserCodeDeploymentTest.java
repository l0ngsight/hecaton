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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientUserCodeDeploymentConfig;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.AttributeConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.UserCodeDeploymentConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.UserCodeUtil;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import usercodedeployment.CapitalizingFirstNameExtractor;
import usercodedeployment.DomainClassWithInnerClass;
import usercodedeployment.EntryProcessorWithAnonymousAndInner;
import usercodedeployment.IncrementingEntryProcessor;
import usercodedeployment.Person;
import usercodedeployment.SampleBaseClass;
import usercodedeployment.SampleSubClass;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static uk.org.hecaton.query.Predicates.equal;
import static uk.org.hecaton.test.SplitBrainTestSupport.blockCommunicationBetween;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("removal")
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientUserCodeDeploymentTest extends ClientTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Parameterized.Parameter(0)
    public UserCodeDeploymentConfig.ClassCacheMode classCacheMode;
    @Parameterized.Parameter(1)
    public UserCodeDeploymentConfig.ProviderMode providerMode;


    @Parameters(name = "ClassCacheMode:{0}, ProviderMode:{1}")
    public static Collection<Object[]> parameters() {
        LinkedList<Object[]> parameters = new LinkedList<>();
        for (UserCodeDeploymentConfig.ClassCacheMode classCacheMode : UserCodeDeploymentConfig.ClassCacheMode.values()) {
            for (UserCodeDeploymentConfig.ProviderMode providerMode : UserCodeDeploymentConfig.ProviderMode.values()) {
                parameters.add(new Object[]{classCacheMode, providerMode});
            }
        }
        return parameters;
    }

    private Config createNodeConfig() {
        Config config = new Config();
//        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
//        config.setClassLoader(filteringCL);
        config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(classCacheMode)
                .setProviderMode(providerMode);
        return config;
    }

    private static ClientConfig createClientConfig() {
        ClientConfig config = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass("usercodedeployment.IncrementingEntryProcessor");
        config.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        return config;
    }

    @Test
    public void testSingleMember() {
        ClientConfig clientConfig = createClientConfig();
        Config config = createNodeConfig();

        factory.newHecatonInstance(config);
        HecatonInstance client = factory.newHecatonClient(clientConfig);

        assertCodeDeploymentWorking(client, new IncrementingEntryProcessor());
    }

    @Test
    public void testWithMultipleMembers() {
        ClientConfig clientConfig = createClientConfig();
        Config config = createNodeConfig();

        factory.newHecatonInstance(config);
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        factory.newHecatonInstance(config);

        assertCodeDeploymentWorking(client, new IncrementingEntryProcessor());
    }

    @Test
    public void testWithMultipleMembersAtStart() {
        ClientConfig clientConfig = createClientConfig();
        Config config = createNodeConfig();

        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        HecatonInstance client = factory.newHecatonClient(clientConfig);

        assertCodeDeploymentWorking(client, new IncrementingEntryProcessor());
    }

    @Test
    public void testWithMultipleNodes_clientReconnectsToNewNode() {
        ClientConfig clientConfig = createClientConfig();
        Config config = createNodeConfig();

        factory.newHecatonInstance(config);
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        factory.newHecatonInstance(config);

        ReconnectListener reconnectListener = new ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);

        factory.shutdownAllMembers();
        factory.newHecatonInstance(config);

        assertOpenEventually(reconnectListener.reconnectedLatch);
        assertCodeDeploymentWorking(client, new IncrementingEntryProcessor());
    }

    @Test
    public void testClassesAreDeployed_whenClientReconnectsToOtherHalf() {
        ClientConfig clientConfig = createClientConfig();
        Config config = createNodeConfig();

        HecatonInstance instance1 = factory.newHecatonInstance(config);
        HecatonInstance instance2 = factory.newHecatonInstance(config);

        assertClusterSizeEventually(2, instance1, instance2);

        // split the cluster
        blockCommunicationBetween(instance1, instance2);

        // make sure that each member quickly drops the other from their member list
        suspectMember(instance1, instance2);
        suspectMember(instance2, instance1);

        assertClusterSizeEventually(1, instance1);
        assertClusterSizeEventually(1, instance2);

        HecatonInstance client = factory.newHecatonClient(clientConfig);
        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());

        UUID connectedMemberUUID = members.iterator().next().getUuid();

        ReconnectListener reconnectListener = new ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);

        if (connectedMemberUUID.equals(instance1.getLocalEndpoint().getUuid())) {
            instance1.shutdown();
        } else {
            instance2.shutdown();
        }

        assertOpenEventually(reconnectListener.reconnectedLatch);

        assertCodeDeploymentWorking(client, new IncrementingEntryProcessor());
    }

    private static void assertCodeDeploymentWorking(HecatonInstance client, EntryProcessor<Integer, Integer, ?> entryProcessor) {
        int keyCount = 100;
        IMap<Integer, Integer> map = client.getMap(randomName());

        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0);
        }

        int incrementCount = 5;
        //doing the call a few times so that the invocation can be done on different members
        for (int i = 0; i < incrementCount; i++) {
            map.executeOnEntries(entryProcessor);
        }

        for (int i = 0; i < keyCount; i++) {
            assertEquals(incrementCount, (int) map.get(i));
        }
    }

    @Test
    public void testWithMultipleMembers_anonymousAndInnerClasses() {
        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addJar(UserCodeUtil.pathRelativeToBinariesFolder("EntryProcessorWithAnonymousAndInner",
                UserCodeUtil.INSTANCE.getCompiledJARName("usercodedeployment-entry-processor-with-anonymous-and-inner")).toFile());
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        Config config = createNodeConfig();

        factory.newHecatonInstance(config);
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        factory.newHecatonInstance(config);

        assertCodeDeploymentWorking(client, new EntryProcessorWithAnonymousAndInner());
    }

    @Test
    public void testCustomAttributeExtractor() {
        String mapName = randomMapName();
        String attributeName = "syntheticAttribute"; //this attribute does not exist in the domain class

        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass(CapitalizingFirstNameExtractor.class);
        clientUserCodeDeploymentConfig.addClass(Person.class);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        Config config = createNodeConfig();
        config.getMapConfig(mapName).addAttributeConfig(new AttributeConfig(attributeName, "usercodedeployment.CapitalizingFirstNameExtractor"));

        factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        HecatonInstance client = factory.newHecatonClient(clientConfig);

        IMap<Integer, Person> map = client.getMap(mapName);
        map.put(0, new Person("ada"));
        map.put(1, new Person("non-ada"));

        Set<Map.Entry<Integer, Person>> results = map.entrySet(equal(attributeName, "ADA"));
        assertEquals(1, results.size());
        assertEquals("ada", results.iterator().next().getValue().getName());
    }

    @Test
    public void testWithParentAndChildClassesWorksIndependentOfOrder_childFirst() {
        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass(SampleSubClass.class);
        clientUserCodeDeploymentConfig.addClass(SampleBaseClass.class);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        factory.newHecatonInstance(createNodeConfig());
        factory.newHecatonClient(clientConfig);
    }

    @Test
    public void testWithParentAndChildClassesWorksIndependentOfOrder_parentFirst() {
        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass(SampleBaseClass.class);
        clientUserCodeDeploymentConfig.addClass(SampleSubClass.class);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        factory.newHecatonInstance(createNodeConfig());
        factory.newHecatonClient(clientConfig);
    }


    @Test
    public void testWithParentAndChildClassesWorksIndependentOfOrder_withChildParentJar() {
        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        /*
         * child parent jar contains two classes as follows. These classes are not put into code base on purpose, in order not to
         * effect the test. Child class is loaded first when reading via JarInputStream.getNextJarEntry, which is the case we
         * wanted to test.
         *
         * package usercodedeployment; import java.io.Serializable; public class ParentClass implements Serializable, Runnable {
         *
         * @Override public void run() {
         *
         * } }
         *
         * package usercodedeployment; public class AChildClass extends AParentClass { }
         *
         */
        clientUserCodeDeploymentConfig.addJar(UserCodeUtil
                .pathRelativeToBinariesFolder("ChildParent", UserCodeUtil.INSTANCE.getCompiledJARName("usercodedeployment-child-parent"))
                .toFile());
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        factory.newHecatonInstance(createNodeConfig());
        factory.newHecatonClient(clientConfig);
    }

    @Test
    public void testWithMainAndInnerClassesWorksIndependentOfOrder_withInnerFirst() {
        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass(DomainClassWithInnerClass.InnerClass.class);
        clientUserCodeDeploymentConfig.addClass(DomainClassWithInnerClass.class);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        factory.newHecatonInstance(createNodeConfig());
        factory.newHecatonClient(clientConfig);
    }

    @Test
    public void testWithMainAndInnerClassesWorksIndependentOfOrder_withMainFirst() {
        ClientConfig clientConfig = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass(DomainClassWithInnerClass.class);
        clientUserCodeDeploymentConfig.addClass(DomainClassWithInnerClass.InnerClass.class);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));

        factory.newHecatonInstance(createNodeConfig());
        factory.newHecatonClient(clientConfig);
    }
}
