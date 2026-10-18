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

package uk.org.hecaton.jet.impl.deployment;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.UserCodeUtil;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.URL;
import java.net.URLClassLoader;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientDeployment_StandaloneClusterTest extends JetTestSupport {
    @SuppressWarnings("removal")
    @Test
    public void when_classAddedUsingUcd_then_visibleToJet() throws Exception {
        URL classUrl = UserCodeUtil.urlRelativeToBinariesFolder("sample");
        try (URLClassLoader urlClassLoader = new URLClassLoader(new URL[] {classUrl}, null)) {
            Class<?> personClz = urlClassLoader.loadClass("com.sample.pojo.person.Person$Appereance");

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setClassLoader(urlClassLoader);
            clientConfig.getUserCodeDeploymentConfig().setEnabled(true).addClass(personClz);

            Config config = new Config();
            config.getJetConfig().setEnabled(true);
            config.getUserCodeDeploymentConfig().setEnabled(true);

            HecatonInstance instance = createHecatonInstance(config);
            createHecatonClient(clientConfig);

            DAG dag = new DAG();
            dag.newVertex("v", () -> new LoadClassesIsolated(true));

            instance.getJet().newJob(dag).join();
        }
    }
}
