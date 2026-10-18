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

package uk.org.hecaton.dataconnection;

import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class})
public class HecatonDataConnectionTest extends HecatonTestSupport {

    private String clusterName;
    private HecatonInstance instance;
    private HecatonDataConnection hecatonDataConnection;

    @Before
    public void setUp() throws Exception {
        clusterName = randomName();
        instance = Hecaton.newHecatonInstance(smallInstanceConfig().setClusterName(clusterName));
    }

    @After
    public void tearDown() throws Exception {
        if (hecatonDataConnection != null) {
            hecatonDataConnection.release();
        }
        if (instance != null) {
            instance.shutdown();
            instance = null;
        }
    }

    @Test
    public void list_resources_should_return_map() {
        IMap<Integer, String> map = instance.getMap("my_map");
        map.put(42, "42");

        DataConnectionConfig dataConnectionConfig = sharedDataConnectionConfig(clusterName);

        hecatonDataConnection = new HecatonDataConnection(dataConnectionConfig);
        Collection<DataConnectionResource> resources = hecatonDataConnection.listResources();

        assertThat(resources).contains(new DataConnectionResource("IMapJournal", "my_map"));
    }

    @Test
    public void list_resources_should_not_return_system_maps() {
        DataConnectionConfig dataConnectionConfig = sharedDataConnectionConfig(clusterName);

        hecatonDataConnection = new HecatonDataConnection(dataConnectionConfig);
        Collection<DataConnectionResource> resources = hecatonDataConnection.listResources();

        assertThat(resources).isEmpty();
    }

    @Test
    public void should_throw_with_missing_config() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig("data-connection-name")
                .setType("hz")
                .setShared(true);

        assertThatThrownBy(() -> hecatonDataConnection = new HecatonDataConnection(dataConnectionConfig))
                .isInstanceOf(HecatonException.class)
                .hasMessage("HecatonDataConnection with name 'data-connection-name' "
                        + "could not be created, "
                        + "provide either a file path with one of "
                        + "\"client_xml_path\" or \"client_yml_path\" properties "
                        + "or a string content with one of \"client_xml\" or \"client_yml\" properties "
                        + "for the client configuration.");
    }

    @Test
    public void should_throw_with_empty_filepath() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig("data-link-name")
                .setType(HecatonDataConnection.class.getName())
                .setProperty(HecatonDataConnection.CLIENT_YML_PATH, "")
                .setShared(true);

        assertThatThrownBy(() -> hecatonDataConnection = new HecatonDataConnection(dataConnectionConfig))
                .isInstanceOf(HecatonException.class);
    }

    @Test
    public void should_throw_with_filepath_string() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig("data-link-name")
                .setType(HecatonDataConnection.class.getName())
                .setProperty(HecatonDataConnection.CLIENT_YML_PATH, "")
                .setProperty(HecatonDataConnection.CLIENT_YML, "")
                .setShared(true);

        assertThatThrownBy(() -> hecatonDataConnection = new HecatonDataConnection(dataConnectionConfig))
                .isInstanceOf(HecatonException.class);
    }

    @Test
    public void shared_client_should_return_same_instance() {
        DataConnectionConfig dataConnectionConfig = sharedDataConnectionConfig(clusterName);
        hecatonDataConnection = new HecatonDataConnection(dataConnectionConfig);
        HecatonInstance c1 = hecatonDataConnection.getClient();
        HecatonInstance c2 = hecatonDataConnection.getClient();

        try {
            assertThat(c1).isSameAs(c2);
        } finally {
            c1.shutdown();
            c2.shutdown();
        }
    }

    @Test
    public void shared_client_from_file_should_return_same_instance() {
        DataConnectionConfig dataConnectionConfig = sharedDataConnectionConfigFromFile(clusterName);
        hecatonDataConnection = new HecatonDataConnection(dataConnectionConfig);
        HecatonInstance c1 = hecatonDataConnection.getClient();
        HecatonInstance c2 = hecatonDataConnection.getClient();

        try {
            assertThat(c1).isSameAs(c2);
        } finally {
            c1.shutdown();
            c2.shutdown();
            // Delete the file at the end of the test
            try {
                String filePath = dataConnectionConfig.getProperty(HecatonDataConnection.CLIENT_XML_PATH);
                assert filePath != null;
                Files.delete(Paths.get(filePath));
            } catch (IOException ignored) {
            }
        }
    }

    @Test(timeout = 60_000)
    public void shared_client_should_be_initialized_lazy() {
        int invalidPort = 9999;
        DataConnectionConfig dataConnectionConfig = sharedDataConnectionConfig(clusterName, invalidPort);
        hecatonDataConnection = new HecatonDataConnection(dataConnectionConfig);
        assertThatNoException().isThrownBy(() -> new HecatonDataConnection(dataConnectionConfig));
    }

    @Test
    public void non_shared_client_should_return_new_client_instance() {
        DataConnectionConfig dataConnectionConfig = nonSharedDataConnectionConfig(clusterName);
        hecatonDataConnection = new HecatonDataConnection(dataConnectionConfig);
        HecatonInstance c1 = hecatonDataConnection.getClient();
        HecatonInstance c2 = hecatonDataConnection.getClient();

        try {
            assertThat(c1).isNotSameAs(c2);
        } finally {
            c1.shutdown();
            c2.shutdown();
        }
    }

    @Test
    public void should_list_resource_types() {
        // given
        DataConnectionConfig dataConnectionConfig = nonSharedDataConnectionConfig(clusterName);
        hecatonDataConnection = new HecatonDataConnection(dataConnectionConfig);

        // when
        Collection<String> resourcedTypes = hecatonDataConnection.resourceTypes();

        //then
        assertThat(resourcedTypes)
                .map(r -> r.toLowerCase(Locale.ROOT))
                .containsExactlyInAnyOrder("imapjournal");
    }

    private DataConnectionConfig nonSharedDataConnectionConfig(String clusterName) {
        return sharedDataConnectionConfig(clusterName)
                .setShared(false);
    }

    @Nonnull
    private DataConnectionConfig sharedDataConnectionConfig(String clusterName) {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig("data-connection-name")
                .setType("HZ")
                .setShared(true);
        try {
            String str = readFile();
            var socketAddress = ((InetSocketAddress) instance.getLocalEndpoint().getSocketAddress()).getPort();
            String xmlString = str.replace("$CLUSTER_NAME$", clusterName)
                                  .replace("$PORT_NUMBER$", String.valueOf(socketAddress));
            dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML, xmlString);
            return dataConnectionConfig;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private static DataConnectionConfig sharedDataConnectionConfig(String clusterName, int port) {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig("data-connection-name")
                .setType("HZ")
                .setShared(true);
        try {
            String str = readFile();
            String xmlString = str
                    .replace("$CLUSTER_NAME$", clusterName)
                    .replace("$PORT_NUMBER$", Integer.toString(port));
            dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML, xmlString);
            return dataConnectionConfig;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private DataConnectionConfig sharedDataConnectionConfigFromFile(String clusterName) {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig("data-link-name")
                .setType(HecatonDataConnection.class.getName())
                .setShared(true);
        try {
            String str = readFile();
            int port = ((InetSocketAddress) instance.getLocalEndpoint().getSocketAddress()).getPort();
            String xmlString = str.replace("$CLUSTER_NAME$", clusterName).replace("$PORT_NUMBER$",
                                                                                  String.valueOf(port));
            Path tempFile = Files.createTempFile("test_client", ".xml");
            Files.writeString(tempFile, xmlString);
            dataConnectionConfig.setProperty(HecatonDataConnection.CLIENT_XML_PATH, tempFile.toString());

            return dataConnectionConfig;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String readFile() throws IOException {
        return Files.readString(Paths.get("src", "test", "resources", "hecaton-client-test-external.xml"));
    }
}
