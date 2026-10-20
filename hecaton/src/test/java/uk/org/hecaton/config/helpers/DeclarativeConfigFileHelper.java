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

package uk.org.hecaton.config.helpers;

import uk.org.hecaton.config.Config;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

public class DeclarativeConfigFileHelper {
    private static final String HECATON_START_TAG = "<hecaton xmlns=\"https://hecaton.org.uk/schema/config\">\n";
    private static final String HECATON_END_TAG = "</hecaton>\n";
    private static final String HECATON_CLIENT_START_TAG =
            "<hecaton-client xmlns=\"https://hecaton.org.uk/schema/client-config\">\n";
    private static final String HECATON_CLIENT_END_TAG = "</hecaton-client>";
    private static final String HECATON_CLIENT_FAILOVER_START_TAG =
            "<hecaton-client-failover xmlns=\"https://hecaton.org.uk/schema/client-config\">\n";
    private static final String HECATON_CLIENT_FAILOVER_END_TAG = "</hecaton-client-failover>";

    private List<String> testConfigPaths = new LinkedList<>();

    // MEMBER

    public File givenXmlConfigFileInWorkDir(String instanceName) throws Exception {
        return givenXmlConfigFileInWorkDir("hecaton.xml", instanceName);
    }

    public File givenXmlConfigFileInWorkDir(String filename, String instanceName) throws Exception {
        String xml = xmlConfig(instanceName);
        return givenConfigFileInWorkDir(filename, xml);
    }

    public File givenYamlConfigFileInWorkDir(String instanceName) throws Exception {
        return givenYamlConfigFileInWorkDir("hecaton.yaml", instanceName);
    }

    public File givenYmlConfigFileInWorkDir(String instanceName) throws Exception {
        return givenYamlConfigFileInWorkDir("hecaton.yml", instanceName);
    }

    public File givenYamlConfigFileInWorkDir(String filename, String instanceName) throws Exception {
        String xml = yamlConfig(instanceName);
        return givenConfigFileInWorkDir(filename, xml);
    }

    public URL givenXmlConfigFileOnClasspath(String instanceName) throws Exception {
        return givenXmlConfigFileOnClasspath("hecaton.xml", instanceName);
    }

    public URL givenXmlConfigFileOnClasspath(String filename, String instanceName) throws Exception {
        String xml = xmlConfig(instanceName);
        return givenConfigFileOnClasspath(filename, xml);
    }

    public URL givenYamlConfigFileOnClasspath(String instanceName) throws Exception {
        return givenYamlConfigFileOnClasspath("hecaton.yaml", instanceName);
    }

    public URL givenYmlConfigFileOnClasspath(String instanceName) throws Exception {
        return givenYamlConfigFileOnClasspath("hecaton.yml", instanceName);
    }

    public URL givenYamlConfigFileOnClasspath(String filename, String instanceName) throws Exception {
        String yaml = yamlConfig(instanceName);
        return givenConfigFileOnClasspath(filename, yaml);
    }

    // CLIENT

    public File givenXmlClientConfigFileInWorkDir(String instanceName) throws Exception {
        return givenXmlClientConfigFileInWorkDir("hecaton-client.xml", instanceName);
    }

    public File givenXmlClientConfigFileInWorkDir(String filename, String instanceName) throws Exception {
        String xml = xmlClientConfig(instanceName);
        return givenConfigFileInWorkDir(filename, xml);
    }

    public URL givenXmlClientConfigFileOnClasspath(String instanceName) throws Exception {
        return givenXmlClientConfigFileOnClasspath("hecaton-client.xml", instanceName);
    }

    public URL givenXmlClientConfigFileOnClasspath(String filename, String instanceName) throws Exception {
        String xml = xmlClientConfig(instanceName);
        return givenConfigFileOnClasspath(filename, xml);
    }

    public File givenYamlClientConfigFileInWorkDir(String instanceName) throws Exception {
        return givenYamlClientConfigFileInWorkDir("hecaton-client.yaml", instanceName);
    }

    public File givenYmlClientConfigFileInWorkDir(String instanceName) throws Exception {
        return givenYamlClientConfigFileInWorkDir("hecaton-client.yml", instanceName);
    }

    public File givenYamlClientConfigFileInWorkDir(String filename, String instanceName) throws Exception {
        String xml = yamlClientConfig(instanceName);
        return givenConfigFileInWorkDir(filename, xml);
    }

    public URL givenYamlClientConfigFileOnClasspath(String instanceName) throws Exception {
        return givenYamlClientConfigFileOnClasspath("hecaton-client.yaml", instanceName);
    }

    public URL givenYmlClientConfigFileOnClasspath(String instanceName) throws Exception {
        return givenYamlClientConfigFileOnClasspath("hecaton-client.yml", instanceName);
    }

    public URL givenYamlClientConfigFileOnClasspath(String filename, String instanceName) throws Exception {
        String yaml = yamlClientConfig(instanceName);
        return givenConfigFileOnClasspath(filename, yaml);
    }

    // CLIENT-FAILOVER

    public File givenXmlClientFailoverConfigFileInWorkDir(int tryCount) throws Exception {
        return givenXmlClientFailoverConfigFileInWorkDir("hecaton-client-failover.xml", tryCount);
    }

    public File givenXmlClientFailoverConfigFileInWorkDir(String filename, int tryCount) throws Exception {
        String xml = xmlFailoverClientConfig(tryCount);
        return givenConfigFileInWorkDir(filename, xml);
    }

    public URL givenXmlClientFailoverConfigFileOnClasspath(int tryCount) throws Exception {
        return givenXmlClientFailoverConfigFileOnClasspath("hecaton-client-failover.xml", tryCount);
    }

    public URL givenXmlClientFailoverConfigFileOnClasspath(String filename, int tryCount) throws Exception {
        String xml = xmlFailoverClientConfig(tryCount);
        return givenConfigFileOnClasspath(filename, xml);
    }

    public File givenYamlClientFailoverConfigFileInWorkDir(int tryCount) throws Exception {
        return givenYamlClientFailoverConfigFileInWorkDir("hecaton-client-failover.yaml", tryCount);
    }

    public File givenYmlClientFailoverConfigFileInWorkDir(int tryCount) throws Exception {
        return givenYamlClientFailoverConfigFileInWorkDir("hecaton-client-failover.yml", tryCount);
    }

    public File givenYamlClientFailoverConfigFileInWorkDir(String filename, int tryCount) throws Exception {
        String xml = yamlFailoverClientConfig(tryCount);
        return givenConfigFileInWorkDir(filename, xml);
    }

    public URL givenYamlClientFailoverConfigFileOnClasspath(int tryCount) throws Exception {
        return givenYamlClientFailoverConfigFileOnClasspath("hecaton-client-failover.yaml", tryCount);
    }

    public URL givenYmlClientFailoverConfigFileOnClasspath(int tryCount) throws Exception {
        return givenYamlClientFailoverConfigFileOnClasspath("hecaton-client-failover.yml", tryCount);
    }

    public URL givenYamlClientFailoverConfigFileOnClasspath(String filename, int tryCount) throws Exception {
        String yaml = yamlFailoverClientConfig(tryCount);
        return givenConfigFileOnClasspath(filename, yaml);
    }

    public File givenConfigFileInWorkDir(String filename, String content) throws IOException {
        File file = new File(filename);
        PrintWriter writer = new PrintWriter(file, StandardCharsets.UTF_8);
        writer.print(content);
        writer.close();

        testConfigPaths.add(file.getAbsolutePath());

        return file;
    }

    public URL givenConfigFileOnClasspath(String filename, String content) throws Exception {
        URL classPathConfigUrl = Config.class.getClassLoader().getResource(".");
        String configFilePath = classPathConfigUrl.getFile() + "/" + filename;
        File file = new File(configFilePath);
        PrintWriter writer = new PrintWriter(file, StandardCharsets.UTF_8);
        writer.println(content);
        writer.close();

        testConfigPaths.add(file.getAbsolutePath());

        return getClass().getClassLoader().getResource(filename);
    }

    public String createFilesWithCycleImports(Function<String, String> fileContentWithImportResource, String... paths) throws Exception {
        for (int i = 1; i < paths.length; i++) {
            createFileWithDependencyImport(paths[i - 1], paths[i], fileContentWithImportResource);
        }
        return createFileWithDependencyImport(paths[0], paths[1], fileContentWithImportResource);
    }

    private String createFileWithDependencyImport(
            String dependent,
            String pathToDependency,
            Function<String, String> fileContentWithImportResource) throws Exception {
        final String xmlContent = fileContentWithImportResource.apply(pathToDependency);
        givenConfigFileInWorkDir(dependent, xmlContent);
        return xmlContent;
    }

    private String xmlConfig(String instanceName) {
        return ""
                + HECATON_START_TAG
                + "  <instance-name>" + instanceName + "</instance-name>"
                + HECATON_END_TAG;
    }

    private String xmlClientConfig(String instanceName) {
        return ""
                + HECATON_CLIENT_START_TAG
                + "  <instance-name>" + instanceName + "</instance-name>"
                + "  <connection-strategy async-start=\"true\" >"
                + "  </connection-strategy>"
                + HECATON_CLIENT_END_TAG;
    }

    private String xmlFailoverClientConfig(int tryCount) {
        return ""
                + HECATON_CLIENT_FAILOVER_START_TAG
                + "  <try-count>" + tryCount + "</try-count>"
                + "  <clients>"
                + "    <client>hecaton-client-c1.xml</client>"
                + "  </clients>"
                + HECATON_CLIENT_FAILOVER_END_TAG;
    }

    private String yamlConfig(String instanceName) {
        return ""
                + "hecaton:\n"
                + "  instance-name: " + instanceName;
    }

    private String yamlClientConfig(String instanceName) {
        return ""
                + "hecaton-client:\n"
                + "  instance-name: " + instanceName + "\n"
                + "  connection-strategy:\n"
                + "    async-start: true\n";
    }

    private String yamlFailoverClientConfig(int tryCount) {
        return ""
                + "hecaton-client-failover:\n"
                + "  try-count: " + tryCount + "\n"
                + "  clients:\n"
                + "    - hecaton-client-c1.yaml";
    }

    public void ensureTestConfigDeleted() {
        if (!testConfigPaths.isEmpty()) {
            for (String testConfigPath : testConfigPaths) {
                File file = new File(testConfigPath);
                file.delete();
            }
        }
    }
}
