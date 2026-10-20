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

package uk.org.hecaton.migrate.files;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.org.hecaton.migrate.files.FileKind.OTHER_TEXT;
import static uk.org.hecaton.migrate.files.FileKind.XML;
import static uk.org.hecaton.migrate.files.FileKind.YAML;

class TextMigratorTest {

    @Test
    void memberXml() {
        String before = """
                <?xml version="1.0" encoding="UTF-8"?>
                <!-- Copyright (c) 2008-2026, Hazelcast, Inc. -->
                <hazelcast xmlns="http://www.hazelcast.com/schema/config"
                           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                           xsi:schemaLocation="http://www.hazelcast.com/schema/config
                           http://www.hazelcast.com/schema/config/hazelcast-config-5.7.xsd">
                    <cluster-name>hazelcast-prod</cluster-name>
                    <properties>
                        <property name="hazelcast.logging.type">slf4j</property>
                    </properties>
                    <map name="hazelcast.sessions"/>
                    <serialization>
                        <data-serializable-factories>
                            <data-serializable-factory factory-id="1">com.example.Factory</data-serializable-factory>
                        </data-serializable-factories>
                    </serialization>
                    <listeners>
                        <listener>com.hazelcast.spring.context.SpringManagedContext</listener>
                    </listeners>
                </hazelcast>
                """;
        String after = """
                <?xml version="1.0" encoding="UTF-8"?>
                <!-- Copyright (c) 2008-2026, Hazelcast, Inc. -->
                <hecaton xmlns="https://hecaton.org.uk/schema/config"
                           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                           xsi:schemaLocation="https://hecaton.org.uk/schema/config
                           https://hecaton.org.uk/schema/config/hecaton-config-5.7.xsd">
                    <cluster-name>hazelcast-prod</cluster-name>
                    <properties>
                        <property name="hecaton.logging.type">slf4j</property>
                    </properties>
                    <map name="hazelcast.sessions"/>
                    <serialization>
                        <data-serializable-factories>
                            <data-serializable-factory factory-id="1">com.example.Factory</data-serializable-factory>
                        </data-serializable-factories>
                    </serialization>
                    <listeners>
                        <listener>uk.org.hecaton.spring.context.SpringManagedContext</listener>
                    </listeners>
                </hecaton>
                """;
        TextMigrator.Result result = TextMigrator.migrate(before, XML);
        assertThat(result.text()).isEqualTo(after);
        // the application's own names stay and are reported
        assertThat(result.leftovers()).extracting(TextMigrator.Leftover::line).containsExactly(7, 11);
    }

    @Test
    void springXml() {
        String before = """
                <beans xmlns:hz="http://www.hazelcast.com/schema/spring"
                       xsi:schemaLocation="http://www.hazelcast.com/schema/spring
                       https://www.hazelcast.com/schema/spring/hazelcast-spring-5.7.xsd">
                    <hz:hazelcast id="instance">
                        <hz:config/>
                    </hz:hazelcast>
                    <bean class="com.hazelcast.spring.HazelcastClientFactoryBean"/>
                </beans>
                """;
        assertThat(TextMigrator.migrate(before, XML).text()).isEqualTo("""
                <beans xmlns:hz="https://hecaton.org.uk/schema/spring"
                       xsi:schemaLocation="https://hecaton.org.uk/schema/spring
                       https://hecaton.org.uk/schema/spring/hecaton-spring-5.7.xsd">
                    <hz:hecaton id="instance">
                        <hz:config/>
                    </hz:hecaton>
                    <bean class="uk.org.hecaton.spring.HecatonClientFactoryBean"/>
                </beans>
                """);
    }

    @Test
    void clientYaml() {
        String before = """
                hazelcast-client:
                  cluster-name: dev
                  properties:
                    hazelcast.client.statistics.enabled: true
                  network:
                    cluster-members:
                      - hazelcast-0.hazelcast.default.svc
                """;
        TextMigrator.Result result = TextMigrator.migrate(before, YAML);
        assertThat(result.text()).isEqualTo("""
                hecaton-client:
                  cluster-name: dev
                  properties:
                    hecaton.client.statistics.enabled: true
                  network:
                    cluster-members:
                      - hazelcast-0.hazelcast.default.svc
                """);
        assertThat(result.leftovers()).extracting(TextMigrator.Leftover::line).containsExactly(7);
    }

    @Test
    void springBootProperties() {
        assertThat(TextMigrator.migrate("spring.hazelcast.config=classpath:hazelcast-client.yaml\n", OTHER_TEXT).text())
                .isEqualTo("spring.hecaton.config=classpath:hecaton-client.yaml\n");
        String yaml = """
                spring:
                  application:
                    name: app
                  hazelcast:
                    config: classpath:hazelcast.yaml
                other:
                  hazelcast: x
                """;
        assertThat(TextMigrator.migrate(yaml, YAML).text()).isEqualTo("""
                spring:
                  application:
                    name: app
                  hecaton:
                    config: classpath:hecaton.yaml
                other:
                  hazelcast: x
                """);
    }

    @Test
    void dockerfileAndEnvironment() {
        String before = """
                FROM hazelcast/hazelcast:5.7.0
                ENV HZ_NETWORK_PUBLICADDRESS=10.0.0.1 HZCLIENT_CLUSTERNAME=dev
                ENV JAVA_OPTS="-Dhazelcast.config=/opt/hazelcast.yaml -Dhz.network.port.port=5702"
                ENV CFG='$HZ_INSTANCE_TRACKING{start_timestamp}'
                CMD ["/opt/hazelcast/bin/hz-start"]
                RUN /opt/app/bin/hz start
                """;
        TextMigrator.Result result = TextMigrator.migrate(before, OTHER_TEXT);
        assertThat(result.text()).isEqualTo("""
                FROM hazelcast/hazelcast:5.7.0
                ENV HC_NETWORK_PUBLICADDRESS=10.0.0.1 HCCLIENT_CLUSTERNAME=dev
                ENV JAVA_OPTS="-Dhecaton.config=/opt/hecaton.yaml -Dhc.network.port.port=5702"
                ENV CFG='$HZ_INSTANCE_TRACKING{start_timestamp}'
                CMD ["/opt/hazelcast/bin/hc-start"]
                RUN /opt/app/bin/hc start
                """);
        // no Hecaton image to switch to: reported, not changed
        assertThat(result.leftovers()).extracting(TextMigrator.Leftover::line).containsExactly(1, 5);
    }

    @Test
    void urlsDomainsAndLabelsUnchanged() {
        String text = """
                # see https://docs.hazelcast.com/hazelcast/latest/
                labels:
                  hazelcast.com/service-per-pod: "true"
                contact: support@hazelcast.com
                """;
        TextMigrator.Result result = TextMigrator.migrate(text, YAML);
        assertThat(result.text()).isEqualTo(text);
        assertThat(result.leftovers()).isEmpty();
    }

    @Test
    void logConfigAndJmx() {
        assertThat(TextMigrator.migrate("<Logger name=\"com.hazelcast.cluster\" level=\"warn\"/>\n", XML).text())
                .isEqualTo("<Logger name=\"uk.org.hecaton.cluster\" level=\"warn\"/>\n");
        assertThat(TextMigrator.migrate("logging.level.com.hazelcast=info\nobject=com.hazelcast:type=IMap\n", OTHER_TEXT)
                .text()).isEqualTo("logging.level.uk.org.hecaton=info\nobject=uk.org.hecaton:type=IMap\n");
    }

    @Test
    void sqlCatalog() {
        assertThat(TextMigrator.migrate("SELECT * FROM hazelcast.public.orders;\nSELECT * FROM public.x;\n",
                OTHER_TEXT).text()).isEqualTo("SELECT * FROM hecaton.public.orders;\nSELECT * FROM public.x;\n");
    }

    @Test
    void idempotent() {
        String once = TextMigrator.migrate("hazelcast:\n  properties:\n    hazelcast.logging.type: jdk\n", YAML).text();
        assertThat(TextMigrator.migrate(once, YAML).text()).isEqualTo(once);
    }
}
