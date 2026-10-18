/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.kafka.connect.impl;

import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.jet.SimpleTestInClusterSupport;
import uk.org.hecaton.jet.core.test.TestProcessorContext;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static uk.org.hecaton.jet.kafka.connect.impl.DummySourceConnector.INSTANCE;
import static uk.org.hecaton.jet.kafka.connect.impl.DummySourceConnector.ITEMS_SIZE;
import static uk.org.hecaton.jet.retry.RetryStrategies.never;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SourceConnectorWrapperTest extends SimpleTestInClusterSupport {

    @BeforeClass
    public static void beforeClass() {
        initialize(1, smallInstanceConfig());
    }

    @Test
    public void should_create_and_start_source_with_minimal_properties() {
        new TestSourceConnectorWrapper(dummySourceConnectorProperties(), instance());

        assertThat(sourceConnectorInstance().isInitialized()).isTrue();
        assertThat(sourceConnectorInstance().isStarted()).isTrue();
    }

    @Test
    public void should_create_task_runners() {
        TestSourceConnectorWrapper sourceConnectorWrapper =
                new TestSourceConnectorWrapper(dummySourceConnectorProperties(), instance());

        TaskRunner taskRunner1 = sourceConnectorWrapper.createTaskRunner();
        assertThat(taskRunner1.getName()).isEqualTo("some-name-task-0");
        taskRunner1.poll();
        Map<String, String> expectedTaskProperties = new HashMap<>();
        expectedTaskProperties.put("name", "some-name");
        expectedTaskProperties.put("connector.class", DummySourceConnector.class.getName());
        expectedTaskProperties.put("task.id", "0");
        DummySourceConnector.DummyTask dummyTask = lastTaskInstance();
        assertThat(dummyTask.getProperties()).containsAllEntriesOf(expectedTaskProperties);
    }

    @Test
    public void should_reconfigure_task_runners() {
        TestSourceConnectorWrapper sourceConnectorWrapper =
                new TestSourceConnectorWrapper(dummySourceConnectorProperties(), instance());

        TaskRunner taskRunner1 = sourceConnectorWrapper.createTaskRunner();
        assertThat(taskRunner1.getName()).isEqualTo("some-name-task-0");
        taskRunner1.poll();
        Map<String, String> expectedTaskProperties = new HashMap<>();
        expectedTaskProperties.put("name", "some-name");
        expectedTaskProperties.put("connector.class", DummySourceConnector.class.getName());
        expectedTaskProperties.put("task.id", "0");
        assertThat(lastTaskInstance().getProperties()).containsAllEntriesOf(expectedTaskProperties);

        sourceConnectorInstance().setProperty("updated-property", "some-value");
        sourceConnectorInstance().triggerReconfiguration();
        taskRunner1.poll();

        expectedTaskProperties = new HashMap<>();
        expectedTaskProperties.put("name", "some-name");
        expectedTaskProperties.put("connector.class", DummySourceConnector.class.getName());
        expectedTaskProperties.put("task.id", "0");
        expectedTaskProperties.put("updated-property", "some-value");
        assertThat(lastTaskInstance().getProperties()).containsAllEntriesOf(expectedTaskProperties);
    }

    private static DummySourceConnector.DummyTask lastTaskInstance() {
        return DummySourceConnector.DummyTask.INSTANCE;
    }

    private static DummySourceConnector sourceConnectorInstance() {
        return INSTANCE;
    }

    @Test
    public void should_fail_with_connector_class_not_found() {
        Properties properties = new Properties();
        properties.setProperty("name", "some-name");
        properties.setProperty("tasks.max", "2");
        properties.setProperty("connector.class", "com.example.non.existing.Connector");
        TestProcessorContext testProcessorContext = new TestProcessorContext()
                .setHecatonInstance(instance());
        var c = new SourceConnectorWrapper(properties, 0, testProcessorContext, never());
        assertThatThrownBy(c::waitNeeded)
                .isInstanceOf(HecatonException.class)
                .cause()
                .hasMessage("Connector class 'com.example.non.existing.Connector' not found. " +
                        "Did you add the connector jar to the job?");
    }

    @Test
    public void should_cleanup_on_destroy() {
        Properties properties = dummySourceConnectorProperties();
        properties.setProperty(ITEMS_SIZE, String.valueOf(3));
        var wrapper = new TestSourceConnectorWrapper(properties, instance());
        assertThat(sourceConnectorInstance().isStarted()).isTrue();

        wrapper.close();

        assertThat(sourceConnectorInstance().isStarted()).isFalse();
    }

    @Nonnull
    private static Properties dummySourceConnectorProperties() {
        Properties properties = new Properties();
        properties.setProperty("name", "some-name");
        properties.setProperty("tasks.max", "2");
        properties.setProperty("connector.class", DummySourceConnector.class.getName());
        return properties;
    }
}
