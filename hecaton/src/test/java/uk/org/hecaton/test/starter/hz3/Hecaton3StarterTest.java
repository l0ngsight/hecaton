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

package uk.org.hecaton.test.starter.hz3;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.annotation.NightlyTest;
import uk.org.hecaton.topic.ITopic;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static org.assertj.core.api.Assertions.assertThat;

@Category(NightlyTest.class)
public class Hecaton3StarterTest {

    private static final String HZ3_MEMBER_CONFIG =
            "<hecaton xmlns=\"https://hecaton.org.uk/schema/config\"\n"
            + "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "           xsi:schemaLocation=\"https://hecaton.org.uk/schema/config\n"
            + "           https://hecaton.org.uk/schema/config/hecaton-config-3.12.xsd\">\n"
            + "    <group>\n"
            + "        <name>dev</name>\n"
            + "    </group>\n"
            + "    <network>\n"
            + "        <port auto-increment=\"true\" port-count=\"100\">3210</port>\n"
            + "        <join>\n"
            + "            <multicast enabled=\"false\">\n"
            + "            </multicast>\n"
            + "        </join>\n"
            + "    </network>\n"
            + "</hecaton>\n";

    @Test
    public void testMapPutGet() {
        HecatonInstance instance = Hecaton3Starter.newHecatonInstance(HZ3_MEMBER_CONFIG);
        IMap<String, String> map = instance.getMap("my-map");
        map.put("key", "value");
        String value = map.get("key");
        assertThat(value).isEqualTo("value");

        instance.shutdown();
    }

    @Test
    public void testTopicMessageListenerPublish() {
        HecatonInstance instance = Hecaton3Starter.newHecatonInstance(HZ3_MEMBER_CONFIG);
        ITopic<String> topic = instance.getTopic("my-topic");
        List<String> result = new ArrayList<>();
        topic.addMessageListener(message -> result.add(message.getMessageObject()));
        topic.publish("value");
        assertTrueEventually(() -> assertThat(result).contains("value"));

        instance.shutdown();
    }
}
