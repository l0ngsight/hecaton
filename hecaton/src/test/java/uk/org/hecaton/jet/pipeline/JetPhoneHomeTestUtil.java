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

package uk.org.hecaton.jet.pipeline;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.util.phonehome.PhoneHome;
import uk.org.hecaton.internal.util.phonehome.PhoneHomeMetrics;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.core.JobStatus;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static uk.org.hecaton.jet.core.JobAssertions.assertThat;

public class JetPhoneHomeTestUtil {
    public static void assertConnectorPhoneHomeCollected(List<Supplier<Pipeline>> pipelineSuppliers,
                                                   String connectorName,
                                                   @Nullable Function<Void, Void> cleanupFn,
                                                   boolean joinJob,
                                                   HecatonInstance... instances) {
        if (instances.length == 0) {
            throw new RuntimeException("instances is empty");
        }

        long expectedCounterCount = countConnectorCount(connectorName, instances).count;

        for (Supplier<Pipeline> supplier : pipelineSuppliers) {
            Pipeline p = supplier.get();
            Job job = instances[0].getJet().newJob(p);
            if (joinJob) {
                job.join();
            } else {
                assertThat(job).eventuallyHasStatus(JobStatus.RUNNING);
            }

            expectedCounterCount++;

            ConnectorCountInfo connectorCountInfo = countConnectorCount(connectorName, instances);
            long connectorCount = connectorCountInfo.count;

            assertThat(connectorCount)
                    .withFailMessage("Connector count for connector name \"%s\" was expected to be %d but it was %d." +
                    " The connector counts jsons: %s", connectorName, expectedCounterCount, connectorCount,
                    connectorCountInfo.connectorCountsJsonList).isEqualTo(expectedCounterCount);

            if (cleanupFn != null) {
                cleanupFn.apply(null);
            }
        }
    }

    private static ConnectorCountInfo countConnectorCount(String connectorName, HecatonInstance[] instances) {
        long count = 0;
        List<String> connectorCountsJsonList = new ArrayList<>();
        for (HecatonInstance instance : instances) {
            PhoneHome phoneHome = new PhoneHome(getNode(instance));
            Map<String, String> parameters = phoneHome.phoneHome(true);

            String connectorCountsJson = parameters.get(PhoneHomeMetrics.JET_CONNECTOR_COUNTS.getQueryParameter());
            connectorCountsJsonList.add(connectorCountsJson);
            try {
                // When there is no connector usage, connector counts are not included in the metrics.
                if (connectorCountsJson != null) {
                    JsonObject connectorCountsObj = Json.parse(connectorCountsJson).asObject();
                    count += connectorCountsObj.get(connectorName).asLong();
                }
            } catch (Throwable ignored) {
            }
        }
        return new ConnectorCountInfo(count, connectorCountsJsonList);
    }

    private static class ConnectorCountInfo {
        ConnectorCountInfo(long count, List<String> connectorCountsJsonList) {
            this.count = count;
            this.connectorCountsJsonList = connectorCountsJsonList;
        }

        /**
         * Represents the connector count, counted in the connector count jsons in all members
         */
        long count;
        /**
         * This list includes all the connector count jsons in all members
         */
        List<String> connectorCountsJsonList;
    }
}
