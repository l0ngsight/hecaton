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

package uk.org.hecaton.internal.serialization.impl.compact.schema;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.serialization.impl.compact.Schema;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.spi.exception.RetryableHecatonException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.spi.properties.ClusterProperty.INVOCATION_RETRY_PAUSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class CompactSchemaReplicationSlowTest extends CompactSchemaReplicationTestBase {

    @Test
    public void testSchemaReplication_whenAMemberThrowsRetryableExceptionAllTheTime_duringPreparationPhase() {
        StubMemberSchemaService stubMemberSchemaService = (StubMemberSchemaService) stubbedSchemaService;
        stubMemberSchemaService.setThrowOnSchemaPreparationRequest(true);

        // member at index 0 will always throw retryable exception
        // in the preparation phase, others will work fine.
        int stubbedMemberIndex = 0;
        setupInstances(index -> index == stubbedMemberIndex ? stubbedSchemaService : createSpiedMemberSchemaService());
        HecatonInstance stubbed = instances[stubbedMemberIndex];

        assertThrows(HecatonSerializationException.class, () -> fillMapUsing(instances[1]));

        for (HecatonInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);
            StubMemberSchemaService stubService = (StubMemberSchemaService) service;

            if (instance == stubbed) {
                // For stubbed member, it must fail all the time
                assertThat(stubService.schemaPreparationRequestCounter)
                        .isGreaterThanOrEqualTo(SchemaReplicator.MAX_RETRIES_FOR_REQUESTS);
            }

            // No-one should call onSchemaAckRequest
            assertNotEquals(SchemaReplicationStatus.REPLICATED, service.getReplicator().getReplicationStatus(SCHEMA));
        }
    }

    @Test
    public void testSchemaReplication_whenAMemberThrowsRetryableExceptionAllTheTime_duringAcknowledgmentPhase() {
        StubMemberSchemaService stubMemberSchemaService = (StubMemberSchemaService) stubbedSchemaService;
        stubMemberSchemaService.setThrowOnSchemaAckRequest(true);

        // member at index 3 will always throw retryable exception
        // in the acknowledgment phase, others will work fine.
        int stubbedMemberIndex = 3;
        setupInstances(index -> index == stubbedMemberIndex ? stubbedSchemaService : createSpiedMemberSchemaService());
        HecatonInstance stubbed = instances[stubbedMemberIndex];

        HecatonInstance initiator = instances[0];
        assertThrows(HecatonSerializationException.class, () -> fillMapUsing(initiator));

        for (HecatonInstance instance : instances) {
            MemberSchemaService service = getSchemaService(instance);
            StubMemberSchemaService stubService = (StubMemberSchemaService) service;

            // Everyone should call onSchemaPreparationRequest, apart from the initiator
            if (instance != initiator) {
                assertThat(stubService.getSchemaPreparationRequestCounter())
                        .isPositive();
            }

            if (instance == stubbed) {
                // For stubbed member, it must fail all the time
                assertThat(stubService.getSchemaAckRequestCounter())
                        .isGreaterThanOrEqualTo(SchemaReplicator.MAX_RETRIES_FOR_REQUESTS);
            }
        }
    }

    @Override
    public Config getConfig() {
        Config config = super.getConfig();
        config.getProperties().setProperty(INVOCATION_RETRY_PAUSE.getName(), String.valueOf(SchemaReplicator.MAX_RETRIES_FOR_REQUESTS));
        return config;
    }

    // Mockito.spy() call fails on Zing. So create a test stub
    @Override
    protected StubMemberSchemaService createSpiedMemberSchemaService() {
        return new StubMemberSchemaService();
    }

    // Test stub for Zing
    protected static class StubMemberSchemaService extends MemberSchemaService {
        // Flag that indicates whether exception should be thrown
        private boolean throwOnSchemaPreparationRequest;

        // Counts number of onSchemaPreparationRequest() calls
        private int schemaPreparationRequestCounter;

        // Flag that indicates whether exception should be thrown
        private boolean throwOnSchemaAckRequest;

        // Counts number of onSchemaAckRequest() calls
        private int schemaAckRequestCounter;

        public void setThrowOnSchemaPreparationRequest(boolean throwOnSchemaPreparationRequest) {
            this.throwOnSchemaPreparationRequest = throwOnSchemaPreparationRequest;
        }

        public void setThrowOnSchemaAckRequest(boolean throwOnSchemaAckRequest) {
            this.throwOnSchemaAckRequest = throwOnSchemaAckRequest;
        }

        public int getSchemaAckRequestCounter() {
            return schemaAckRequestCounter;
        }

        public int getSchemaPreparationRequestCounter() {
            return schemaPreparationRequestCounter;
        }

        @Override
        public void onSchemaPreparationRequest(Schema schema) {
            if (schema.equals(SCHEMA)) {
                schemaPreparationRequestCounter++;
            }

            if (throwOnSchemaPreparationRequest) {
                throw new RetryableHecatonException();
            }
            super.onSchemaPreparationRequest(schema);
        }

        @Override
        public void onSchemaAckRequest(long schemaId) {
            if (schemaId == SCHEMA.getSchemaId()) {
                schemaAckRequestCounter++;
            }

            if (throwOnSchemaAckRequest) {
                throw new RetryableHecatonException();
            }
            super.onSchemaAckRequest(schemaId);
        }
    }
}
