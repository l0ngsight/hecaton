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

package uk.org.hecaton.auditlog;

import uk.org.hecaton.auditlog.impl.ILoggerAuditlogService;
import uk.org.hecaton.config.AuditlogConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.security.auth.callback.CallbackHandler;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The audit log (F4): events of the cluster reach the configured audit log.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class AuditlogTest extends HecatonTestSupport {

    private final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);

    @After
    public void tearDown() {
        factory.terminateAll();
        RecordingFactory.EVENTS.clear();
    }

    @Test
    public void eventsReachTheConfiguredAuditlog() {
        Config config = smallInstanceConfig();
        config.setAuditlogConfig(new AuditlogConfig().setEnabled(true)
                .setFactoryClassName(RecordingFactory.class.getName()));
        HecatonInstance first = factory.newHecatonInstance(config);
        factory.newHecatonInstance(config);
        assertClusterSizeEventually(2, first);

        getNode(first).getNodeEngine().getWanReplicationService().addWanReplicationConfig(
                new WanReplicationConfig().setName("wan")
                        .addBatchReplicationPublisherConfig(new WanBatchPublisherConfig().setClusterName("b")
                                .setTargetEndpoints("127.0.0.1:1")));

        assertTrueEventually(() -> assertThat(RecordingFactory.EVENTS).extracting(AuditableEvent::typeId)
                .contains(AuditlogTypeIds.CLUSTER_MEMBER_ADDED, AuditlogTypeIds.WAN_ADD_CONFIG));
        AuditableEvent wan = RecordingFactory.EVENTS.stream()
                .filter(event -> event.typeId().equals(AuditlogTypeIds.WAN_ADD_CONFIG)).findFirst().orElseThrow();
        assertThat(wan.parameters()).containsEntry("wanReplicationName", "wan");
        assertThat(wan.getTimestamp()).isPositive();
    }

    @Test
    public void theDefaultAuditlogWritesToALogger() {
        Config config = smallInstanceConfig();
        config.setAuditlogConfig(new AuditlogConfig().setEnabled(true));
        HecatonInstance member = factory.newHecatonInstance(config);
        assertThat(getNode(member).getNodeExtension().getAuditlogService()).isInstanceOf(ILoggerAuditlogService.class);
    }

    @Test
    public void withoutAuditlogNothingIsRecorded() {
        HecatonInstance member = factory.newHecatonInstance(smallInstanceConfig());
        getNode(member).getNodeExtension().getAuditlogService().log("X", Level.INFO, "ignored");
        assertThat(RecordingFactory.EVENTS).isEmpty();
    }

    /** Records the events in a static list. */
    public static class RecordingFactory implements AuditlogServiceFactory {
        static final List<AuditableEvent> EVENTS = new CopyOnWriteArrayList<>();

        @Override
        public void init(CallbackHandler callbackHandler, Properties properties) {
        }

        @Override
        public AuditlogService createAuditlog() {
            return new ILoggerAuditlogService(uk.org.hecaton.logging.Logger.getLogger("test.auditlog")) {
                @Override
                public void log(AuditableEvent event) {
                    EVENTS.add(event);
                }
            };
        }
    }
}
