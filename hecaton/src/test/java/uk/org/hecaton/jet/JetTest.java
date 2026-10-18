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

package uk.org.hecaton.jet;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.impl.JetClientInstanceImpl;
import uk.org.hecaton.jet.impl.exception.JetDisabledException;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.test.TestSources;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.jet.impl.JobRepository.INTERNAL_JET_OBJECTS_PREFIX;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JetTest extends JetTestSupport {

    @Test
    public void when_defaultMapConfig_then_notUsed() {
        // When
        Config config = smallInstanceConfig();
        config.getMapConfig("default")
                .setTimeToLiveSeconds(MapConfig.DEFAULT_TTL_SECONDS + 1);
        HecatonInstance instance = createHecatonInstance(config);

        // Then
        int actualTTL = instance.getConfig()
                .findMapConfig(INTERNAL_JET_OBJECTS_PREFIX + "fooMap").getTimeToLiveSeconds();
        assertEquals(MapConfig.DEFAULT_TTL_SECONDS, actualTTL);
    }

    @Test
    public void when_jetDisabled_then_getJetInstanceThrowsException() {
        // When
        Config config = smallInstanceConfig();
        config.getJetConfig().setEnabled(false);
        HecatonInstance instance = createHecatonInstance(config);

        // Then
        assertThrows(JetDisabledException.class, instance::getJet);
    }

    @Test
    public void when_jetDisabled_and_usingClient_then_getJetInstanceThrowsException() {
        // When
        Config config = smallInstanceConfig();
        config.getJetConfig().setEnabled(false);
        createHecatonInstance(config);
        HecatonInstance client = createHecatonClient();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1))
                .writeTo(Sinks.noop());

        assertThrows(JetDisabledException.class, () -> client.getJet().newJob(p).join());
    }

    @Test
    public void when_jetDisabled_and_usingClient_then_getSummaryListThrowsException() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setEnabled(false);
        createHecatonInstance(config);
        HecatonInstance client = createHecatonClient();
        JetClientInstanceImpl jet = (JetClientInstanceImpl) client.getJet();

        assertThrows(JetDisabledException.class, jet::getJobSummaryList);
        assertThrows(JetDisabledException.class, jet::getJobAndSqlSummaryList);
    }

    @Test
    public void when_resourceUploadDisabled_and_submitJobWithResource_then_jobFails() {
        // When
        HecatonInstance hz = createHecatonInstance();

        JobConfig jobConfig = new JobConfig().addClass(JetTest.class);
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(1))
                .writeTo(Sinks.noop());

        // Then
        assertThrows(JetException.class, () -> hz.getJet().newJob(p, jobConfig));
    }

}
