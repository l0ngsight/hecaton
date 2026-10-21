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

package uk.org.hecaton.jet.impl;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.core.AbstractProcessor;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Isolated jobs (F18): a job with a member selector runs on the selected members only.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class IsolatedJobTest extends HecatonTestSupport {

    private static final Set<UUID> RAN_ON = ConcurrentHashMap.newKeySet();

    private HecatonInstance selected;
    private HecatonInstance other;

    @Before
    public void setUp() {
        RAN_ON.clear();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        selected = factory.newHecatonInstance(config("jet"));
        other = factory.newHecatonInstance(config("data"));
        assertClusterSizeEventually(2, selected, other);
    }

    @Test
    public void normalJobRunsOnSelectedMembersOnly() {
        other.getJet().newJobBuilder(recordingDag())
                .withMemberSelector(IsolatedJobTest::isJetMember)
                .start()
                .join();

        assertThat(RAN_ON).containsExactly(selected.getCluster().getLocalMember().getUuid());
    }

    @Test
    public void lightJobRunsOnSelectedMembersOnly() {
        other.getJet().newJobBuilder(recordingDag())
                .withMemberSelector(IsolatedJobTest::isJetMember)
                .asLightJob()
                .start()
                .join();

        assertThat(RAN_ON).containsExactly(selected.getCluster().getLocalMember().getUuid());
    }

    private static boolean isJetMember(Member member) {
        return "jet".equals(member.getAttribute("role"));
    }

    private static Config config(String role) {
        Config config = smallInstanceConfig();
        config.getJetConfig().setEnabled(true);
        config.getMemberAttributeConfig().setAttribute("role", role);
        return config;
    }

    private static DAG recordingDag() {
        DAG dag = new DAG();
        dag.newVertex("record", RecordingP::new);
        return dag;
    }

    /** Records the member it runs on. */
    private static final class RecordingP extends AbstractProcessor {

        @Override
        protected void init(@Nonnull Context context) {
            RAN_ON.add(context.hecatonInstance().getCluster().getLocalMember().getUuid());
        }

        @Override
        public boolean complete() {
            return true;
        }
    }
}
