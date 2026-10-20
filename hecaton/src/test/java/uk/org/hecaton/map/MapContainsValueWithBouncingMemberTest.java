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

package uk.org.hecaton.map;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.SlowTest;
import uk.org.hecaton.test.bounce.BounceMemberRule;
import uk.org.hecaton.test.bounce.BounceTestConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class MapContainsValueWithBouncingMemberTest extends HecatonTestSupport {

    private static final int MAP_SIZE = 100_000;
    private static final int DURATION_SECONDS = 30;
    private static final Logger log = LoggerFactory.getLogger(MapContainsValueWithBouncingMemberTest.class);

    @Parameterized.Parameter
    public boolean ttlEnabled;

    @Parameterized.Parameters(name = "ttlEnabled: {0}")
    public static Collection<Boolean> parameters() {
        return asList(false, true);
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfigWithoutJetAndMetrics();
    }

    @Rule
    public BounceMemberRule bounceMemberRule =
            BounceMemberRule.with(this::getConfig)
                    .driverType(BounceTestConfiguration.DriverType.MEMBER)
                    .clusterSize(2)
                    .driverCount(2)
                    .build();

    @Test
    public void testContainsValue_whenAddingAndTerminatingMembers_thenContainsValueShouldNotFail() {
        final IMap<String, Object> map = bounceMemberRule.getSteadyMember().getMap(randomMapName());
        populateMap(map);

        bounceMemberRule.testRepeatedly(new Runnable[]{
                () -> {
                    // regenerate some expired entries
                    putEntry(map, ThreadLocalRandom.current().nextInt(MAP_SIZE));

                    assertThat(map.containsValue("whatever")).isFalse();
                }
        }, DURATION_SECONDS);

        log.info("size after bouncing: " + map.size());
    }

    private void populateMap(IMap<String, Object> map) {
        for (int i = 0; i < MAP_SIZE; i++) {
            putEntry(map, i);
        }
    }

    private void putEntry(IMap<String, Object> map, int index) {
        if (ttlEnabled) {
            map.put("name" + index, index, ThreadLocalRandom.current().nextInt(5, 20), SECONDS);
        } else {
            map.put("name" + index, index);
        }
    }
}
