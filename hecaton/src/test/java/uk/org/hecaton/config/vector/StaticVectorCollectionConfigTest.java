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

package uk.org.hecaton.config.vector;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.stream.Collectors;

import static uk.org.hecaton.config.vector.VectorTestHelper.buildVectorCollectionConfig;
import static java.util.function.Function.identity;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StaticVectorCollectionConfigTest extends HecatonTestSupport {

    @Test
    public void setOneVectorCollectionConfig() {
        String collectionName = "vector-collection-1";
        var vectorCollectionConfig = buildVectorCollectionConfig(collectionName, "index-1", 2, Metric.EUCLIDEAN);
        Config config = new Config().addVectorCollectionConfig(vectorCollectionConfig);

        HecatonInstance instance = createHecatonInstance(config);

        var actual = instance.getConfig().getVectorCollectionConfigOrNull(collectionName);
        assertThat(actual).isEqualTo(vectorCollectionConfig);
    }

    @Test
    public void setSeveralVectorCollectionConfig() {
        var vectorCollectionConfigs = range(0, 3)
                .mapToObj(i -> buildVectorCollectionConfig("collection-" + i, "index-" + i, i + 1, Metric.EUCLIDEAN)
                        .setBackupCount(i).setAsyncBackupCount(i))
                .collect(Collectors.toMap(VectorCollectionConfig::getName, identity()));
        Config config = new Config().setVectorCollectionConfigs(vectorCollectionConfigs);

        HecatonInstance instance = createHecatonInstance(config);

        var actual = instance.getConfig().getVectorCollectionConfigs();
        assertThat(actual.entrySet()).containsExactlyInAnyOrderElementsOf(vectorCollectionConfigs.entrySet());
    }

    @Test
    public void memberTest_addAndGetWildcardVectorCollection_then_success() {
        String collectionName = "vector-collection-*";
        var vectorCollectionConfig = buildVectorCollectionConfig(collectionName, "index-1", 2, Metric.EUCLIDEAN);
        Config config = new Config().addVectorCollectionConfig(vectorCollectionConfig);

        HecatonInstance instance = createHecatonInstance(config);

        var actual = instance.getConfig().getVectorCollectionConfigOrNull("vector-collection-123");
        assertThat(actual).isEqualTo(vectorCollectionConfig);
    }
}
