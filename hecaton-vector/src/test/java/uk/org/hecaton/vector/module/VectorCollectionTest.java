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

package uk.org.hecaton.vector.module;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.vector.Metric;
import uk.org.hecaton.config.vector.VectorCollectionConfig;
import uk.org.hecaton.config.vector.VectorIndexConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.HecatonFeature;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.vector.SearchOptions;
import uk.org.hecaton.vector.SearchResult;
import uk.org.hecaton.vector.SearchResults;
import uk.org.hecaton.vector.VectorCollection;
import uk.org.hecaton.vector.VectorDocument;
import uk.org.hecaton.vector.VectorValues;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.offset;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorCollectionTest extends HecatonTestSupport {

    private static final int DIMENSION = 8;
    private static final int COUNT = 1000;

    private final TestHecatonFactory factory = new TestHecatonFactory();
    private final Random random = new Random(7);

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void featureIsAvailable() {
        assertThat(HecatonFeature.VECTOR_COLLECTION.isAvailable()).isTrue();
    }

    @Test
    public void documentsAreStoredAndSearched() {
        HecatonInstance[] members = factory.newInstances(this::config, 3);
        VectorCollection<Integer, String> collection = VectorCollection.getCollection(members[0], "docs");
        float[][] vectors = fill(collection);
        assertThat(collection.size()).isEqualTo(COUNT);

        VectorDocument<String> document = collection.getAsync(5).toCompletableFuture().join();
        assertThat(document.getValue()).isEqualTo("doc5");
        assertThat(((VectorValues.SingleVectorValues) document.getVectors()).vector()).isEqualTo(vectors[5]);

        // every member finds a stored vector as the best match
        for (HecatonInstance member : members) {
            VectorCollection<Integer, String> view = VectorCollection.getCollection(member, "docs");
            for (int i = 0; i < 20; i++) {
                SearchResults<Integer, String> results = view.searchAsync(VectorValues.of(vectors[i]),
                        SearchOptions.builder().limit(5).includeValue().includeVectors().build())
                        .toCompletableFuture().join();
                assertThat(results.size()).isEqualTo(5);
                SearchResult<Integer, String> best = results.results().next();
                assertThat(best.getKey()).isEqualTo(i);
                assertThat(best.getValue()).isEqualTo("doc" + i);
                assertThat(best.getScore()).isEqualTo(1f, offset(1e-5f));
                assertThat(((VectorValues.SingleVectorValues) best.getVectors()).vector()).isEqualTo(vectors[i]);
            }
        }
        // without values and vectors
        SearchResult<Integer, String> bare = collection.searchAsync(VectorValues.of(vectors[1]), SearchOptions.of(1, false, false))
                .toCompletableFuture().join().results().next();
        assertThat(bare.getKey()).isEqualTo(1);
        assertThat(bare.getValue()).isNull();
        assertThat(bare.getVectors()).isNull();
    }

    @Test
    public void writesUpdateTheIndexes() {
        HecatonInstance member = factory.newHecatonInstance(config());
        VectorCollection<Integer, String> collection = VectorCollection.getCollection(member, "docs");
        float[][] vectors = fill(collection);
        float[] moved = randomVector();
        assertThat(collection.putAsync(1, VectorDocument.of("moved", VectorValues.of(moved)))
                .toCompletableFuture().join().getValue()).isEqualTo("doc1");
        assertThat(best(collection, moved)).isEqualTo(1);
        assertThat(best(collection, vectors[1])).isNotEqualTo(1);

        assertThat(collection.putIfAbsentAsync(1, VectorDocument.of("x", VectorValues.of(vectors[1])))
                .toCompletableFuture().join().getValue()).isEqualTo("moved");
        assertThat(collection.removeAsync(2).toCompletableFuture().join().getValue()).isEqualTo("doc2");
        collection.deleteAsync(3).toCompletableFuture().join();
        assertThat(best(collection, vectors[2])).isNotEqualTo(2);
        assertThat(best(collection, vectors[3])).isNotEqualTo(3);
        assertThat(collection.size()).isEqualTo(COUNT - 2);

        collection.optimizeAsync().toCompletableFuture().join();
        assertThat(best(collection, moved)).isEqualTo(1);

        collection.clearAsync().toCompletableFuture().join();
        assertThat(collection.size()).isZero();
        assertThat(collection.searchAsync(VectorValues.of(moved), SearchOptions.of(3, true, false))
                .toCompletableFuture().join().size()).isZero();
    }

    @Test
    public void indexesFollowMigrationsAndMemberLoss() {
        HecatonInstance first = factory.newHecatonInstance(config());
        VectorCollection<Integer, String> collection = VectorCollection.getCollection(first, "docs");
        float[][] vectors = fill(collection);
        HecatonInstance second = factory.newHecatonInstance(config());
        HecatonInstance third = factory.newHecatonInstance(config());
        waitAllForSafeState(first, second, third);
        assertAllFound(VectorCollection.getCollection(third, "docs"), vectors);

        first.getLifecycleService().terminate();
        waitAllForSafeState(second, third);
        VectorCollection<Integer, String> survivor = VectorCollection.getCollection(second, "docs");
        assertThat(survivor.size()).isEqualTo(COUNT);
        assertAllFound(survivor, vectors);
    }

    @Test
    public void severalIndexes() {
        Config config = smallInstanceConfig().addVectorCollectionConfig(new VectorCollectionConfig("multi")
                .addVectorIndexConfig(new VectorIndexConfig("text", Metric.COSINE, 3))
                .addVectorIndexConfig(new VectorIndexConfig("image", Metric.EUCLIDEAN, 2)));
        HecatonInstance member = factory.newHecatonInstance(config);
        VectorCollection<String, String> collection = VectorCollection.getCollection(member, "multi");
        collection.setAsync("a", VectorDocument.of("A", VectorValues.of("text", new float[] {1, 0, 0},
                "image", new float[] {0, 0}))).toCompletableFuture().join();
        collection.setAsync("b", VectorDocument.of("B", VectorValues.of("text", new float[] {0, 1, 0},
                "image", new float[] {5, 5}))).toCompletableFuture().join();
        assertThat(collection.searchAsync(VectorValues.of("text", new float[] {0, 2, 0}), SearchOptions.of(1, true, false))
                .toCompletableFuture().join().results().next().getKey()).isEqualTo("b");
        assertThat(collection.searchAsync(VectorValues.of("image", new float[] {0.5f, 0}), SearchOptions.of(1, true, false))
                .toCompletableFuture().join().results().next().getKey()).isEqualTo("a");

        assertThatThrownBy(() -> collection.setAsync("c", VectorDocument.of("C", VectorValues.of(new float[] {1, 0, 0}))))
                .hasMessageContaining("has 2 indexes");
        assertThatThrownBy(() -> collection.setAsync("c", VectorDocument.of("C", VectorValues.of("text", new float[] {1, 0, 0}))))
                .hasMessageContaining("no vector for the index 'image'");
        assertThatThrownBy(() -> collection.searchAsync(VectorValues.of(new float[] {1, 0, 0}), SearchOptions.of(1, true, false)))
                .hasMessageContaining("name the index");
    }

    @Test
    public void collectionNeedsAConfiguration() {
        HecatonInstance member = factory.newHecatonInstance(smallInstanceConfig());
        assertThatThrownBy(() -> VectorCollection.getCollection(member, "unknown"))
                .hasStackTraceContaining("has no configuration with indexes");
        // a configuration added at runtime
        VectorCollection<Integer, String> collection = VectorCollection.getCollection(member,
                new VectorCollectionConfig("runtime").addVectorIndexConfig(new VectorIndexConfig(null, Metric.DOT, 2)));
        collection.setAsync(1, VectorDocument.of("one", VectorValues.of(new float[] {1, 1}))).toCompletableFuture().join();
        assertThat(best(collection, new float[] {1, 1})).isEqualTo(1);
    }

    @Test
    public void client() {
        HecatonInstance[] members = factory.newInstances(this::config, 2);
        HecatonInstance client = factory.newHecatonClient();
        VectorCollection<Integer, String> collection = VectorCollection.getCollection(client, "docs");
        float[][] vectors = fill(collection);
        assertThat(collection.size()).isEqualTo(COUNT);
        assertThat(VectorCollection.<Integer, String>getCollection(members[1], "docs").size()).isEqualTo(COUNT);
        assertThat(collection.getAsync(9).toCompletableFuture().join().getValue()).isEqualTo("doc9");
        assertAllFound(collection, vectors);
        SearchResult<Integer, String> best = collection.searchAsync(VectorValues.of(vectors[4]),
                SearchOptions.builder().limit(3).includeValue().includeVectors().build())
                .toCompletableFuture().join().results().next();
        assertThat(best.getKey()).isEqualTo(4);
        assertThat(best.getValue()).isEqualTo("doc4");
        assertThat(((VectorValues.SingleVectorValues) best.getVectors()).vector()).isEqualTo(vectors[4]);

        assertThat(collection.putIfAbsentAsync(4, VectorDocument.of("x", VectorValues.of(vectors[0])))
                .toCompletableFuture().join().getValue()).isEqualTo("doc4");
        assertThat(collection.removeAsync(4).toCompletableFuture().join().getValue()).isEqualTo("doc4");
        collection.deleteAsync(5).toCompletableFuture().join();
        collection.optimizeAsync().toCompletableFuture().join();
        assertThat(collection.size()).isEqualTo(COUNT - 2);
        assertThatThrownBy(() -> collection.setAsync(1, VectorDocument.of("bad", VectorValues.of(new float[3])))
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class).hasMessageContaining("dimensions");
        collection.clearAsync().toCompletableFuture().join();
        assertThat(collection.size()).isZero();
        collection.destroy();
    }

    private Config config() {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "31");
        config.addVectorCollectionConfig(new VectorCollectionConfig("docs").setBackupCount(1)
                .addVectorIndexConfig(new VectorIndexConfig(null, Metric.EUCLIDEAN, DIMENSION)));
        return config;
    }

    private float[][] fill(VectorCollection<Integer, String> collection) {
        float[][] vectors = new float[COUNT][];
        Map<Integer, VectorDocument<String>> batch = new HashMap<>();
        for (int i = 0; i < COUNT; i++) {
            vectors[i] = randomVector();
            if (i < COUNT / 2) {
                collection.putAsync(i, VectorDocument.of("doc" + i, VectorValues.of(vectors[i]))).toCompletableFuture().join();
            } else {
                batch.put(i, VectorDocument.of("doc" + i, VectorValues.of(vectors[i])));
            }
        }
        collection.putAllAsync(batch).toCompletableFuture().join();
        return vectors;
    }

    private static void assertAllFound(VectorCollection<Integer, String> collection, float[][] vectors) {
        List<Integer> missed = new ArrayList<>();
        for (int i = 0; i < vectors.length; i += 10) {
            if (best(collection, vectors[i]) != i) {
                missed.add(i);
            }
        }
        assertThat(missed).isEmpty();
    }

    private static int best(VectorCollection<Integer, ?> collection, float[] vector) {
        SearchResults<Integer, ?> results = collection.searchAsync(VectorValues.of(vector), SearchOptions.of(1, false, false))
                .toCompletableFuture().join();
        return results.size() == 0 ? -1 : results.results().next().getKey();
    }

    private float[] randomVector() {
        float[] vector = new float[DIMENSION];
        for (int i = 0; i < DIMENSION; i++) {
            vector[i] = (float) random.nextGaussian();
        }
        return vector;
    }
}
