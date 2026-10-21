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

import uk.org.hecaton.config.vector.Metric;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HnswIndexTest {

    private static final int DIMENSION = 24;
    private static final int COUNT = 3000;
    private static final int QUERIES = 50;
    private static final int K = 10;

    private final Random random = new Random(42);

    @Test
    public void recallIsHighForEveryMetric() {
        for (Metric metric : Metric.values()) {
            HnswIndex<Integer, String> index = new HnswIndex<>(metric, DIMENSION, 16, 100);
            List<float[]> vectors = new ArrayList<>();
            for (int i = 0; i < COUNT; i++) {
                float[] vector = randomVector();
                vectors.add(vector);
                index.put(i, vector, "v" + i);
            }
            assertThat(recall(index, vectors, metric, 64)).as(metric.name()).isGreaterThan(0.9);
        }
    }

    @Test
    public void updatesAndRemovalsAreSeenBySearches() {
        HnswIndex<Integer, String> index = new HnswIndex<>(Metric.EUCLIDEAN, DIMENSION, 16, 100);
        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < COUNT; i++) {
            float[] vector = randomVector();
            vectors.add(vector);
            index.put(i, vector, "v" + i);
        }
        // move half of the vectors, remove a quarter
        for (int i = 0; i < COUNT / 2; i++) {
            float[] vector = randomVector();
            vectors.set(i, vector);
            index.put(i, vector, "moved" + i);
        }
        for (int i = COUNT / 2; i < COUNT * 3 / 4; i++) {
            index.remove(i);
            vectors.set(i, null);
        }
        assertThat(index.size()).isEqualTo(COUNT * 3 / 4);
        assertThat(recall(index, vectors, Metric.EUCLIDEAN, 64)).isGreaterThan(0.85);

        List<HnswIndex.Hit<Integer, String>> hits = index.search(vectors.get(3), 1, 64);
        assertThat(hits.get(0).key()).isEqualTo(3);
        assertThat(hits.get(0).payload()).isEqualTo("moved3");
        assertThat(hits.get(0).score()).isEqualTo(1f);

        index.compact();
        assertThat(index.removedCount()).isZero();
        assertThat(index.size()).isEqualTo(COUNT * 3 / 4);
        assertThat(recall(index, vectors, Metric.EUCLIDEAN, 64)).isGreaterThan(0.9);
    }

    @Test
    public void smallIndexesAreSearchedExactly() {
        HnswIndex<Integer, String> index = new HnswIndex<>(Metric.DOT, 2, 4, 10);
        index.put(1, new float[] {1, 0}, "a");
        index.put(2, new float[] {0, 1}, "b");
        index.put(3, new float[] {-1, 0}, "c");
        List<HnswIndex.Hit<Integer, String>> hits = index.search(new float[] {1, 0.1f}, 3, 10);
        assertThat(hits).extracting(HnswIndex.Hit::key).containsExactly(1, 2, 3);
        assertThat(hits.get(0).score()).isEqualTo(1f);
        assertThat(hits.get(2).score()).isEqualTo(0f);
        index.remove(1);
        index.remove(2);
        index.remove(3);
        assertThat(index.search(new float[] {1, 0}, 3, 10)).isEmpty();
        assertThatThrownBy(() -> index.put(4, new float[3], "d")).isInstanceOf(IllegalArgumentException.class);
    }

    private double recall(HnswIndex<Integer, String> index, List<float[]> vectors, Metric metric, int ef) {
        int found = 0;
        int expected = 0;
        for (int q = 0; q < QUERIES; q++) {
            float[] query = randomVector();
            Set<Integer> exact = exact(vectors, query, metric);
            Set<Integer> approximate = index.search(query, K, ef).stream().map(HnswIndex.Hit::key)
                    .collect(Collectors.toSet());
            approximate.retainAll(exact);
            found += approximate.size();
            expected += exact.size();
        }
        return (double) found / expected;
    }

    private static Set<Integer> exact(List<float[]> vectors, float[] query, Metric metric) {
        List<Integer> ids = new ArrayList<>();
        for (int i = 0; i < vectors.size(); i++) {
            if (vectors.get(i) != null) {
                ids.add(i);
            }
        }
        ids.sort(Comparator.comparingDouble(i -> -similarity(vectors.get(i), query, metric)));
        return new HashSet<>(ids.subList(0, Math.min(K, ids.size())));
    }

    private static double similarity(float[] a, float[] b, Metric metric) {
        double dot = 0;
        double na = 0;
        double nb = 0;
        double l2 = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
            l2 += (a[i] - b[i]) * (a[i] - b[i]);
        }
        return switch (metric) {
            case EUCLIDEAN -> -l2;
            case COSINE -> dot / Math.sqrt(na * nb);
            case DOT -> dot;
        };
    }

    private float[] randomVector() {
        float[] vector = new float[DIMENSION];
        for (int i = 0; i < DIMENSION; i++) {
            vector[i] = (float) random.nextGaussian();
        }
        return vector;
    }
}
