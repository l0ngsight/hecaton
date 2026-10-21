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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A Hierarchical Navigable Small World graph (Malkov and Yashunin, "Efficient and robust approximate
 * nearest neighbor search using Hierarchical Navigable Small World graphs") over the vectors of one
 * index of one partition.
 * <p>
 * Every node is on level 0 and, with exponentially falling probability, on the levels above; a
 * search descends greedily from the top level and explores level 0 with a candidate list of
 * {@code ef} nodes. A removed node stays in the graph, so that it can still be passed through,
 * until the graph is {@linkplain #compact() compacted}. Distances are "smaller is closer":
 * squared Euclidean distance, one minus the cosine (vectors are normalized when added) or minus
 * the dot product.
 * <p>
 * Not thread-safe: the caller locks.
 *
 * @param <K> the type of the keys
 * @param <T> the type of the payloads returned with the results
 */
final class HnswIndex<K, T> {

    private static final int INITIAL_CAPACITY = 16;
    private static final int MAX_LEVEL = 16;
    private static final int MIN_REMOVED_TO_COMPACT = 1024;
    private static final Comparator<Candidate> CLOSEST_FIRST = Comparator.comparingDouble(Candidate::distance);
    private static final Comparator<Candidate> FARTHEST_FIRST = CLOSEST_FIRST.reversed();

    private final Metric metric;
    private final int dimension;
    private final int maxDegree;
    private final int efConstruction;
    private final double levelFactor;

    private final Map<K, Integer> nodes = new HashMap<>();
    private float[][] vectors = new float[INITIAL_CAPACITY][];
    private int[][][] links = new int[INITIAL_CAPACITY][][];
    private Object[] keys = new Object[INITIAL_CAPACITY];
    private Object[] payloads = new Object[INITIAL_CAPACITY];
    private final BitSet removed = new BitSet();
    private int count;
    private int entryPoint = -1;
    private int topLevel = -1;

    HnswIndex(Metric metric, int dimension, int maxDegree, int efConstruction) {
        this.metric = metric;
        this.dimension = dimension;
        this.maxDegree = Math.max(2, maxDegree);
        this.efConstruction = Math.max(this.maxDegree, efConstruction);
        this.levelFactor = 1 / Math.log(this.maxDegree);
    }

    int dimension() {
        return dimension;
    }

    /** The number of (not removed) vectors. */
    int size() {
        return nodes.size();
    }

    /** The number of removed vectors still in the graph. */
    int removedCount() {
        return count - nodes.size();
    }

    /** Adds the vector of a key, replacing the one it had. */
    void put(K key, float[] vector, T payload) {
        if (vector.length != dimension) {
            throw new IllegalArgumentException("The vector has " + vector.length + " dimensions, the index " + dimension);
        }
        Integer old = nodes.get(key);
        float[] stored = metric == Metric.COSINE ? normalized(vector) : vector.clone();
        if (old != null && Arrays.equals(vectors[old], stored)) {
            payloads[old] = payload;
            return;
        }
        if (old != null) {
            removed.set(old);
            payloads[old] = null;
        }
        nodes.put(key, insert(key, stored, payload));
        compactIfMostlyRemoved();
    }

    /** Removes the vector of a key; returns whether there was one. */
    boolean remove(K key) {
        Integer node = nodes.remove(key);
        if (node == null) {
            return false;
        }
        removed.set(node);
        payloads[node] = null;
        if (nodes.isEmpty()) {
            clear();
        } else {
            compactIfMostlyRemoved();
        }
        return true;
    }

    void clear() {
        nodes.clear();
        vectors = new float[INITIAL_CAPACITY][];
        links = new int[INITIAL_CAPACITY][][];
        keys = new Object[INITIAL_CAPACITY];
        payloads = new Object[INITIAL_CAPACITY];
        removed.clear();
        count = 0;
        entryPoint = -1;
        topLevel = -1;
    }

    private void compactIfMostlyRemoved() {
        if (removedCount() > Math.max(MIN_REMOVED_TO_COMPACT, size())) {
            compact();
        }
    }

    /** Rebuilds the graph without the removed vectors. */
    @SuppressWarnings("unchecked")
    void compact() {
        if (removedCount() == 0) {
            return;
        }
        float[][] oldVectors = vectors;
        Object[] oldKeys = keys;
        Object[] oldPayloads = payloads;
        List<Integer> live = new ArrayList<>(nodes.values());
        live.sort(null);
        clear();
        for (int node : live) {
            K key = (K) oldKeys[node];
            nodes.put(key, insert(key, oldVectors[node], (T) oldPayloads[node]));
        }
    }

    /**
     * The {@code limit} closest vectors to {@code query}, closest first, exploring {@code ef}
     * candidates (at least {@code limit}).
     */
    @SuppressWarnings("unchecked")
    List<Hit<K, T>> search(float[] query, int limit, int ef) {
        if (query.length != dimension) {
            throw new IllegalArgumentException("The query vector has " + query.length + " dimensions, the index "
                    + dimension);
        }
        if (nodes.isEmpty() || limit <= 0) {
            return List.of();
        }
        float[] q = metric == Metric.COSINE ? normalized(query) : query;
        int width = Math.max(ef, limit);
        List<Candidate> found = count <= width ? exact(q, limit) : approximate(q, limit, width);
        List<Hit<K, T>> hits = new ArrayList<>(found.size());
        for (Candidate candidate : found) {
            hits.add(new Hit<>((K) keys[candidate.node], (T) payloads[candidate.node], score(candidate.distance)));
        }
        return hits;
    }

    private List<Candidate> approximate(float[] query, int limit, int width) {
        int node = entryPoint;
        for (int level = topLevel; level > 0; level--) {
            node = greedy(query, node, level);
        }
        List<Candidate> found = new ArrayList<>(limit);
        for (Candidate candidate : searchLevel(query, node, width, 0)) {
            if (!removed.get(candidate.node) && found.size() < limit) {
                found.add(candidate);
            }
        }
        if (found.size() < Math.min(limit, nodes.size()) && removedCount() > 0) {
            // too many removed nodes among the candidates
            return exact(query, limit);
        }
        return found;
    }

    /** The score of a distance: 1 for identical vectors, falling towards 0 as they move apart. */
    float score(float distance) {
        return switch (metric) {
            case EUCLIDEAN -> 1 / (1 + distance);
            case COSINE -> (2 - distance) / 2;
            case DOT -> (1 - distance) / 2;
        };
    }

    private int insert(K key, float[] vector, T payload) {
        int node = count++;
        ensureCapacity(count);
        int level = randomLevel();
        vectors[node] = vector;
        keys[node] = key;
        payloads[node] = payload;
        links[node] = new int[level + 1][];
        for (int l = 0; l <= level; l++) {
            links[node][l] = new int[0];
        }
        if (entryPoint < 0) {
            entryPoint = node;
            topLevel = level;
            return node;
        }
        int nearest = entryPoint;
        for (int l = topLevel; l > level; l--) {
            nearest = greedy(vector, nearest, l);
        }
        for (int l = Math.min(level, topLevel); l >= 0; l--) {
            List<Candidate> candidates = searchLevel(vector, nearest, efConstruction, l);
            List<Candidate> neighbours = selectNeighbours(candidates, degree(l));
            int[] own = new int[neighbours.size()];
            for (int i = 0; i < own.length; i++) {
                own[i] = neighbours.get(i).node;
                link(own[i], node, l);
            }
            links[node][l] = own;
            nearest = candidates.get(0).node;
        }
        if (level > topLevel) {
            entryPoint = node;
            topLevel = level;
        }
        return node;
    }

    /** Adds a link from {@code from} to {@code to} on a level, pruning the links of {@code from}. */
    private void link(int from, int to, int level) {
        int[] current = links[from][level];
        int[] extended = Arrays.copyOf(current, current.length + 1);
        extended[current.length] = to;
        if (extended.length <= degree(level)) {
            links[from][level] = extended;
            return;
        }
        List<Candidate> candidates = new ArrayList<>(extended.length);
        for (int neighbour : extended) {
            candidates.add(new Candidate(neighbour, distance(vectors[from], vectors[neighbour])));
        }
        candidates.sort(CLOSEST_FIRST);
        List<Candidate> kept = selectNeighbours(candidates, degree(level));
        int[] pruned = new int[kept.size()];
        for (int i = 0; i < pruned.length; i++) {
            pruned[i] = kept.get(i).node;
        }
        links[from][level] = pruned;
    }

    /**
     * The heuristic of the paper: a candidate is kept if it is closer to the new node than to every
     * neighbour kept so far, so that the links point in different directions; the remaining places
     * are filled with the closest of the others.
     */
    private List<Candidate> selectNeighbours(List<Candidate> closestFirst, int max) {
        List<Candidate> selected = new ArrayList<>(max);
        List<Candidate> skipped = new ArrayList<>();
        for (Candidate candidate : closestFirst) {
            if (selected.size() >= max) {
                break;
            }
            boolean diverse = true;
            for (Candidate kept : selected) {
                if (distance(vectors[candidate.node], vectors[kept.node]) < candidate.distance) {
                    diverse = false;
                    break;
                }
            }
            (diverse ? selected : skipped).add(candidate);
        }
        for (int i = 0; i < skipped.size() && selected.size() < max; i++) {
            selected.add(skipped.get(i));
        }
        return selected;
    }

    private int greedy(float[] query, int start, int level) {
        int current = start;
        float best = distance(query, vectors[current]);
        boolean improved = true;
        while (improved) {
            improved = false;
            for (int neighbour : links[current][level]) {
                float d = distance(query, vectors[neighbour]);
                if (d < best) {
                    best = d;
                    current = neighbour;
                    improved = true;
                }
            }
        }
        return current;
    }

    /** The {@code ef} closest nodes found on a level from {@code start}, closest first. */
    private List<Candidate> searchLevel(float[] query, int start, int ef, int level) {
        BitSet visited = new BitSet(count);
        PriorityQueue<Candidate> candidates = new PriorityQueue<>(CLOSEST_FIRST);
        PriorityQueue<Candidate> results = new PriorityQueue<>(FARTHEST_FIRST);
        Candidate first = new Candidate(start, distance(query, vectors[start]));
        visited.set(start);
        candidates.add(first);
        results.add(first);
        while (!candidates.isEmpty()) {
            Candidate closest = candidates.poll();
            if (results.size() >= ef && closest.distance > results.peek().distance) {
                break;
            }
            int[][] nodeLinks = links[closest.node];
            if (level >= nodeLinks.length) {
                continue;
            }
            for (int neighbour : nodeLinks[level]) {
                if (visited.get(neighbour)) {
                    continue;
                }
                visited.set(neighbour);
                float d = distance(query, vectors[neighbour]);
                if (results.size() < ef || d < results.peek().distance) {
                    Candidate candidate = new Candidate(neighbour, d);
                    candidates.add(candidate);
                    results.add(candidate);
                    if (results.size() > ef) {
                        results.poll();
                    }
                }
            }
        }
        List<Candidate> sorted = new ArrayList<>(results);
        sorted.sort(CLOSEST_FIRST);
        return sorted;
    }

    private List<Candidate> exact(float[] query, int limit) {
        PriorityQueue<Candidate> results = new PriorityQueue<>(FARTHEST_FIRST);
        for (int node : nodes.values()) {
            float d = distance(query, vectors[node]);
            if (results.size() < limit || d < results.peek().distance) {
                results.add(new Candidate(node, d));
                if (results.size() > limit) {
                    results.poll();
                }
            }
        }
        List<Candidate> sorted = new ArrayList<>(results);
        sorted.sort(CLOSEST_FIRST);
        return sorted;
    }

    private float distance(float[] a, float[] b) {
        if (metric == Metric.EUCLIDEAN) {
            float sum = 0;
            for (int i = 0; i < a.length; i++) {
                float d = a[i] - b[i];
                sum += d * d;
            }
            return sum;
        }
        float dot = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
        }
        return metric == Metric.COSINE ? 1 - dot : -dot;
    }

    private int degree(int level) {
        return level == 0 ? 2 * maxDegree : maxDegree;
    }

    private int randomLevel() {
        double uniform = 1 - ThreadLocalRandom.current().nextDouble();
        return Math.min(MAX_LEVEL, (int) (-Math.log(uniform) * levelFactor));
    }

    private void ensureCapacity(int capacity) {
        if (capacity > vectors.length) {
            int newCapacity = Math.max(capacity, vectors.length * 2);
            vectors = Arrays.copyOf(vectors, newCapacity);
            links = Arrays.copyOf(links, newCapacity);
            keys = Arrays.copyOf(keys, newCapacity);
            payloads = Arrays.copyOf(payloads, newCapacity);
        }
    }

    private static float[] normalized(float[] vector) {
        double norm = 0;
        for (float v : vector) {
            norm += v * v;
        }
        float[] result = vector.clone();
        if (norm > 0) {
            float scale = (float) (1 / Math.sqrt(norm));
            for (int i = 0; i < result.length; i++) {
                result[i] *= scale;
            }
        }
        return result;
    }

    private record Candidate(int node, float distance) {
    }

    /** A search result: the key, its payload and the score. */
    record Hit<K, T>(K key, T payload, float score) {
    }
}
