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

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.TestInClusterSupport;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.pipeline.test.TestSources;
import uk.org.hecaton.map.IMap;
import org.junit.Before;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.cache.Cache;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static uk.org.hecaton.jet.impl.util.ExceptionUtil.peel;
import static java.nio.file.Files.readAllBytes;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public abstract class PipelineTestSupport extends TestInClusterSupport {

    protected int itemCount = 1024;
    protected final String srcName = journaledMapName();
    protected final String sinkName = randomName();

    protected Pipeline p;
    protected Sink<Object> sink;
    protected BatchSource<Object> source;

    protected IMap<String, Integer> srcMap;
    protected IList<Object> srcList;
    protected IList<Object> sinkList;

    protected ICache<String, Integer> srcCache;

    @Before
    public void beforePipelineTestSupport() {
        p = Pipeline.create();
        srcMap = hz().getMap(srcName);
        srcCache = hz().getCacheManager().getCache(srcName);
        srcList = hz().getList(srcName);
        source = Sources.list(srcName);

        sink = Sinks.list(sinkName);
        sinkList = hz().getList(sinkName);
    }

    protected Job execute() {
        return execute(new JobConfig());
    }

    protected Job execute(JobConfig config) {
        return execute(p, config);
    }

    protected void executeAndPeel() throws Throwable {
        try {
            execute();
        } catch (CompletionException e) {
            throw peel(e);
        }
    }

    protected Job start() {
        return hz().getJet().newJob(p);
    }

    protected Job start(JobConfig jobConfig) {
        return hz().getJet().newJob(p, jobConfig);
    }

    protected BatchStage<Integer> batchStageFromList(List<Integer> input) {
        return p.readFrom(TestSources.items(input));
    }

    protected static String journaledMapName() {
        return randomMapName(JOURNALED_MAP_PREFIX);
    }

    protected static String journaledCacheName() {
        return randomMapName(JOURNALED_CACHE_PREFIX);
    }

    protected void addToSrcList(Collection<Integer> data) {
        srcList.addAll(data);
    }

    protected void putToBatchSrcMap(Collection<Integer> data) {
        putToMap(srcMap, data);
    }

    protected void putToBatchSrcCache(Collection<Integer> data) {
        putToCache(srcCache, data);
    }

    protected static void putToMap(Map<String, Integer> dest, Collection<Integer> data) {
        int[] key = {0};
        data.forEach(i -> dest.put(String.valueOf(key[0]++), i));
    }

    protected static void putToCache(Cache<String, Integer> dest, Collection<Integer> data) {
        int[] key = {0};
        data.forEach(i -> dest.put(String.valueOf(key[0]++), i));
    }

    protected <T> Sink<T> sinkList() {
        return Sinks.list(sinkName);
    }

    protected <T> Stream<T> sinkStreamOf(Class<T> type) {
        return sinkList.stream().map(type::cast);
    }

    @SuppressWarnings("unchecked")
    protected <K, V> Stream<Entry<K, V>> sinkStreamOfEntry() {
        return sinkList.stream().map(Entry.class::cast);
    }

    @SuppressWarnings("unchecked")
    protected <T> Map<T, Integer> sinkToBag() {
        return toBag((List<T>) this.sinkList);
    }

    /**
     * Uses {@code formatFn} to stringify each item of the given stream, sorts
     * the strings, then outputs them line by line.
     * <p>
     * If you supply the optional {@code distinctKeyFn}, it will use it to
     * eliminate the items with the same key, keeping the last one in the
     * stream. Keeping the last duplicate item is the way to de-duplicate a
     * stream of early window results.
     */
    protected static <T, K> String streamToString(
            @Nonnull Stream<? extends T> stream,
            @Nonnull Function<? super T, ? extends String> formatFn,
            @Nullable Function<? super T, ? extends K> distinctKeyFn
    ) {
        if (distinctKeyFn != null) {
            stream = stream.collect(toMap(distinctKeyFn, identity(), (t0, t1) -> t1))
                           .values().stream();
        }
        return stream.map(formatFn)
                     .sorted()
                     .collect(joining("\n"));
    }

    /**
     * Uses {@code formatFn} to stringify each item of the given stream, sorts
     * the strings, then outputs them line by line.
     */
    protected static <T> String streamToString(
            @Nonnull Stream<? extends T> stream,
            @Nonnull Function<? super T, ? extends String> formatFn
    ) {
        return streamToString(stream, formatFn, null);
    }

    protected static <T> Map<T, Integer> toBag(Collection<T> coll) {
        Map<T, Integer> bag = new HashMap<>();
        for (T t : coll) {
            bag.merge(t, 1, (count, x) -> count + 1);
        }
        return bag;
    }

    protected static List<Integer> sequence(int itemCount) {
        return IntStream.range(0, itemCount).boxed().collect(toList());
    }

    protected static long roundDown(long value, long unit) {
        return value - value % unit;
    }

    protected static long roundUp(long value, long unit) {
        return roundDown(value + unit - 1, unit);
    }

    protected static List<HecatonInstance> createRemoteCluster(Config config, int size) {
        ArrayList<HecatonInstance> instances = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            instances.add(Hecaton.newHecatonInstance(config));
        }
        return instances;
    }

    protected static ClientConfig getClientConfigForRemoteCluster(HecatonInstance instance) {
        ClientConfig clientConfig = new ClientConfig();
        Address address = instance.getCluster().getLocalMember().getAddress();
        clientConfig.getNetworkConfig().addAddress(address.getHost() + ':' + address.getPort());
        clientConfig.setClusterName(instance.getConfig().getClusterName());
        return clientConfig;
    }

    static String readLocalClusterConfig(String file, String clusterName, @Nonnull HecatonInstance instance) throws IOException {
        int port = ((InetSocketAddress) instance.getLocalEndpoint().getSocketAddress()).getPort();
        byte[] bytes = readAllBytes(Paths.get("src", "test", "resources", file));
        return new String(bytes, StandardCharsets.UTF_8)
                .replace("$CLUSTER_NAME$", clusterName)
                .replace("$PORT_NUMBER$", String.valueOf(port));
    }
}
