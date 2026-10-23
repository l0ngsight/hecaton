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

package uk.org.hecaton.collection.impl.queue;

import uk.org.hecaton.collection.ItemListener;
import uk.org.hecaton.collection.impl.queue.operations.AddAllOperation;
import uk.org.hecaton.collection.impl.queue.operations.ClearOperation;
import uk.org.hecaton.collection.impl.queue.operations.CompareAndRemoveOperation;
import uk.org.hecaton.collection.impl.queue.operations.ContainsOperation;
import uk.org.hecaton.collection.impl.queue.operations.DrainOperation;
import uk.org.hecaton.collection.impl.queue.operations.IteratorOperation;
import uk.org.hecaton.collection.impl.queue.operations.OfferOperation;
import uk.org.hecaton.collection.impl.queue.operations.PeekOperation;
import uk.org.hecaton.collection.impl.queue.operations.PollOperation;
import uk.org.hecaton.collection.impl.queue.operations.RemainingCapacityOperation;
import uk.org.hecaton.collection.impl.queue.operations.RemoveOperation;
import uk.org.hecaton.collection.impl.queue.operations.SizeOperation;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.ConcurrencyUtil;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.partition.strategy.StringPartitioningStrategy;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.SerializableList;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * One logical queue spread over several physical queues, its shards, each on its own partition
 * (load protection, F23). A member keeps one instance per sharded queue that it uses, shared by
 * the queue's member-side proxy and by the tasks of client requests.
 * <p>
 * The member keeps a hint of each shard's size, from the results of its own operations and a
 * refresh every {@value #REFRESH_MILLIS} ms while the queue is in use. Offers go to the shard with
 * the fewest items and polls take from the shard with the most, which keeps the shards balanced and
 * the queue as a whole roughly FIFO; within a shard, FIFO holds. A poll or offer that has to wait
 * waits on one shard at a time, for at most {@value #WAIT_SLICE_MILLIS} ms, then looks at all
 * shards again: no thread is blocked while it waits.
 */
@SuppressWarnings("checkstyle:classdataabstractioncoupling")
public final class ShardedQueue {

    /** Size hints are refreshed this often while the queue is in use. */
    static final long REFRESH_MILLIS = 50;
    /** A waiting poll or offer waits on one shard at a time for at most this long. */
    static final long WAIT_SLICE_MILLIS = 50;
    private static final long IDLE_MILLIS = 1_000;
    private static final long FULL = Long.MAX_VALUE / 2;

    private final String name;
    private final List<String> shardNames;
    private final int[] partitionIds;
    private final NodeEngine nodeEngine;
    private final AtomicLongArray sizeHints;
    private final AtomicInteger rotation = new AtomicInteger();
    private final Map<UUID, List<UUID>> listenerRegistrations = new ConcurrentHashMap<>();
    private volatile long lastUsed;

    ShardedQueue(String name, List<String> shardNames, NodeEngine nodeEngine) {
        this.name = name;
        this.shardNames = Collections.unmodifiableList(new ArrayList<>(shardNames));
        this.nodeEngine = nodeEngine;
        this.partitionIds = new int[shardNames.size()];
        IPartitionService partitionService = nodeEngine.getPartitionService();
        for (int i = 0; i < partitionIds.length; i++) {
            partitionIds[i] = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(shardNames.get(i)));
        }
        this.sizeHints = new AtomicLongArray(shardNames.size());
    }

    /**
     * The shard names of a logical queue: the configured names, or {@code <name>@<key>} with keys
     * that put the shards on partitions spread evenly over the partition range.
     */
    static List<String> shardNames(String name, QueueConfig config, IPartitionService partitionService) {
        if (!config.getShardNames().isEmpty()) {
            return config.getShardNames();
        }
        int shards = config.getShardCount();
        int partitionCount = partitionService.getPartitionCount();
        int first = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
        List<String> names = new ArrayList<>(shards);
        for (int i = 0; i < shards; i++) {
            int target = (int) ((first + (long) i * partitionCount / shards) % partitionCount);
            for (int k = 0; ; k++) {
                String key = name + "-shard-" + i + "-" + k;
                if (partitionService.getPartitionId(key) == target) {
                    names.add(name + "@" + key);
                    break;
                }
            }
        }
        return names;
    }

    public String getName() {
        return name;
    }

    public List<String> getShardNames() {
        return shardNames;
    }

    public int shardCount() {
        return shardNames.size();
    }

    /** Offers an item, waiting up to {@code timeoutMillis} for room ({@code -1}: without a limit). */
    public CompletableFuture<Boolean> offer(Data item, long timeoutMillis) {
        touch();
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        long deadline = deadline(timeoutMillis);
        offerRound(item, byHint(true), 0, deadline, result);
        return result;
    }

    /** Polls an item, waiting up to {@code timeoutMillis} for one ({@code -1}: without a limit); null if none. */
    public CompletableFuture<Data> poll(long timeoutMillis) {
        touch();
        CompletableFuture<Data> result = new CompletableFuture<>();
        long deadline = deadline(timeoutMillis);
        pollRound(byHint(false), 0, deadline, result);
        return result;
    }

    /** The head of the fullest shard that has one, or null. */
    public CompletableFuture<Data> peek() {
        touch();
        CompletableFuture<Data> result = new CompletableFuture<>();
        peekRound(byHint(false), 0, result);
        return result;
    }

    /** The number of items in all shards. */
    public CompletableFuture<Integer> size() {
        touch();
        List<CompletableFuture<Long>> sizes = new ArrayList<>(shardCount());
        for (int i = 0; i < shardCount(); i++) {
            int shard = i;
            sizes.add(invoke(new SizeOperation(shardNames.get(i)), i).thenApply(size -> {
                long items = number(size);
                sizeHints.set(shard, items);
                return items;
            }));
        }
        return CompletableFuture.allOf(sizes.toArray(new CompletableFuture[0])).thenApply(ignored -> {
            long total = 0;
            for (CompletableFuture<Long> size : sizes) {
                total += (Long) size.join();
            }
            return (int) Math.min(total, Integer.MAX_VALUE);
        });
    }

    /** Whether every item is in some shard. */
    public boolean containsAll(Collection<Data> items) {
        touch();
        for (Data item : items) {
            boolean found = false;
            for (int i = 0; i < shardCount() && !found; i++) {
                found = isTrue(join(invoke(new ContainsOperation(shardNames.get(i), List.of(item)), i)));
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    /** Removes one occurrence of {@code item} from the first shard that has it. */
    public boolean remove(Data item) {
        touch();
        for (int i = 0; i < shardCount(); i++) {
            if (isTrue(join(invoke(new RemoveOperation(shardNames.get(i), item), i)))) {
                return true;
            }
        }
        return false;
    }

    /** Removes and returns up to {@code maxSize} items ({@code -1}: all), shard by shard. */
    public List<Data> drain(int maxSize) {
        touch();
        List<Data> drained = new ArrayList<>();
        for (int i = 0; i < shardCount(); i++) {
            int remaining = maxSize < 0 ? -1 : maxSize - drained.size();
            if (remaining == 0) {
                break;
            }
            SerializableList items = (SerializableList) object(join(invoke(new DrainOperation(shardNames.get(i), remaining), i)));
            for (Object item : items.getCollection()) {
                drained.add(item(item));
            }
        }
        return drained;
    }

    /** The items of all shards, shard by shard. */
    public List<Data> items() {
        touch();
        List<Data> all = new ArrayList<>();
        for (int i = 0; i < shardCount(); i++) {
            SerializableList items = (SerializableList) object(join(invoke(new IteratorOperation(shardNames.get(i)), i)));
            for (Object item : items.getCollection()) {
                all.add(item(item));
            }
        }
        return all;
    }

    /**
     * Adds the items, spread over the shards from the emptiest; true if every shard took its part.
     * Not atomic across shards.
     */
    public boolean addAll(List<Data> items) {
        touch();
        int[] order = byHint(true);
        List<List<Data>> parts = new ArrayList<>(shardCount());
        for (int i = 0; i < shardCount(); i++) {
            parts.add(new ArrayList<>());
        }
        for (int i = 0; i < items.size(); i++) {
            parts.get(order[i % order.length]).add(items.get(i));
        }
        boolean all = true;
        for (int i = 0; i < shardCount(); i++) {
            if (!parts.get(i).isEmpty()) {
                all &= isTrue(join(invoke(new AddAllOperation(shardNames.get(i), parts.get(i)), i)));
            }
        }
        return all;
    }

    /** Removes the items ({@code retain}: all other items) from every shard; true if any shard changed. */
    public boolean compareAndRemove(List<Data> items, boolean retain) {
        touch();
        boolean changed = false;
        for (int i = 0; i < shardCount(); i++) {
            changed |= isTrue(join(invoke(new CompareAndRemoveOperation(shardNames.get(i), items, retain), i)));
        }
        return changed;
    }

    public void clear() {
        touch();
        for (int i = 0; i < shardCount(); i++) {
            join(invoke(new ClearOperation(shardNames.get(i)), i));
        }
    }

    /** The room left in all shards together. */
    public int remainingCapacity() {
        long remaining = 0;
        for (int i = 0; i < shardCount(); i++) {
            remaining += number(join(invoke(new RemainingCapacityOperation(shardNames.get(i)), i)));
        }
        return (int) Math.min(remaining, Integer.MAX_VALUE);
    }

    /** Registers {@code listener} on every shard; the returned ID removes it from all of them. */
    public UUID addItemListener(QueueService service, ItemListener<?> listener, boolean includeValue) {
        List<UUID> registrations = new ArrayList<>(shardCount());
        for (String shardName : shardNames) {
            registrations.add(service.addItemListener(shardName, listener, includeValue));
        }
        UUID id = UuidUtil.newUnsecureUUID();
        listenerRegistrations.put(id, registrations);
        return id;
    }

    public boolean removeItemListener(QueueService service, UUID registrationId) {
        List<UUID> registrations = listenerRegistrations.remove(registrationId);
        if (registrations == null) {
            return false;
        }
        for (int i = 0; i < registrations.size(); i++) {
            service.removeItemListener(shardNames.get(i), registrations.get(i));
        }
        return true;
    }

    /** Refreshes the size hints, if the queue has been used recently; called every {@value #REFRESH_MILLIS} ms. */
    void refreshSizeHints() {
        if (System.currentTimeMillis() - lastUsed < IDLE_MILLIS) {
            for (int i = 0; i < shardCount(); i++) {
                int shard = i;
                invoke(new SizeOperation(shardNames.get(i)), i).thenAccept(size -> {
                    if (sizeHints.get(shard) != FULL) {
                        sizeHints.set(shard, number(size));
                    }
                });
            }
        }
    }

    private void offerRound(Data item, int[] order, int index, long deadline, CompletableFuture<Boolean> result) {
        if (result.isDone()) {
            // cancelled, for example by an interrupted caller
            return;
        }
        if (index < order.length) {
            int shard = order[index];
            InternalCompletableFuture<Object> offer = invoke(new OfferOperation(shardNames.get(shard), 0, item), shard);
            offer.whenCompleteAsync((offered, t) -> guarded(result, () -> {
                if (t != null) {
                    result.completeExceptionally(t);
                } else if (isTrue(offered)) {
                    sizeHints.getAndUpdate(shard, size -> size == FULL ? 1 : size + 1);
                    result.complete(true);
                } else {
                    sizeHints.set(shard, FULL);
                    offerRound(item, order, index + 1, deadline, result);
                }
            }), ConcurrencyUtil.getDefaultAsyncExecutor());
            return;
        }
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
            result.complete(false);
            return;
        }
        // every shard is full: wait on one of them, then look at all of them again
        int shard = Math.floorMod(rotation.getAndIncrement(), shardCount());
        long wait = Math.min(remaining, WAIT_SLICE_MILLIS);
        InternalCompletableFuture<Object> offer = invoke(new OfferOperation(shardNames.get(shard), wait, item), shard);
        offer.whenCompleteAsync((offered, t) -> guarded(result, () -> {
            if (t != null) {
                result.completeExceptionally(t);
            } else if (isTrue(offered)) {
                sizeHints.set(shard, 1);
                result.complete(true);
            } else {
                offerRound(item, byHint(true), 0, deadline, result);
            }
        }), ConcurrencyUtil.getDefaultAsyncExecutor());
    }

    private void pollRound(int[] order, int index, long deadline, CompletableFuture<Data> result) {
        if (result.isDone()) {
            // cancelled, for example by an interrupted caller
            return;
        }
        if (index < order.length) {
            int shard = order[index];
            invoke(new PollOperation(shardNames.get(shard), 0), shard).whenCompleteAsync((item, t) -> guarded(result, () -> {
                if (t != null) {
                    result.completeExceptionally(t);
                } else if (item != null) {
                    sizeHints.getAndUpdate(shard, size -> size == FULL ? size : Math.max(0, size - 1));
                    deliver(shard, item(item), result);
                } else {
                    sizeHints.set(shard, 0);
                    pollRound(order, index + 1, deadline, result);
                }
            }), ConcurrencyUtil.getDefaultAsyncExecutor());
            return;
        }
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
            result.complete(null);
            return;
        }
        // every shard is empty: wait on one of them, then look at all of them again
        int shard = Math.floorMod(rotation.getAndIncrement(), shardCount());
        long wait = Math.min(remaining, WAIT_SLICE_MILLIS);
        invoke(new PollOperation(shardNames.get(shard), wait), shard).whenCompleteAsync((item, t) -> guarded(result, () -> {
            if (t != null) {
                result.completeExceptionally(t);
            } else if (item != null) {
                deliver(shard, item(item), result);
            } else {
                pollRound(byHint(false), 0, deadline, result);
            }
        }), ConcurrencyUtil.getDefaultAsyncExecutor());
    }

    /** Hands a polled item to the caller, or back to its shard if the caller has gone. */
    private void deliver(int shard, Data item, CompletableFuture<Data> result) {
        if (!result.complete(item)) {
            invoke(new OfferOperation(shardNames.get(shard), -1, item), shard);
        }
    }

    private void peekRound(int[] order, int index, CompletableFuture<Data> result) {
        if (index == order.length) {
            result.complete(null);
            return;
        }
        int shard = order[index];
        invoke(new PeekOperation(shardNames.get(shard)), shard).whenCompleteAsync((item, t) -> guarded(result, () -> {
            if (t != null) {
                result.completeExceptionally(t);
            } else if (item != null) {
                result.complete(item(item));
            } else {
                peekRound(order, index + 1, result);
            }
        }), ConcurrencyUtil.getDefaultAsyncExecutor());
    }

    /** All shards, the emptiest first for offers or the fullest first for polls; ties rotate. */
    private int[] byHint(boolean emptiestFirst) {
        int n = shardCount();
        int start = rotation.getAndIncrement();
        Integer[] order = new Integer[n];
        long[] hints = new long[n];
        for (int i = 0; i < n; i++) {
            order[i] = Math.floorMod(start + i, n);
            hints[i] = sizeHints.get(i);
        }
        Arrays.sort(order, (a, b) -> emptiestFirst ? Long.compare(hints[a], hints[b])
                : Long.compare(hints[b] == FULL ? -1 : hints[b], hints[a] == FULL ? -1 : hints[a]));
        int[] result = new int[n];
        for (int i = 0; i < n; i++) {
            result[i] = order[i];
        }
        return result;
    }

    private static Object join(InternalCompletableFuture<Object> future) {
        return future.joinInternal();
    }

    /** Invokes {@code operation} on the shard's partition; items come back as {@link Data}, other results as objects. */
    private InternalCompletableFuture<Object> invoke(Operation operation, int shard) {
        return nodeEngine.getOperationService()
                .createInvocationBuilder(QueueService.SERVICE_NAME, operation, partitionIds[shard])
                .setResultDeserialized(false)
                .invoke();
    }

    private Object object(Object result) {
        return result instanceof Data data ? nodeEngine.toObject(data) : result;
    }

    private Data item(Object result) {
        return result == null || result instanceof Data ? (Data) result : nodeEngine.toData(result);
    }

    private boolean isTrue(Object result) {
        return Boolean.TRUE.equals(object(result));
    }

    private long number(Object result) {
        return ((Number) object(result)).longValue();
    }

    /** Runs a callback of a round, completing {@code result} with the callback's failure, if any. */
    private static void guarded(CompletableFuture<?> result, Runnable callback) {
        try {
            callback.run();
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }
    }

    private void touch() {
        lastUsed = System.currentTimeMillis();
    }

    private static long deadline(long timeoutMillis) {
        if (timeoutMillis < 0) {
            return Long.MAX_VALUE;
        }
        long now = System.currentTimeMillis();
        return now + timeoutMillis < now ? Long.MAX_VALUE : now + timeoutMillis;
    }

    @Override
    public String toString() {
        return "ShardedQueue{name=" + name + ", shards=" + shardNames + '}';
    }
}
