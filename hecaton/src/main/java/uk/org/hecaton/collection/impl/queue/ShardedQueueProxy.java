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

import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ItemEvent;
import uk.org.hecaton.collection.ItemListener;
import uk.org.hecaton.collection.LocalQueueStats;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.AbstractDistributedObject;
import uk.org.hecaton.spi.impl.NodeEngine;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

/**
 * The member-side proxy of a sharded queue (load protection, F23): one {@link IQueue} over the
 * queue's shards, through {@link ShardedQueue}.
 * <p>
 * Differences from a single queue: items keep their order within a shard but only roughly across
 * shards; {@code maxSize} applies to each shard; {@code addAll}, {@code drainTo} and the bulk
 * removals are not atomic across shards; item events carry the logical queue's name.
 *
 * @param <E> the type of the items
 */
public class ShardedQueueProxy<E> extends AbstractDistributedObject<QueueService> implements IQueue<E> {

    private final String name;
    private final ShardedQueue queue;

    ShardedQueueProxy(String name, ShardedQueue queue, QueueService service, NodeEngine nodeEngine) {
        super(nodeEngine, service);
        this.name = name;
        this.queue = queue;
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    /** The logical queue that this proxy reads and writes. */
    public ShardedQueue getShardedQueue() {
        return queue;
    }

    @Override
    public boolean add(@Nonnull E e) {
        if (offer(e)) {
            return true;
        }
        throw new IllegalStateException("Queue is full!");
    }

    @Override
    public boolean offer(@Nonnull E e) {
        return join(queue.offer(toData(checkNotNull(e, "Null item is not allowed!")), 0));
    }

    @Override
    public void put(@Nonnull E e) throws InterruptedException {
        joinInterruptibly(queue.offer(toData(checkNotNull(e, "Null item is not allowed!")), -1));
    }

    @Override
    public boolean offer(@Nonnull E e, long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        return joinInterruptibly(queue.offer(toData(checkNotNull(e, "Null item is not allowed!")), unit.toMillis(timeout)));
    }

    @Nonnull
    @Override
    public E take() throws InterruptedException {
        return toObject(joinInterruptibly(queue.poll(-1)));
    }

    @Override
    public E poll(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        return toObject(joinInterruptibly(queue.poll(unit.toMillis(timeout))));
    }

    @Override
    public E poll() {
        return toObject(join(queue.poll(0)));
    }

    @Override
    public E remove() {
        E item = poll();
        if (item == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return item;
    }

    @Override
    public E peek() {
        return toObject(join(queue.peek()));
    }

    @Override
    public E element() {
        E item = peek();
        if (item == null) {
            throw new NoSuchElementException("Queue is empty!");
        }
        return item;
    }

    @Override
    public int size() {
        return join(queue.size());
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    @Override
    public void clear() {
        queue.clear();
    }

    @Override
    public boolean remove(@Nonnull Object o) {
        return queue.remove(toData(checkNotNull(o, "Null item is not allowed!")));
    }

    @Override
    public boolean contains(@Nonnull Object o) {
        return queue.containsAll(List.of(toData(checkNotNull(o, "Null item is not allowed!"))));
    }

    @Override
    public boolean containsAll(@Nonnull Collection<?> objects) {
        return queue.containsAll(toDataList(objects));
    }

    @Override
    public int drainTo(@Nonnull Collection<? super E> objects) {
        return drainTo(objects, -1);
    }

    @Override
    public int drainTo(@Nonnull Collection<? super E> objects, int maxElements) {
        List<Data> drained = queue.drain(maxElements);
        for (Data item : drained) {
            objects.add(toObject(item));
        }
        return drained.size();
    }

    @Override
    public boolean addAll(@Nonnull Collection<? extends E> items) {
        return queue.addAll(toDataList(items));
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> objects) {
        return queue.compareAndRemove(toDataList(objects), false);
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> objects) {
        return queue.compareAndRemove(toDataList(objects), true);
    }

    @Nonnull
    @Override
    public Iterator<E> iterator() {
        List<E> items = new ArrayList<>();
        for (Data item : queue.items()) {
            items.add(toObject(item));
        }
        return items.iterator();
    }

    @Nonnull
    @Override
    public Object[] toArray() {
        List<E> items = new ArrayList<>();
        iterator().forEachRemaining(items::add);
        return items.toArray();
    }

    @Nonnull
    @Override
    public <T> T[] toArray(@Nonnull T[] array) {
        List<E> items = new ArrayList<>();
        iterator().forEachRemaining(items::add);
        return items.toArray(array);
    }

    @Nonnull
    @Override
    public UUID addItemListener(@Nonnull ItemListener<E> listener, boolean includeValue) {
        checkNotNull(listener, "Null listener is not allowed!");
        ItemListener<E> renaming = new ItemListener<>() {
            @Override
            public void itemAdded(ItemEvent<E> event) {
                listener.itemAdded(new ItemEvent<>(name, event.getEventType(), event.getItem(), event.getMember()));
            }

            @Override
            public void itemRemoved(ItemEvent<E> event) {
                listener.itemRemoved(new ItemEvent<>(name, event.getEventType(), event.getItem(), event.getMember()));
            }
        };
        return queue.addItemListener(getService(), renaming, includeValue);
    }

    @Override
    public boolean removeItemListener(@Nonnull UUID registrationId) {
        return queue.removeItemListener(getService(), checkNotNull(registrationId, "Null registrationId is not allowed!"));
    }

    @Override
    public LocalQueueStats getLocalQueueStats() {
        List<LocalQueueStats> stats = new ArrayList<>();
        for (String shardName : queue.getShardNames()) {
            stats.add(getService().createLocalQueueStats(shardName));
        }
        return new ShardedLocalQueueStats(stats);
    }

    @Override
    protected void postDestroy() {
        for (String shardName : queue.getShardNames()) {
            getNodeEngine().getProxyService().destroyDistributedObject(QueueService.SERVICE_NAME, shardName,
                    getNodeEngine().getLocalMember().getUuid());
        }
        getService().forgetShardedQueue(name);
    }

    @Override
    public String toString() {
        return "IQueue{name='" + name + "', shards=" + queue.getShardNames() + '}';
    }

    private List<Data> toDataList(Collection<?> objects) {
        List<Data> items = new ArrayList<>(objects.size());
        for (Object o : objects) {
            items.add(toData(checkNotNull(o, "Null item is not allowed!")));
        }
        return items;
    }

    private E toObject(Data data) {
        return getNodeEngine().toObject(data);
    }

    private static <T> T join(CompletableFuture<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            future.cancel(false);
            Thread.currentThread().interrupt();
            throw rethrow(e);
        } catch (ExecutionException e) {
            throw rethrow(e.getCause());
        }
    }

    private static <T> T joinInterruptibly(CompletableFuture<T> future) throws InterruptedException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            // stops the waiting; an item that arrives later goes back to its shard
            future.cancel(false);
            throw e;
        } catch (ExecutionException e) {
            throw rethrow(e.getCause());
        }
    }
}
