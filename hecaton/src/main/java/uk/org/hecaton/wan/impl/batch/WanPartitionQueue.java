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
package uk.org.hecaton.wan.impl.batch;

import uk.org.hecaton.internal.services.DistributedObjectNamespace;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.wan.impl.InternalWanEvent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The WAN events of one partition, in the order they happened: the events to send when the member
 * owns the partition, a copy of the owner's queue when it holds a backup. Thread-safe.
 */
final class WanPartitionQueue {

    private final ArrayDeque<InternalWanEvent<Object>> events = new ArrayDeque<>();
    /** The number of events, readable without the lock. */
    private volatile int size;

    static ServiceNamespace namespaceOf(InternalWanEvent<?> event) {
        return new DistributedObjectNamespace(event.getServiceName(), event.getObjectName());
    }

    synchronized void add(InternalWanEvent<Object> event) {
        events.addLast(event);
        size = events.size();
    }

    int size() {
        return size;
    }

    /** The first {@code max} events, which stay in the queue. */
    synchronized List<InternalWanEvent<Object>> peek(int max) {
        List<InternalWanEvent<Object>> first = new ArrayList<>(Math.min(max, events.size()));
        Iterator<InternalWanEvent<Object>> iterator = events.iterator();
        while (first.size() < max && iterator.hasNext()) {
            first.add(iterator.next());
        }
        return first;
    }

    /**
     * Removes the sent events from the head of the queue (they are still there unless the queue was
     * cleared or replaced meanwhile).
     *
     * @return per namespace, how many were removed
     */
    synchronized Map<ServiceNamespace, Integer> removeSent(List<InternalWanEvent<Object>> sent) {
        Map<ServiceNamespace, Integer> removed = new HashMap<>();
        for (InternalWanEvent<Object> event : sent) {
            if (events.peekFirst() != event) {
                break;
            }
            events.pollFirst();
            removed.merge(namespaceOf(event), 1, Integer::sum);
        }
        size = events.size();
        return removed;
    }

    /** Removes the first {@code count} events of each namespace (sent by the owner of the partition). */
    synchronized int removeFirst(Map<ServiceNamespace, Integer> counts) {
        Map<ServiceNamespace, Integer> remaining = new HashMap<>(counts);
        int removed = 0;
        Iterator<InternalWanEvent<Object>> iterator = events.iterator();
        while (iterator.hasNext() && !remaining.isEmpty()) {
            ServiceNamespace namespace = namespaceOf(iterator.next());
            Integer count = remaining.get(namespace);
            if (count != null) {
                iterator.remove();
                removed++;
                if (count == 1) {
                    remaining.remove(namespace);
                } else {
                    remaining.put(namespace, count - 1);
                }
            }
        }
        size = events.size();
        return removed;
    }

    /** Removes all events of a namespace. */
    synchronized int removeAll(ServiceNamespace namespace) {
        int before = events.size();
        events.removeIf(event -> namespaceOf(event).equals(namespace));
        size = events.size();
        return before - events.size();
    }

    /** @return how many events were removed */
    synchronized int clear() {
        int cleared = events.size();
        events.clear();
        size = 0;
        return cleared;
    }

    /** The events of the given namespaces, in order. */
    synchronized List<InternalWanEvent<Object>> events(Collection<ServiceNamespace> namespaces) {
        List<InternalWanEvent<Object>> selected = new ArrayList<>();
        for (InternalWanEvent<Object> event : events) {
            if (namespaces.contains(namespaceOf(event))) {
                selected.add(event);
            }
        }
        return selected;
    }

    /** Replaces the events of the given namespaces (replication from the partition's owner). */
    synchronized void replace(Collection<ServiceNamespace> namespaces, List<InternalWanEvent<Object>> replicated) {
        events.removeIf(event -> namespaces.contains(namespaceOf(event)));
        events.addAll(replicated);
        size = events.size();
    }

    synchronized void collectNamespaces(Set<ServiceNamespace> namespaces) {
        for (InternalWanEvent<Object> event : events) {
            namespaces.add(namespaceOf(event));
        }
    }
}
