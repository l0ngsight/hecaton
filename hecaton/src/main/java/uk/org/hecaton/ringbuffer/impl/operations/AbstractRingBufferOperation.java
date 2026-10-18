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

package uk.org.hecaton.ringbuffer.impl.operations;

import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.internal.monitor.impl.LocalTopicStatsImpl;
import uk.org.hecaton.internal.namespace.impl.NodeEngineThreadLocalContext;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.internal.services.ServiceNamespaceAware;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.ringbuffer.StaleSequenceException;
import uk.org.hecaton.ringbuffer.impl.RingbufferContainer;
import uk.org.hecaton.ringbuffer.impl.RingbufferService;
import uk.org.hecaton.ringbuffer.impl.RingbufferWaitNotifyKey;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.NamedOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.WaitNotifyKey;
import uk.org.hecaton.topic.impl.reliable.ReliableTopicService;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static uk.org.hecaton.ringbuffer.impl.RingbufferDataSerializerHook.F_ID;
import static uk.org.hecaton.ringbuffer.impl.RingbufferService.SERVICE_NAME;

/**
 * Common logic for all ring buffer operations :
 * <ul>
 * <li>getting the ring buffer container or creating a new one if necessary</li>
 * <li>serialization/deserialization of ring buffer name</li>
 * <li>defines the factory ID for the {@link IdentifiedDataSerializable}</li>
 * </ul>
 */
public abstract class AbstractRingBufferOperation extends Operation implements NamedOperation, IdentifiedDataSerializable,
        PartitionAwareOperation, ServiceNamespaceAware {

    protected String name;

    public AbstractRingBufferOperation() {
    }

    public AbstractRingBufferOperation(String name) {
        this.name = name;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns an {@link RingbufferContainer} or creates a new one if necessary by calling
     * {@link RingbufferService#getOrCreateContainer(int, ObjectNamespace, RingbufferConfig)}.
     * Also calls the {@link RingbufferContainer#cleanup()} before returning
     * the container. This will currently remove any expired items.
     *
     * @return the ringbuffer container
     */
    RingbufferContainer getRingBufferContainer() {
        final RingbufferService service = getService();
        final ObjectNamespace ns = RingbufferService.getRingbufferNamespace(name);

        RingbufferContainer ringbuffer = service.getContainerOrNull(getPartitionId(), ns);
        if (ringbuffer == null) {
            ringbuffer = service.getOrCreateContainer(getPartitionId(), ns, service.getRingbufferConfig(name));
        }

        ringbuffer.cleanup();
        return ringbuffer;
    }

    /**
     * Returns an {@link RingbufferContainer} or null if one doesn't exist.
     * <p>
     * If it does it also calls the {@link RingbufferContainer#cleanup()} before returning
     * the container. This will currently remove any expired items.
     *
     * @return the ringbuffer container
     */
    RingbufferContainer getRingBufferContainerOrNull() {
        final RingbufferService service = getService();
        final ObjectNamespace ns = RingbufferService.getRingbufferNamespace(name);

        RingbufferContainer ringbuffer = service.getContainerOrNull(getPartitionId(), ns);
        if (ringbuffer != null) {
            ringbuffer.cleanup();
        }

        return ringbuffer;
    }

    /**
     * Returns {@link WaitNotifyKey} of the ringbuffer.
     *
     * If the RingbufferContainer exists it reuses it's {@link RingbufferContainer#getRingEmptyWaitNotifyKey()}.
     * If the RingbufferContainer doesn't exist it creates new RingbufferWaitNotifyKey and doesn't recreate
     * the ringbuffer container.
     *
     * @return WaitNotifyKey of the ringbuffer
     */
    WaitNotifyKey getRingbufferWaitNotifyKey() {
        final RingbufferService service = getService();
        final ObjectNamespace ns = RingbufferService.getRingbufferNamespace(name);
        RingbufferContainer ringbuffer = service.getContainerOrNull(getPartitionId(), ns);
        if (ringbuffer != null) {
            return ringbuffer.getRingEmptyWaitNotifyKey();
        } else {
            return new RingbufferWaitNotifyKey(ns, getPartitionId());
        }
    }

    /**
     * Returns the User Code Namespace used for this RingBuffer
     *
     * @return the configured {@code Namespace} name
     */
    public String getUserCodeNamespace() {
        // Obtain NodeEngine reference and set for use later in operations
        NodeEngine engine = NodeEngineThreadLocalContext.getNodeEngineThreadLocalContext();
        setNodeEngine(engine);
        return RingbufferService.lookupUserCodeNamespace(engine, name, getPartitionId());
    }

    @Override
    public void logError(Throwable e) {
        if (e instanceof StaleSequenceException) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest(e.getMessage(), e);
            } else if (logger.isFineEnabled()) {
                logger.fine(e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        } else {
            super.logError(e);
        }
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readString();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(name);
    }

    @Override
    public ObjectNamespace getServiceNamespace() {
        return getRingBufferContainer().getNamespace();
    }

    /**
     * ReliableTopic is built on top of RingBuffer. This method determines if 'publish' operation
     * is actually called on ReliableTopic and reports the statistics to {@link LocalTopicStatsImpl}.
     */
    protected void reportReliableTopicPublish(int publishCount) {
        reportReliableTopicStat(publishCount, (topic) ->
                getReliableTopicService().getLocalTopicStats(topic).incrementPublishes());
    }

    /**
     * ReliableTopic is built on top of RingBuffer. This method determines if 'read' operation
     * is actually called on ReliableTopic and reports the statistics to {@link LocalTopicStatsImpl}.
     */
    protected void reportReliableTopicReceived(int receivedCount) {
        reportReliableTopicStat(receivedCount, (topic) ->
                getReliableTopicService().getLocalTopicStats(topic).incrementReceives());
    }

    private void reportReliableTopicStat(int count, Consumer<String> statsReporter) {
        if (name.startsWith(RingbufferService.TOPIC_RB_PREFIX)) {
            String reliableTopicName = name.substring(RingbufferService.TOPIC_RB_PREFIX.length());
            IntStream.range(0, count).forEach((i) -> statsReporter.accept(reliableTopicName));
        }
    }

    private ReliableTopicService getReliableTopicService() {
        return getNodeEngine().getService(ReliableTopicService.SERVICE_NAME);
    }
}
