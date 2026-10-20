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

package uk.org.hecaton.topic.impl;

import uk.org.hecaton.config.TopicConfig;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.spi.impl.operationservice.AbstractNamedOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.locks.Lock;

/**
 * ITopic publication operation used when global ordering is enabled
 * (all nodes listening to the same topic get their messages in the same order).
 *
 * @see TotalOrderedTopicProxy
 * @see TopicConfig#isGlobalOrderingEnabled()
 */
public class PublishOperation extends AbstractNamedOperation
        implements IdentifiedDataSerializable {

    private Data message;

    public PublishOperation() {
    }

    public PublishOperation(String name, Data message) {
        super(name);
        this.message = message;
    }

    /**
     * {@inheritDoc}
     * Increments the local statistics for the number of published
     * messages.
     *
     * @throws Exception
     */
    @Override
    public void afterRun() throws Exception {
        TopicService service = getService();
        service.incrementPublishes(name);
    }

    @Override
    public void run() throws Exception {
        TopicService service = getService();
        TopicEvent topicEvent = new TopicEvent(name, message, getCallerAddress());
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(TopicService.SERVICE_NAME, name);

        Lock lock = service.getOrderLock(name);
        lock.lock();
        try {
            eventService.publishEvent(TopicService.SERVICE_NAME, registrations, topicEvent, name.hashCode());
        } finally {
            lock.unlock();
        }
    }


    @Override
    public int getFactoryId() {
        return TopicDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return TopicDataSerializerHook.PUBLISH;
    }

    @Override
    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, message);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        message = IOUtil.readData(in);
    }
}
