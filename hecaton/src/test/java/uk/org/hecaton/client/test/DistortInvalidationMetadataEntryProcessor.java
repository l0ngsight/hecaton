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

package uk.org.hecaton.client.test;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.core.Offloadable;
import uk.org.hecaton.internal.nearcache.impl.invalidation.Invalidator;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataGenerator;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.nearcache.MapNearCacheManager;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.internal.util.UuidUtil;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.internal.util.RandomPicker.getInt;
import static java.lang.Integer.MAX_VALUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class DistortInvalidationMetadataEntryProcessor
        implements EntryProcessor<Integer, Integer, Object>, IdentifiedDataSerializable, HecatonInstanceAware, Offloadable {

    static final int CLASS_ID = 3;

    private int mapSize;
    private String mapName;
    private int duration;
    private HecatonInstance instance;

    public DistortInvalidationMetadataEntryProcessor() {
    }

    @Override
    public Object process(Map.Entry<Integer, Integer> entry) {
        final HecatonInstance instance = this.instance;
        final AtomicBoolean stopTest = new AtomicBoolean();

        Thread distortSequence = new Thread(() -> {
            while (!stopTest.get()) {
                distortRandomPartitionSequence(mapName, instance);
                sleepSeconds(1);
            }
        });

        Thread distortUuid = new Thread(() -> {
            while (!stopTest.get()) {
                distortRandomPartitionUuid(instance);
                sleepSeconds(5);
            }
        });

        Thread put = new Thread(() -> {
            // change some data
            while (!stopTest.get()) {
                try {
                    int key = getInt(mapSize);
                    int value = getInt(Integer.MAX_VALUE);
                    Map<Integer, Integer> map = instance.getMap(mapName);
                    int oldValue = map.put(key, value);
                    sleepAtLeastMillis(100);
                } catch (HecatonInstanceNotActiveException e) {
                    break;
                }
            }
        });

        put.start();
        distortSequence.start();
        distortUuid.start();

        sleepSeconds(duration);

        // stop threads
        stopTest.set(true);
        try {
            distortUuid.join();
            distortSequence.join();
            put.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < mapSize; i++) {
            System.out.println(instance.getMap(mapName).get(i));
        }
        return null;
    }

    private void distortRandomPartitionSequence(String mapName, HecatonInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        MetaDataGenerator metaDataGenerator = invalidator.getMetaDataGenerator();
        InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        metaDataGenerator.setCurrentSequence(mapName, getInt(partitionCount), getInt(MAX_VALUE));
    }

    private void distortRandomPartitionUuid(HecatonInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        int partitionCount = nodeEngineImpl.getPartitionService().getPartitionCount();
        int partitionId = getInt(partitionCount);
        MapService mapService = nodeEngineImpl.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        Invalidator invalidator = mapNearCacheManager.getInvalidator();
        MetaDataGenerator metaDataGenerator = invalidator.getMetaDataGenerator();

        metaDataGenerator.setUuid(partitionId, UuidUtil.newUnsecureUUID());
    }

    private void sleepSeconds(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sleepAtLeastMillis(int sleepFor) {
        boolean interrupted = false;
        try {
            long remainingNanos = MILLISECONDS.toNanos(sleepFor);
            long sleepUntil = System.nanoTime() + remainingNanos;
            while (remainingNanos > 0) {
                try {
                    NANOSECONDS.sleep(remainingNanos);
                } catch (InterruptedException e) {
                    interrupted = true;
                } finally {
                    remainingNanos = sleepUntil - System.nanoTime();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public int getFactoryId() {
        return IdentifiedFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        out.writeInt(mapSize);
        out.writeInt(duration);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readString();
        mapSize = in.readInt();
        duration = in.readInt();
    }

    @Override
    public void setHecatonInstance(HecatonInstance hecatonInstance) {
        this.instance = hecatonInstance;
    }

    @Override
    public String getExecutorName() {
        return OFFLOADABLE_EXECUTOR;
    }
}
