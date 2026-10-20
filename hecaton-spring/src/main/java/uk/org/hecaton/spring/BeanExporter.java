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
package uk.org.hecaton.spring;

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.cardinality.CardinalityEstimator;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.cp.CPMap;
import uk.org.hecaton.cp.ISemaphore;
import uk.org.hecaton.cp.lock.FencedLock;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.durableexecutor.DurableExecutorService;
import uk.org.hecaton.flakeidgen.FlakeIdGenerator;
import uk.org.hecaton.jet.JetService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.multimap.MultiMap;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.scheduledexecutor.IScheduledExecutorService;
import uk.org.hecaton.sql.SqlService;
import uk.org.hecaton.topic.ITopic;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;

import javax.annotation.Nonnull;

final class BeanExporter
        implements BeanDefinitionRegistryPostProcessor {

    private static final ILogger LOGGER = Logger.getLogger(BeanExporter.class);
    private final HecatonInstance hecatonInstance;
    private final ExposeHecatonObjects.Configuration exposeConf;

    BeanExporter(HecatonInstance hecatonInstance, ExposeHecatonObjects.Configuration exposeConf) {
        this.hecatonInstance = hecatonInstance;
        this.exposeConf = exposeConf;
    }

    @Override
    public void postProcessBeanDefinitionRegistry(@Nonnull BeanDefinitionRegistry registry)
            throws BeansException {

        final Config hecatonConfiguration = hecatonInstance.getConfig();

        if (LOGGER.isFineEnabled()) {
            LOGGER.fine("Exposing Hecaton objects, using configuration: %s", exposeConf);
        }

        var reg = new HecatonObjectExtractionConfiguration.RegistrationChain(hecatonInstance, registry, exposeConf)
                .register(registry, "jetService", JetService.class, (hz, n) -> hz.getJet())
                .register(registry, "sqlService", SqlService.class, (hz, n) -> hz.getSql())

                .register(hecatonConfiguration.getMapConfigs(), IMap.class, HecatonInstance::getMap)
                .register(hecatonConfiguration.getQueueConfigs(), IQueue.class, HecatonInstance::getQueue)
                .register(hecatonConfiguration.getFlakeIdGeneratorConfigs(), FlakeIdGenerator.class,
                          HecatonInstance::getFlakeIdGenerator)
                .register(hecatonConfiguration.getMultiMapConfigs(), MultiMap.class, HecatonInstance::getMultiMap)
                .register(hecatonConfiguration.getPNCounterConfigs(), PNCounter.class, HecatonInstance::getPNCounter)
                .register(hecatonConfiguration.getReliableTopicConfigs(), ITopic.class, HecatonInstance::getReliableTopic)
                .register(hecatonConfiguration.getReplicatedMapConfigs(), ReplicatedMap.class,
                          HecatonInstance::getReplicatedMap)
                .register(hecatonConfiguration.getRingbufferConfigs(), Ringbuffer.class, HecatonInstance::getRingbuffer)
                .register(hecatonConfiguration.getSetConfigs(), ISet.class, HecatonInstance::getSet)
                .register(hecatonConfiguration.getTopicConfigs(), ITopic.class, HecatonInstance::getTopic)
                .register(hecatonConfiguration.getScheduledExecutorConfigs(), IScheduledExecutorService.class,
                          HecatonInstance::getScheduledExecutorService)
                .register(hecatonConfiguration.getDurableExecutorConfigs(), DurableExecutorService.class,
                          HecatonInstance::getDurableExecutorService)
                .register(hecatonConfiguration.getExecutorConfigs(), IExecutorService.class,
                          HecatonInstance::getExecutorService)
                .register(hecatonConfiguration.getListConfigs(), IList.class,
                          HecatonInstance::getList)
                .register(hecatonConfiguration.getCardinalityEstimatorConfigs(), CardinalityEstimator.class,
                          HecatonInstance::getCardinalityEstimator)

                .register(hecatonConfiguration.getCPSubsystemConfig().getCpMapConfigs(), CPMap.class,
                          (i, n) -> i.getCPSubsystem().getMap(n))
                .register(hecatonConfiguration.getCPSubsystemConfig().getLockConfigs(), FencedLock.class,
                          (i, n) -> i.getCPSubsystem().getLock(n))
                .register(hecatonConfiguration.getCPSubsystemConfig().getSemaphoreConfigs(), ISemaphore.class,
                          (i, n) -> i.getCPSubsystem().getSemaphore(n));

        if (cachePresent()) {
            reg.register(hecatonConfiguration.getCacheConfigs(), ICache.class,
                         (instance, name) -> instance.getCacheManager().getCache(name));
        }
    }

    private boolean cachePresent() {
        final String cacheClassName = "javax.cache.Cache";
        try {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            Class<?> aClass = contextClassLoader.loadClass(cacheClassName);
            assert aClass != null;
            return true;
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            LOGGER.fine("Javax Cache class '%s' not found, skipping adding ICache beans", cacheClassName);
            return false;
        }
    }

    @Override
    public void postProcessBeanFactory(@Nonnull ConfigurableListableBeanFactory configurableListableBeanFactory)
            throws BeansException {
        // Required for Spring 5 compatibility.
        // Do not remove this override while Spring 5 compatibility is still supported.
    }
}
