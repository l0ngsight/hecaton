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

package uk.org.hecaton.test;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;

public final class ClusterFailureTestSupport {
    private ClusterFailureTestSupport() {
    }

    public abstract static class SingleFailingInstanceClusterFailure {
        protected HecatonInstance[] hecatonInstances;
        protected HecatonInstance failingInstance;
        protected TestHecatonFactory factory = new TestHecatonFactory();
        protected HecatonInstance client;
        protected int initialClusterMembersCount;
        protected Config config;

        public void initialize(int initialClusterMembersCount, Config config) {
            this.initialClusterMembersCount = initialClusterMembersCount;
            this.config = config;

            hecatonInstances = factory.newInstances(config, initialClusterMembersCount);
            assertClusterSizeEventually(initialClusterMembersCount, null);
            failingInstance = factory.newHecatonInstance(config);
            assertClusterSizeEventually(initialClusterMembersCount + 1, failingInstance);
            HecatonTestSupport.waitAllForSafeState(hecatonInstances);
            HecatonTestSupport.waitAllForSafeState(failingInstance);
            client = null;
        }

        public HecatonInstance createClient(ClientConfig clientConfig) {
            client = factory.newHecatonClient(clientConfig);
            return client;
        }

        public void cleanUp() {
            client.getLifecycleService().terminate();
            factory.terminateAll();
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }

        public abstract void fail();

        public abstract void recover();

        public HecatonInstance getFailingInstance() {
            return failingInstance;
        }

        public HecatonInstance getNotFailingInstance() {
            return hecatonInstances[0];
        }

        protected void assertClusterSizeEventually(int size, HecatonInstance additionalInstance) {
            for (int i = 0; i < hecatonInstances.length; i++) {
                HecatonInstance instance = hecatonInstances[i];
                HecatonTestSupport.assertClusterSizeEventually(size, instance);
            }
            if (additionalInstance != null) {
                HecatonTestSupport.assertClusterSizeEventually(size, additionalInstance);
            }
        }
    }

    public static class NetworkProblemClusterFailure extends SingleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            for (HecatonInstance hecatonInstance : hecatonInstances) {
                HecatonTestSupport.closeConnectionBetween(hecatonInstance, failingInstance);
            }
            assertClusterSizeEventually(initialClusterMembersCount, null);
            HecatonTestSupport.waitAllForSafeState(hecatonInstances);
        }

        @Override
        public void recover() {
            failingInstance.getLifecycleService().terminate();
            failingInstance = factory.newHecatonInstance(config);
            assertClusterSizeEventually(initialClusterMembersCount + 1, failingInstance);
            HecatonTestSupport.waitAllForSafeState(hecatonInstances);
            HecatonTestSupport.waitAllForSafeState(failingInstance);
        }
    }

    public static class NodeReplacementClusterFailure extends SingleFailingInstanceClusterFailure {
        private HecatonInstance replacementInstance;

        @Override
        public void fail() {
            failingInstance.getLifecycleService().terminate();
            assertClusterSizeEventually(initialClusterMembersCount, null);
            replacementInstance = factory.newHecatonInstance(config);
            assertClusterSizeEventually(initialClusterMembersCount + 1, replacementInstance);
            HecatonTestSupport.waitAllForSafeState(hecatonInstances);
            HecatonTestSupport.waitAllForSafeState(replacementInstance);
        }

        @Override
        public void recover() {
            failingInstance = replacementInstance;
            replacementInstance = null;
        }
    }

    public static class NodeTerminationClusterFailure extends SingleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            failingInstance.getLifecycleService().terminate();
            assertClusterSizeEventually(initialClusterMembersCount, null);
            HecatonTestSupport.waitAllForSafeState(hecatonInstances);
        }

        @Override
        public void recover() {
            failingInstance = factory.newHecatonInstance(config);
            assertClusterSizeEventually(initialClusterMembersCount + 1, failingInstance);
            HecatonTestSupport.waitAllForSafeState(hecatonInstances);
            HecatonTestSupport.waitAllForSafeState(failingInstance);
        }
    }

    public static class NodeShutdownClusterFailure extends SingleFailingInstanceClusterFailure {
        @Override
        public void fail() {
            failingInstance.shutdown();
            assertClusterSizeEventually(initialClusterMembersCount, null);
            HecatonTestSupport.waitAllForSafeState(hecatonInstances);
        }

        @Override
        public void recover() {
            failingInstance = factory.newHecatonInstance(config);
            assertClusterSizeEventually(initialClusterMembersCount + 1, failingInstance);
            HecatonTestSupport.waitAllForSafeState(hecatonInstances);
            HecatonTestSupport.waitAllForSafeState(failingInstance);
        }
    }

    public abstract static class MultipleFailingInstanceClusterFailure {
        protected HecatonInstance[] hecatonInstances;
        protected HecatonInstance[] failingInstances;
        protected TestHecatonFactory factory;
        protected HecatonInstance client;
        protected int initialClusterMembersCount;
        protected int currentSize;
        protected int lastLivingFailingInstanceIndex;
        protected Config config;

        public void initialize(int initialClusterMembersCount, int initialFailingMembersCount, Config config) {
            this.initialClusterMembersCount = initialClusterMembersCount;
            this.config = config;
            factory = new TestHecatonFactory();
            hecatonInstances = factory.newInstances(config, initialClusterMembersCount);
            assertClusterSizeEventually(initialClusterMembersCount);
            failingInstances = factory.newInstances(config, initialFailingMembersCount);
            currentSize = initialClusterMembersCount + initialFailingMembersCount;
            lastLivingFailingInstanceIndex = initialFailingMembersCount - 1;
            assertClusterSizeEventually(currentSize);
            HecatonTestSupport.waitAllForSafeState(hecatonInstances);
            HecatonTestSupport.waitAllForSafeState(failingInstances);
            client = null;
        }

        public HecatonInstance createClient(ClientConfig clientConfig) {
            client = factory.newHecatonClient(clientConfig);
            return client;
        }

        public void cleanUp() {
            client.getLifecycleService().terminate();
            factory.terminateAll();
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }

        public abstract void fail();

        public HecatonInstance getFailingInstance() {
            return failingInstances[0];
        }

        public HecatonInstance getNotFailingInstance() {
            return hecatonInstances[0];
        }

        protected void assertClusterSizeEventually(int size) {
            for (int i = 0; i < hecatonInstances.length; i++) {
                HecatonInstance instance = hecatonInstances[i];
                HecatonTestSupport.assertClusterSizeEventually(size, instance);
            }
            if (failingInstances != null) {
                for (int i = 0; i <= lastLivingFailingInstanceIndex; i++) {
                    HecatonInstance instance = failingInstances[i];
                    HecatonTestSupport.assertClusterSizeEventually(size, instance);
                }
            }
        }
    }
}
