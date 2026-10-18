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

package uk.org.hecaton.spi.impl.operationservice.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_RegressionTest extends HecatonTestSupport {

    @Test(expected = ExecutionException.class, timeout = 120000)
    public void testIssue2509() throws Exception {

        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(2);

        HecatonInstance h1 = nodeFactory.newHecatonInstance();
        HecatonInstance h2 = nodeFactory.newHecatonInstance();

        UnDeserializable unDeserializable = new UnDeserializable(1);
        IExecutorService executorService = h1.getExecutorService("default");
        Issue2509Runnable task = new Issue2509Runnable(unDeserializable);
        Future<?> future = executorService.submitToMember(task, h2.getCluster().getLocalMember());
        future.get();
    }

    public static class Issue2509Runnable implements Callable<Integer>, DataSerializable {

        private UnDeserializable unDeserializable;

        Issue2509Runnable() {
        }

        Issue2509Runnable(UnDeserializable unDeserializable) {
            this.unDeserializable = unDeserializable;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(unDeserializable);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            unDeserializable = in.readObject();
        }

        @Override
        public Integer call() {
            return unDeserializable.foo;
        }
    }

    public static class UnDeserializable implements DataSerializable {

        private int foo;

        UnDeserializable(int foo) {
            this.foo = foo;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(foo);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            foo = in.readInt();
        }
    }
}
