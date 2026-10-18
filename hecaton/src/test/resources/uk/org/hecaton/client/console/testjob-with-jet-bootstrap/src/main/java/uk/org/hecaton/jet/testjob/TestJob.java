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

package uk.org.hecaton.jet.testjob;

import uk.org.hecaton.jet.Jet;
import uk.org.hecaton.jet.JetInstance;
import uk.org.hecaton.jet.core.AbstractProcessor;
import uk.org.hecaton.jet.core.DAG;

public class TestJob {

    public static void main(String[] args) {
        DAG dag = new DAG();
        dag.newVertex("v", StreamNoopProcessor::new);
        JetInstance instance = Jet.bootstrappedInstance();
        instance.newJob(dag);
    }

    private static class StreamNoopProcessor extends AbstractProcessor {
        @Override
        public boolean complete() {
            return false;
        }
    }
}
