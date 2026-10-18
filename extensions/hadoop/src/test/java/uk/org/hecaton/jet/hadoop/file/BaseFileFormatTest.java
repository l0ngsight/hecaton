/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.jet.hadoop.file;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.function.ConsumerEx;
import uk.org.hecaton.jet.JetException;
import uk.org.hecaton.jet.hadoop.impl.HadoopTestSupport;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.file.FileSourceBuilder;
import uk.org.hecaton.jet.pipeline.test.Assertions;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParametrizedRunner.class)
public abstract class BaseFileFormatTest extends HadoopTestSupport {

    @Parameter
    public boolean useHadoop;

    protected String currentDir;

    @Parameters(name = "{index}: useHadoop={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(true, false);
    }

    @Override
    protected boolean useHadoop() {
        return useHadoop;
    }

    @Before
    public void setUp() {
        currentDir = Paths.get(".").toAbsolutePath().normalize().toString();
    }

    @SafeVarargs
    protected final <T> void assertItemsInSource(FileSourceBuilder<T> source, T... items) {
        assertItemsInSource(source, collected -> assertThat(collected).containsOnly(items));
    }

    protected <T> void assertItemsInSource(
            FileSourceBuilder<T> source, ConsumerEx<List<T>> assertion
    ) {
        assertItemsInSource(1, source, assertion);
    }

    protected <T> void assertItemsInSource(
            int memberCount, FileSourceBuilder<T> source, ConsumerEx<List<T>> assertion
    ) {
        if (useHadoop) {
            source.useHadoopForLocalFiles(true);
        }

        Pipeline p = Pipeline.create();

        p.readFrom(source.build())
         .apply(Assertions.assertCollected(assertion));

        HecatonInstance[] instances = createHecatonInstances(memberCount);
        try {
            instances[0].getJet().newJob(p).join();
        } finally {
            for (HecatonInstance instance : instances) {
                instance.shutdown();
            }
        }
    }

    protected void assertJobFailed(
            FileSourceBuilder<?> source,
            Class<? extends Throwable> expectedRootException,
            String expectedMessage
    ) {
        if (useHadoop) {
            source.useHadoopForLocalFiles(true);
        }

        Pipeline p = Pipeline.create();

        p.readFrom(source.build())
         .writeTo(Sinks.logger());

        HecatonInstance[] instances = createHecatonInstances(1);

        try {
            assertThatThrownBy(() -> instances[0].getJet().newJob(p).join())
                    .hasCauseInstanceOf(JetException.class)
                    // can't use hasRootCauseInstanceOf because of mismatch between shaded/non-shaded version
                    .hasStackTraceContaining(expectedRootException.getName())
                    .hasMessageContaining(expectedMessage);
        } finally {
            for (HecatonInstance instance : instances) {
                instance.shutdown();
            }
        }
    }
}
