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

package uk.org.hecaton.jet.elastic;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import static java.util.Map.of;
import static org.assertj.core.api.Assertions.assertThat;

public class ElasticClientsTest extends BaseElasticTest {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void afterClass() {
        factory.terminateAll();
    }

    @Override
    protected HecatonInstance createHecatonInstance() {
        return factory.newHecatonInstance(config());
    }

    @Test
    public void given_clientAsString_whenReadFromElasticSource_thenFinishSuccessfully() {
        ElasticsearchContainer container = ElasticSupport.elastic.get();
        String httpHostAddress = container.getHttpHostAddress();

        indexDocument("my-index", of("name", "Frantisek"));

        Pipeline p = Pipeline.create();
        p.readFrom(ElasticSources.elastic(
                () -> ElasticClients.client(httpHostAddress),
                SearchHit::getSourceAsString)
        ).writeTo(Sinks.list(results));

        submitJob(p);

        assertThat(results).hasSize(1);
    }
}
