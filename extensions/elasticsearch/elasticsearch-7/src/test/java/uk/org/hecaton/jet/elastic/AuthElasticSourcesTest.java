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
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.pipeline.BatchSource;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.test.IgnoreInJenkinsOnWindows;
import uk.org.hecaton.jet.test.SerialTest;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import javax.annotation.Nonnull;

import static uk.org.hecaton.jet.elastic.ElasticClients.client;
import static uk.org.hecaton.jet.elastic.ElasticSupport.PORT;
import static java.util.Map.of;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category({NightlyTest.class, SerialTest.class, IgnoreInJenkinsOnWindows.class})
public class AuthElasticSourcesTest extends BaseElasticTest {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void afterClass() {
        factory.terminateAll();
    }

    @Override protected SupplierEx<RestClientBuilder> elasticClientSupplier() {
        return ElasticSupport.secureElasticClientSupplier();
    }

    @Override
    protected HecatonInstance createHecatonInstance() {
        return factory.newHecatonInstance(config());
    }

    @Test
    public void given_authenticatedClient_whenReadFromElasticSource_thenFinishSuccessfully() {
        indexDocument("my-index", of("name", "Frantisek"));

        Pipeline p = Pipeline.create();
        p.readFrom(elasticSource(elasticClientSupplier()))
         .writeTo(Sinks.list(results));

        submitJob(p);
    }

    @Test
    public void given_clientWithWrongPassword_whenReadFromElasticSource_thenFailWithAuthenticationException() {
        ElasticsearchContainer container = ElasticSupport.secureElastic.get();
        String containerIp = container.getHost();
        Integer port = container.getMappedPort(PORT);

        Pipeline p = Pipeline.create();
        p.readFrom(elasticSource(() -> client("elastic", "WrongPassword", containerIp, port)))
         .writeTo(Sinks.list(results));

        assertThatThrownBy(() -> submitJob(p))
                .hasRootCauseInstanceOf(ResponseException.class)
                .hasStackTraceContaining("unable to authenticate user [elastic]");
    }

    @Test
    public void given_clientWithoutAuthentication_whenReadFromElasticSource_then_failWithAuthenticationException() {
        ElasticsearchContainer container = ElasticSupport.secureElastic.get();
        String containerIp = container.getHost();
        Integer port = container.getMappedPort(PORT);

        Pipeline p = Pipeline.create();
        p.readFrom(elasticSource(() -> client(containerIp, port)))
         .writeTo(Sinks.list(results));

        assertThatThrownBy(() -> submitJob(p))
                .hasRootCauseInstanceOf(ResponseException.class)
                .hasStackTraceContaining("missing authentication credentials");
    }

    @Nonnull
    private BatchSource<String> elasticSource(SupplierEx<RestClientBuilder> clientFn) {
        return ElasticSources.builder()
                             .clientFn(clientFn)
                             .searchRequestFn(SearchRequest::new)
                             .mapToItemFn(SearchHit::getSourceAsString)
                             .retries(0)
                             .build();
    }
}
