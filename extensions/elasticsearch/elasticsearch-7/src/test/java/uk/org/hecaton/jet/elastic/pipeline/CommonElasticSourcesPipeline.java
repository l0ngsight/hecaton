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
package uk.org.hecaton.jet.elastic.pipeline;

import uk.org.hecaton.collection.IList;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.elastic.ElasticSourceBuilder;
import uk.org.hecaton.jet.elastic.ElasticSources;
import uk.org.hecaton.jet.pipeline.BatchSource;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public final class CommonElasticSourcesPipeline {

    private CommonElasticSourcesPipeline() {
    }

    public static Pipeline readFromIndexAsStringPipeline(
            String index,
            SupplierEx<RestClientBuilder> elasticSupplier,
            IList<String> resultsList) {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticSupplier)
                .searchRequestFn(() -> new SearchRequest(index))
                .mapToItemFn(SearchHit::getSourceAsString)
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(resultsList));

        return p;
    }

    public static Pipeline readFromIndexExtractNamePipeline(
            String index,
            SupplierEx<RestClientBuilder> elasticSupplier,
            IList<String> resultsList) {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticSupplier)
                .searchRequestFn(() -> new SearchRequest(index))
                .mapToItemFn(hit -> (String) hit.getSourceAsMap().get("name"))
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(resultsList));

        return p;
    }

    public static Pipeline readFromIndexUsingSourceFactoryMethod1ExtractNamePipeline(
            SupplierEx<RestClientBuilder> elasticSupplier,
            IList<String> resultsList) {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = ElasticSources.elastic(
                elasticSupplier,
                hit -> (String) hit.getSourceAsMap().get("name")
        );

        p.readFrom(source)
                .writeTo(Sinks.list(resultsList));

        return p;
    }

    public static Pipeline readFromIndexUsingSourceFactoryMethod2ExtractNamePipeline(
            String index,
            SupplierEx<RestClientBuilder> elasticSupplier,
            IList<String> resultsList) {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = ElasticSources.elastic(
                elasticSupplier,
                () -> new SearchRequest(index),
                hit -> (String) hit.getSourceAsMap().get("name")
        );

        p.readFrom(source)
                .writeTo(Sinks.list(resultsList));

        return p;
    }

    public static Pipeline readFromIndexUsingScrollAsStringPipeline(
            String index,
            SupplierEx<RestClientBuilder> elasticSupplier,
            IList<String> resultsList) {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticSupplier)
                .searchRequestFn(() -> {
                    SearchRequest sr = new SearchRequest(index);

                    sr.source().size(10) // needs to scroll 5 times
                            .query(matchAllQuery());
                    return sr;
                })
                .mapToItemFn(SearchHit::getSourceAsString)
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(resultsList));

        return p;
    }

    public static Pipeline readFromIndexWithQueryExtractNamePipeline(
            String index,
            SupplierEx<RestClientBuilder> elasticSupplier,
            IList<String> resultsList) {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticSupplier)
                .searchRequestFn(() -> new SearchRequest(index)
                        .source(new SearchSourceBuilder().query(QueryBuilders.matchQuery("name", "Frantisek"))))
                .mapToItemFn(hit -> (String) hit.getSourceAsMap().get("name"))
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(resultsList));

        return p;
    }

    public static Pipeline readFromIndexAsStringEnableSlicingPipeline(
            String index,
            SupplierEx<RestClientBuilder> elasticSupplier,
            IList<String> resultsList) {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticSupplier)
                .searchRequestFn(() -> new SearchRequest(index))
                .mapToItemFn(SearchHit::getSourceAsString)
                .enableSlicing()
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(resultsList));

        return p;
    }

    public static Pipeline readFromIndexAsStringZeroRetriesPipeline(
            String index,
            SupplierEx<RestClientBuilder> elasticSupplier,
            IList<String> resultsList) {
        Pipeline p = Pipeline.create();

        BatchSource<String> source = new ElasticSourceBuilder<>()
                .clientFn(elasticSupplier)
                .searchRequestFn(() -> new SearchRequest(index))
                .mapToItemFn(SearchHit::getSourceAsString)
                .retries(0) // we expect the exception -> faster test
                .build();

        p.readFrom(source)
                .writeTo(Sinks.list(resultsList));

        return p;
    }

}
