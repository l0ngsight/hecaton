/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.migrate.rewrite;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Properties;

/** The published Hecaton artifacts and the Hazelcast 5.7 artifacts they replace (spec §5.2). */
final class HecatonArtifacts {

    /**
     * A Hazelcast artifact and its Hecaton replacement.
     *
     * @param hazelcastGroupId    Hazelcast group ID
     * @param hazelcastArtifactId Hazelcast artifact ID
     * @param hecatonGroupId      Hecaton group ID
     * @param hecatonArtifactId   Hecaton artifact ID
     */
    record Artifact(String hazelcastGroupId, String hazelcastArtifactId, String hecatonGroupId, String hecatonArtifactId) {
    }

    /** Hecaton's version, which replaces the Hazelcast version. */
    static final String VERSION = loadVersion();

    /** Artifacts of removed features (cloud-provider discovery and connectors, D9) are not listed. */
    static final List<Artifact> ARTIFACTS = List.of(
            core("hecaton"),
            core("hecaton-spring"),
            core("hecaton-sql"),
            core("hecaton-mapstore"),
            core("hecaton-tpc-engine"),
            core("hecaton-spring-boot3"),
            core("hecaton-spring-boot4"),
            core("hecaton-distribution"),
            jet("avro"),
            jet("cdc-debezium"),
            jet("cdc-mysql"),
            jet("cdc-postgres"),
            jet("csv"),
            jet("elasticsearch-7"),
            jet("grpc"),
            jet("hadoop"),
            jet("hadoop-all"),
            jet("hadoop-core"),
            jet("kafka"),
            jet("kafka-connect"),
            jet("mongodb"),
            jet("protobuf"),
            jet("python"));

    private HecatonArtifacts() {
    }

    private static Artifact core(String hecatonArtifactId) {
        return new Artifact("com.hazelcast", hecatonArtifactId.replace("hecaton", "hazelcast"),
                "uk.org.hecaton", hecatonArtifactId);
    }

    private static Artifact jet(String connector) {
        return new Artifact("com.hazelcast.jet", "hazelcast-jet-" + connector, "uk.org.hecaton.jet", "hecaton-jet-" + connector);
    }

    private static String loadVersion() {
        try (InputStream in = HecatonArtifacts.class.getResourceAsStream("version.properties")) {
            Properties properties = new Properties();
            properties.load(in);
            return properties.getProperty("hecaton.version");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
