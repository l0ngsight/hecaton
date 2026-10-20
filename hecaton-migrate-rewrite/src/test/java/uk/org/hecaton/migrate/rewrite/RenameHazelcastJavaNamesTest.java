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

import org.junit.jupiter.api.Test;
import org.openrewrite.java.JavaParser;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;
import org.openrewrite.test.TypeValidation;

import static org.openrewrite.java.Assertions.java;

class RenameHazelcastJavaNamesTest implements RewriteTest {

    /** Just enough of the Hazelcast 5.7 API to type-attribute the samples. */
    private static final String[] HAZELCAST_STUBS = {
            """
            package com.hazelcast.config;
            public class Config {
                public Config setProperty(String name, String value) { return this; }
            }
            """,
            """
            package com.hazelcast.map;
            public interface IMap<K, V> {
                V put(K key, V value);
            }
            """,
            """
            package com.hazelcast.core;
            public interface HazelcastInstance {
                <K, V> com.hazelcast.map.IMap<K, V> getMap(String name);
            }
            """,
            """
            package com.hazelcast.core;
            public interface HazelcastInstanceAware {
                void setHazelcastInstance(HazelcastInstance instance);
            }
            """,
            """
            package com.hazelcast.core;
            public final class Hazelcast {
                public static HazelcastInstance newHazelcastInstance(com.hazelcast.config.Config config) { return null; }
            }
            """,
            """
            package com.hazelcast.client;
            public final class HazelcastClient {
                public static com.hazelcast.core.HazelcastInstance newHazelcastClient() { return null; }
            }
            """,
            """
            package com.hazelcast.instance;
            public class BuildInfo {
                public static final int UNKNOWN_HAZELCAST_VERSION = -1;
            }
            """
    };

    @Override
    public void defaults(RecipeSpec spec) {
        spec.recipe(new FromHazelcast())
                .parser(JavaParser.fromJavaVersion().dependsOn(HAZELCAST_STUBS))
                // the Hecaton types are not on the test classpath
                .afterTypeValidationOptions(TypeValidation.none());
    }

    @Test
    void embeddedMember() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import com.hazelcast.config.Config;
                        import com.hazelcast.core.Hazelcast;
                        import com.hazelcast.core.HazelcastInstance;
                        import com.hazelcast.map.IMap;

                        class App {
                            void run() {
                                Config config = new Config().setProperty("hazelcast.logging.type", "slf4j");
                                HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
                                IMap<String, String> sessions = hz.getMap("hazelcast.sessions");
                                sessions.put("k", "v");
                            }
                        }
                        """,
                        """
                        package com.example;

                        import uk.org.hecaton.config.Config;
                        import uk.org.hecaton.core.Hecaton;
                        import uk.org.hecaton.core.HecatonInstance;
                        import uk.org.hecaton.map.IMap;

                        class App {
                            void run() {
                                Config config = new Config().setProperty("hecaton.logging.type", "slf4j");
                                HecatonInstance hz = Hecaton.newHecatonInstance(config);
                                IMap<String, String> sessions = hz.getMap("hazelcast.sessions");
                                sessions.put("k", "v");
                            }
                        }
                        """
                )
        );
    }

    @Test
    void clientAndOverriddenMethod() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import com.hazelcast.client.HazelcastClient;
                        import com.hazelcast.core.HazelcastInstance;
                        import com.hazelcast.core.HazelcastInstanceAware;

                        class Task implements HazelcastInstanceAware {
                            private HazelcastInstance hazelcastInstance;

                            @Override
                            public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
                                this.hazelcastInstance = hazelcastInstance;
                            }

                            static Task connect() {
                                Task task = new Task();
                                task.setHazelcastInstance(HazelcastClient.newHazelcastClient());
                                return task;
                            }
                        }
                        """,
                        """
                        package com.example;

                        import uk.org.hecaton.client.HecatonClient;
                        import uk.org.hecaton.core.HecatonInstance;
                        import uk.org.hecaton.core.HecatonInstanceAware;

                        class Task implements HecatonInstanceAware {
                            private HecatonInstance hazelcastInstance;

                            @Override
                            public void setHecatonInstance(HecatonInstance hazelcastInstance) {
                                this.hazelcastInstance = hazelcastInstance;
                            }

                            static Task connect() {
                                Task task = new Task();
                                task.setHecatonInstance(HecatonClient.newHecatonClient());
                                return task;
                            }
                        }
                        """
                )
        );
    }

    @Test
    void constant() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        import com.hazelcast.instance.BuildInfo;

                        class Versions {
                            int unknown = BuildInfo.UNKNOWN_HAZELCAST_VERSION;
                        }
                        """,
                        """
                        package com.example;

                        import uk.org.hecaton.instance.BuildInfo;

                        class Versions {
                            int unknown = BuildInfo.UNKNOWN_HECATON_VERSION;
                        }
                        """
                )
        );
    }

    @Test
    void stringsOfOtherProductsUnchanged() {
        rewriteRun(
                java(
                        """
                        package com.example;

                        class Docs {
                            String url = "https://docs.hazelcast.com/hazelcast/latest/";
                            String name = "my-hazelcast-cache";
                        }
                        """
                )
        );
    }
}
