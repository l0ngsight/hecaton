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
package uk.org.hecaton.instance.module;

import uk.org.hecaton.instance.impl.Node;

/**
 * A member feature shipped in its own jar (spec §7.2), such as High-Density memory
 * ({@code hecaton-hd}) or persistence ({@code hecaton-persistence}). These modules need Java 25,
 * while core keeps Java 17 bytecode for the client.
 * <p>
 * Modules are found with {@link java.util.ServiceLoader} when a member starts (register the
 * implementation in {@code META-INF/services/uk.org.hecaton.instance.module.MemberModule}). A
 * module provides features by also implementing {@link MemberCapability} interfaces; core asks
 * {@link MemberModules} for a capability where it needs one.
 */
public interface MemberModule {

    /**
     * The module's short name, such as {@code hd}; its artifact is {@code uk.org.hecaton:hecaton-<name>}.
     */
    String name();

    /**
     * Called once while the member starts, before its services are created. Validate the
     * configuration here and fail fast.
     */
    default void init(Node node) {
    }
}
