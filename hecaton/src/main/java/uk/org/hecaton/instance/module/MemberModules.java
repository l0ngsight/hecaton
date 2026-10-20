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
import uk.org.hecaton.logging.ILogger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * The {@link MemberModule}s of a member and the {@link MemberCapability capabilities} they
 * provide.
 */
public final class MemberModules {

    private final List<MemberModule> modules;
    private final Map<Class<? extends MemberCapability>, MemberModule> capabilities = new LinkedHashMap<>();

    /**
     * @throws IllegalStateException if two modules have the same name or provide the same capability
     */
    public MemberModules(Iterable<? extends MemberModule> modules) {
        this.modules = new ArrayList<>();
        for (MemberModule module : modules) {
            for (MemberModule other : this.modules) {
                if (other.name().equals(module.name())) {
                    throw new IllegalStateException("Two member modules are named " + module.name() + ": "
                            + other.getClass().getName() + " and " + module.getClass().getName());
                }
            }
            this.modules.add(module);
            for (Class<?> type : module.getClass().getInterfaces()) {
                addCapabilities(type, module);
            }
        }
    }

    /** The modules registered with {@link ServiceLoader} in {@code classLoader}. */
    public static MemberModules load(ClassLoader classLoader) {
        return new MemberModules(ServiceLoader.load(MemberModule.class, classLoader));
    }

    private void addCapabilities(Class<?> type, MemberModule module) {
        if (MemberCapability.class.isAssignableFrom(type) && type != MemberCapability.class) {
            @SuppressWarnings("unchecked")
            Class<? extends MemberCapability> capability = (Class<? extends MemberCapability>) type;
            MemberModule previous = capabilities.putIfAbsent(capability, module);
            if (previous != null && previous != module) {
                throw new IllegalStateException("Member modules " + previous.name() + " and " + module.name()
                        + " both provide " + capability.getSimpleName() + "; remove one of them");
            }
        }
        for (Class<?> superType : type.getInterfaces()) {
            addCapabilities(superType, module);
        }
    }

    /** Initializes every module ({@link MemberModule#init}). */
    public void init(Node node, ILogger logger) {
        for (MemberModule module : modules) {
            logger.info("Member module " + module.name() + " (" + module.getClass().getName() + ")");
            module.init(node);
        }
    }

    public List<MemberModule> modules() {
        return List.copyOf(modules);
    }

    /** The module providing {@code capability}, if there is one. */
    public <T extends MemberCapability> Optional<T> find(Class<T> capability) {
        return Optional.ofNullable(capability.cast(capabilities.get(capability)));
    }

    /**
     * The module providing {@code capability}.
     *
     * @param feature what needs the capability, for the error message
     * @throws IllegalStateException naming the module's artifact if no module provides it
     */
    public <T extends MemberCapability> T require(Class<T> capability, String feature) {
        return find(capability).orElseThrow(() -> new IllegalStateException(
                feature + " requires uk.org.hecaton:hecaton-" + moduleName(capability)
                        + " on the member's classpath"));
    }

    static String moduleName(Class<? extends MemberCapability> capability) {
        if (capability == NativeMemoryCapability.class) {
            return "hd";
        } else if (capability == PersistenceCapability.class) {
            return "persistence";
        } else if (capability == TieredStoreCapability.class) {
            return "tiered";
        } else if (capability == VectorCapability.class) {
            return "vector";
        }
        throw new IllegalArgumentException("Unknown capability " + capability.getName());
    }
}
