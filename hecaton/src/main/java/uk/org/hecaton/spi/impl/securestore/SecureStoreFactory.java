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

package uk.org.hecaton.spi.impl.securestore;

import uk.org.hecaton.config.SecureStoreConfig;

import javax.annotation.Nonnull;

/**
 * Factory to provide {@link SecureStore} implementations.
 */
public interface SecureStoreFactory {
    /**
     * Returns a {@link SecureStore} implementation for the provided configuration.
     *
     * @param secureStoreConfig the Secure Store configuration
     * @return a {@link SecureStore} implementation
     * @throws uk.org.hecaton.config.InvalidConfigurationException if the configuration is invalid
     *                                                            or not supported by this factory
     */
    @Nonnull
    SecureStore getSecureStore(@Nonnull SecureStoreConfig secureStoreConfig);
}
