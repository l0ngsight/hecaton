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

package uk.org.hecaton.jet;

import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientFailoverConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.Preconditions;
import uk.org.hecaton.jet.config.JetConfig;

import javax.annotation.Nonnull;

/**
 * Entry point to the Jet product.
 *
 * @since Jet 3.0
 *
 * @deprecated After 5.0 Jet was merged into core Hecaton product. Use
 * the {@link Hecaton} class as the entry point.
 */
@Deprecated(since = "5.0", forRemoval = true)
public final class Jet {

    private Jet() {
    }

    /**
     * @since Jet 4.0
     * @deprecated
     * Please use {@link Hecaton#bootstrappedInstance()} and then get
     * {@link JetService} from the created {@link HecatonInstance}
     * by using {@link HecatonInstance#getJet()}}.
     */
    @Nonnull
    @Deprecated(since = "5.0", forRemoval = true)
    public static JetInstance bootstrappedInstance() {
        return (JetInstance) Hecaton.bootstrappedInstance().getJet();
    }

    /**
     * @deprecated
     * Please use {@link Hecaton#newHecatonInstance()} and then get
     * {@link JetService} from the created {@link HecatonInstance}
     * by using {@link HecatonInstance#getJet()}}.
     */
    @Deprecated(since = "5.0", forRemoval = true)
    @Nonnull
    public static JetInstance newJetInstance() {
        return (JetInstance) Hecaton.newHecatonInstance().getJet();
    }

    /**
     * @deprecated
     * Use {@link Hecaton#newHecatonInstance(Config)} and then get
     * {@link JetService} from the created {@link HecatonInstance} by
     * using {@link HecatonInstance#getJet()}}.
     */
    @Deprecated(since = "5.0", forRemoval = true)
    @Nonnull
    public static JetInstance newJetInstance(@Nonnull JetConfig config) {
        Preconditions.checkNotNull(config, "config");
        Config hzConfig = new Config();
        hzConfig.setJetConfig(config);
        return (JetInstance) Hecaton.newHecatonInstance(hzConfig).getJet();
    }

    /**
     * @deprecated
     * Use {@link HecatonClient#newHecatonClient()} and then get
     * {@link JetService} from the created {@link HecatonInstance}
     * client by using {@link HecatonInstance#getJet()}}.
     */
    @Deprecated(since = "5.0", forRemoval = true)
    @Nonnull
    public static JetInstance newJetClient() {
        return (JetInstance) HecatonClient.newHecatonClient().getJet();
    }

    /**
     * @deprecated
     * Use {@link HecatonClient#newHecatonClient(ClientConfig)} and
     * then get {@link JetService} from the created {@link
     * HecatonInstance} client by using {@link HecatonInstance#getJet()}}.
     */
    @Deprecated(since = "5.0", forRemoval = true)
    @Nonnull
    public static JetInstance newJetClient(@Nonnull ClientConfig config) {
        Preconditions.checkNotNull(config, "config");
        return (JetInstance) HecatonClient.newHecatonClient(config).getJet();
    }

    /**
     * @deprecated
     * Use {@link HecatonClient#newHecatonFailoverClient()} and
     * then get {@link JetService} from the created {@link HecatonInstance}
     * client by using {@link HecatonInstance#getJet()}}.
     */
    @Deprecated(since = "5.0", forRemoval = true)
    @Nonnull
    public static JetInstance newJetFailoverClient() {
        return (JetInstance) HecatonClient.newHecatonFailoverClient().getJet();
    }

    /**
     * @deprecated
     * Use {@link HecatonClient#newHecatonFailoverClient(ClientFailoverConfig)}
     * and then get {@link JetService} from the created {@link HecatonInstance}
     * client by using {@link HecatonInstance#getJet()}}.
     */
    @Deprecated(since = "5.0", forRemoval = true)
    @Nonnull
    public static JetInstance newJetFailoverClient(@Nonnull ClientFailoverConfig config) {
        Preconditions.checkNotNull(config, "config");
        return (JetInstance) HecatonClient.newHecatonFailoverClient(config).getJet();
    }
}
