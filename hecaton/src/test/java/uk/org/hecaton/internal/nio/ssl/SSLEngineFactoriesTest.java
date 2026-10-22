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
package uk.org.hecaton.internal.nio.ssl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.SSLConfig;
import uk.org.hecaton.nio.ssl.SSLContextFactory;
import uk.org.hecaton.nio.ssl.SSLEngineFactory;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.nio.file.Path;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SSLEngineFactoriesTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void clientNeedsNoKeyStore() {
        SSLEngine engine = SSLEngineFactories.create(new SSLConfig().setEnabled(true), true, null).create(true, null);
        assertThat(engine.getUseClientMode()).isTrue();
    }

    @Test
    public void appliesEngineProperties() throws Exception {
        TestCertificates ca = new TestCertificates(folder.getRoot().toPath(), "ca");
        Path keyStore = ca.keyStore("member", "127.0.0.1");
        SSLConfig config = new SSLConfig().setEnabled(true)
                .setProperty("keyStore", keyStore.toString())
                .setProperty("keyStorePassword", TestCertificates.PASSWORD)
                .setProperty("keyStoreType", "PKCS12")
                .setProperty("protocols", "TLSv1.3")
                .setProperty("ciphersuites", "TLS_AES_128_GCM_SHA256")
                .setProperty("mutualAuthentication", "required");
        SSLEngineFactory factory = SSLEngineFactories.create(config, false, null);

        SSLEngine accepting = factory.create(false, null);
        assertThat(accepting.getNeedClientAuth()).isTrue();
        assertThat(accepting.getEnabledProtocols()).containsExactly("TLSv1.3");
        assertThat(accepting.getEnabledCipherSuites()).containsExactly("TLS_AES_128_GCM_SHA256");

        SSLEngine connecting = factory.create(true, new Address("127.0.0.1", 5701));
        assertThat(connecting.getPeerHost()).isEqualTo("127.0.0.1");
        assertThat(connecting.getSSLParameters().getEndpointIdentificationAlgorithm()).isNull();
    }

    @Test
    public void usesAConfiguredContextFactory() {
        SSLConfig config = new SSLConfig().setEnabled(true).setFactoryImplementation(new DefaultContextFactory());
        assertThat(SSLEngineFactories.create(config, false, null).create(false, null)).isNotNull();
    }

    @Test
    public void rejectsOtherFactoryTypes() {
        SSLConfig config = new SSLConfig().setEnabled(true).setFactoryImplementation(new Object());
        assertThatThrownBy(() -> SSLEngineFactories.create(config, false, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("neither an SSLEngineFactory nor an SSLContextFactory");
    }

    static class DefaultContextFactory implements SSLContextFactory {

        private SSLContext context;

        @Override
        public void init(Properties properties) throws Exception {
            context = SSLContext.getDefault();
        }

        @Override
        public SSLContext getSSLContext() {
            return context;
        }
    }
}
