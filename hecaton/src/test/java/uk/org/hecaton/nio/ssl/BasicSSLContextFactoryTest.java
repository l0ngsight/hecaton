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
package uk.org.hecaton.nio.ssl;

import uk.org.hecaton.internal.nio.ssl.TestCertificates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.net.ssl.SSLContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static uk.org.hecaton.test.HecatonTestSupport.sleepMillis;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BasicSSLContextFactoryTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void readsTheStoresOnceByDefault() throws Exception {
        BasicSSLContextFactory factory = new BasicSSLContextFactory();
        factory.init(properties(keyStore("a"), null));
        SSLContext first = factory.getSSLContext();

        sleepMillis(50);
        assertThat(factory.getSSLContext()).isSameAs(first);
    }

    @Test
    public void readsTheStoresAgainAfterTheKeyMaterialDuration() throws Exception {
        Path keyStore = keyStore("a");
        BasicSSLContextFactory factory = new BasicSSLContextFactory();
        factory.init(properties(keyStore, "PT0.1S"));
        SSLContext first = factory.getSSLContext();
        assertThat(factory.getSSLContext()).isSameAs(first);

        Files.copy(keyStore("b"), keyStore, StandardCopyOption.REPLACE_EXISTING);
        sleepMillis(200);
        SSLContext second = factory.getSSLContext();
        assertThat(second).isNotSameAs(first);
        assertThat(factory.getSSLContext()).isSameAs(second);
    }

    @Test
    public void unreadableStoreFails() {
        Properties properties = properties(folder.getRoot().toPath().resolve("missing.p12"), null);
        assertThatThrownBy(() -> new BasicSSLContextFactory().init(properties)).hasMessageContaining("missing.p12");
    }

    private Path keyStore(String name) throws Exception {
        return new TestCertificates(folder.newFolder().toPath(), "ca-" + name).keyStore(name);
    }

    private static Properties properties(Path keyStore, String keyMaterialDuration) {
        Properties properties = new Properties();
        properties.setProperty("keyStore", keyStore.toString());
        properties.setProperty("keyStorePassword", TestCertificates.PASSWORD);
        properties.setProperty("keyStoreType", "PKCS12");
        if (keyMaterialDuration != null) {
            properties.setProperty("keyMaterialDuration", keyMaterialDuration);
        }
        return properties;
    }
}
