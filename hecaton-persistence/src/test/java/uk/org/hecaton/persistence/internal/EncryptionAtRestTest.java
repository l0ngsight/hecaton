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

package uk.org.hecaton.persistence.internal;

import com.sun.net.httpserver.HttpServer;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DataPersistenceConfig;
import uk.org.hecaton.config.EncryptionAtRestConfig;
import uk.org.hecaton.config.JavaKeyStoreSecureStoreConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.SecureStoreConfig;
import uk.org.hecaton.config.VaultSecureStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.persistence.BackupTaskState;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Encryption at rest (F10): persisted data is encrypted with a data key that the master key from
 * the secure store protects; the master key can be rotated.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class EncryptionAtRestTest extends HecatonTestSupport {

    private static final int ENTRIES = 500;
    private static final String SECRET = "top-secret-value";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(5);

    @Test
    public void dataIsEncryptedAndSurvivesRestartsAndKeyRotation() throws Exception {
        File baseDir = folder.newFolder();
        File keyStore = folder.newFile("keys.p12");
        SecretKey first = aesKey();
        SecretKey second = aesKey();
        writeKeyStore(keyStore, List.of("k1"), List.of(first));
        JavaKeyStoreSecureStoreConfig store = new JavaKeyStoreSecureStoreConfig(keyStore).setType("PKCS12")
                .setPassword("pw");

        HecatonInstance member = factory.newHecatonInstance(config(baseDir, store));
        fill(member);
        member.getCluster().shutdown();
        assertThat(filesContain(baseDir, SECRET)).isFalse();

        // a new current key: the data key is wrapped with it
        writeKeyStore(keyStore, List.of("k1", "k2"), List.of(first, second));
        member = factory.newHecatonInstance(config(baseDir, store.setCurrentKeyAlias("k2")));
        assertData(member);
        member.getCluster().shutdown();

        // the old key is not needed any more
        writeKeyStore(keyStore, List.of("k2"), List.of(second));
        member = factory.newHecatonInstance(config(baseDir, new JavaKeyStoreSecureStoreConfig(keyStore)
                .setType("PKCS12").setPassword("pw")));
        assertData(member);
    }

    @Test
    public void anotherKeyCannotReadTheData() throws Exception {
        File baseDir = folder.newFolder();
        File keyStore = folder.newFile("keys.p12");
        writeKeyStore(keyStore, List.of("k1"), List.of(aesKey()));
        JavaKeyStoreSecureStoreConfig store = new JavaKeyStoreSecureStoreConfig(keyStore).setType("PKCS12")
                .setPassword("pw");
        HecatonInstance member = factory.newHecatonInstance(config(baseDir, store));
        fill(member);
        member.getCluster().shutdown();

        writeKeyStore(keyStore, List.of("k1"), List.of(aesKey()));
        assertThatThrownBy(() -> factory.newHecatonInstance(config(baseDir, store)))
                .hasStackTraceContaining("None of the master keys");
    }

    @Test
    public void backupIsEncryptedAndRestoredWithTheMasterKey() throws Exception {
        File baseDir = folder.newFolder();
        File backupDir = folder.newFolder();
        File keyStore = folder.newFile("keys.p12");
        writeKeyStore(keyStore, List.of("k1"), List.of(aesKey()));
        JavaKeyStoreSecureStoreConfig store = new JavaKeyStoreSecureStoreConfig(keyStore).setType("PKCS12")
                .setPassword("pw");
        Config config = config(baseDir, store);
        config.getPersistenceConfig().setBackupDir(backupDir);
        HecatonInstance member = factory.newHecatonInstance(config);
        fill(member);
        member.getCluster().getPersistenceService().backup(1);
        HecatonInstance backedUp = member;
        assertTrueEventually(() -> assertThat(backedUp.getCluster().getPersistenceService().getBackupTaskStatus()
                .getState()).isEqualTo(BackupTaskState.SUCCESS));
        member.getCluster().shutdown();

        File backup = new File(backupDir, "backup-1");
        assertThat(filesContain(backup, SECRET)).isFalse();
        member = factory.newHecatonInstance(config(backup, store));
        assertData(member);
    }

    @Test
    public void keysFromVault() throws Exception {
        File baseDir = folder.newFolder();
        List<String> versions = new ArrayList<>();
        versions.add(Base64.getEncoder().encodeToString(aesKey().getEncoded()));
        HttpServer vault = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        vault.createContext("/v1/secret/data/hecaton", exchange -> {
            String query = exchange.getRequestURI().getQuery();
            int version = query == null ? versions.size() : Integer.parseInt(query.substring("version=".length()));
            boolean authorized = "vault-token".equals(exchange.getRequestHeaders().getFirst("X-Vault-Token"));
            byte[] body = ("{\"data\":{\"data\":{\"key\":\"" + versions.get(version - 1) + "\"},"
                    + "\"metadata\":{\"version\":" + version + "}}}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(authorized ? 200 : 403, authorized ? body.length : -1);
            if (authorized) {
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
            }
            exchange.close();
        });
        vault.start();
        try {
            VaultSecureStoreConfig store = new VaultSecureStoreConfig("http://127.0.0.1:" + vault.getAddress().getPort(),
                    "secret/data/hecaton", "vault-token");
            HecatonInstance member = factory.newHecatonInstance(config(baseDir, store));
            fill(member);
            member.getCluster().shutdown();
            assertThat(filesContain(baseDir, SECRET)).isFalse();

            // rotation: a new version of the secret
            versions.add(Base64.getEncoder().encodeToString(aesKey().getEncoded()));
            member = factory.newHecatonInstance(config(baseDir, store));
            assertData(member);
        } finally {
            vault.stop(0);
        }
    }

    private static void fill(HecatonInstance member) {
        IMap<Integer, String> map = member.getMap("persistent");
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, SECRET + i);
        }
    }

    private static void assertData(HecatonInstance member) {
        assertTrueEventually(() -> assertThat(getNode(member).getNodeExtension().isStartCompleted()).isTrue());
        IMap<Integer, String> map = member.getMap("persistent");
        assertThat(map.size()).isEqualTo(ENTRIES);
        assertThat(map.get(7)).isEqualTo(SECRET + 7);
    }

    private static Config config(File baseDir, SecureStoreConfig store) {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "31");
        config.getPersistenceConfig().setEnabled(true).setBaseDir(baseDir)
                .setValidationTimeoutSeconds(60).setDataLoadTimeoutSeconds(60)
                .setEncryptionAtRestConfig(new EncryptionAtRestConfig().setEnabled(true).setAlgorithm("AES/GCM/NoPadding")
                        .setSecureStoreConfig(store));
        config.addMapConfig(new MapConfig("persistent")
                .setDataPersistenceConfig(new DataPersistenceConfig().setEnabled(true)));
        return config;
    }

    private static SecretKey aesKey() throws Exception {
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        return generator.generateKey();
    }

    private static void writeKeyStore(File file, List<String> aliases, List<SecretKey> keys) throws Exception {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        for (int i = 0; i < aliases.size(); i++) {
            keyStore.setEntry(aliases.get(i), new KeyStore.SecretKeyEntry(keys.get(i)),
                    new KeyStore.PasswordProtection("pw".toCharArray()));
        }
        try (FileOutputStream out = new FileOutputStream(file)) {
            keyStore.store(out, "pw".toCharArray());
        }
    }

    private static boolean filesContain(File dir, String text) throws IOException {
        byte[] needle = text.getBytes(StandardCharsets.UTF_8);
        try (Stream<Path> files = Files.walk(dir.toPath())) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                byte[] content = Files.readAllBytes(file);
                for (int i = 0; i + needle.length <= content.length; i++) {
                    int j = 0;
                    while (j < needle.length && content[i + j] == needle[j]) {
                        j++;
                    }
                    if (j == needle.length) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
