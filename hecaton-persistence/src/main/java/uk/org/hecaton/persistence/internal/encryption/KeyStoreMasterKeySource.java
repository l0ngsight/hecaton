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

package uk.org.hecaton.persistence.internal.encryption;

import uk.org.hecaton.config.JavaKeyStoreSecureStoreConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Master keys from a Java KeyStore (PKCS12, JCEKS, ...): the secret key entry {@code current-key-alias}
 * (by default the last alias in alphabetical order) is the current key; the other secret key entries,
 * most recent alias first, are previous keys.
 */
class KeyStoreMasterKeySource implements MasterKeySource {

    private final JavaKeyStoreSecureStoreConfig config;

    KeyStoreMasterKeySource(JavaKeyStoreSecureStoreConfig config) {
        this.config = config;
    }

    @Override
    public List<byte[]> keys() {
        char[] password = config.getPassword() == null ? null : config.getPassword().toCharArray();
        try (InputStream in = Files.newInputStream(config.getPath().toPath())) {
            KeyStore keyStore = KeyStore.getInstance(config.getType());
            keyStore.load(in, password);
            List<String> aliases = aliases(keyStore);
            List<byte[]> keys = new ArrayList<>();
            for (String alias : aliases) {
                Key key = keyStore.getKey(alias, password);
                if (key != null && key.getEncoded() != null && "AES".equalsIgnoreCase(key.getAlgorithm())) {
                    keys.add(key.getEncoded());
                }
            }
            if (keys.isEmpty()) {
                throw new IllegalStateException("The keystore " + config.getPath() + " has no AES key");
            }
            return keys;
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("Cannot read the encryption keys from " + config.getPath() + ": "
                    + e.getMessage(), e);
        }
    }

    /** The aliases of the key entries, the current key first, then most recent alias first. */
    private List<String> aliases(KeyStore keyStore) throws KeyStoreException {
        List<String> aliases = new ArrayList<>();
        for (String alias : Collections.list(keyStore.aliases())) {
            if (keyStore.isKeyEntry(alias)) {
                aliases.add(alias);
            }
        }
        aliases.sort(Collections.reverseOrder());
        String current = config.getCurrentKeyAlias();
        if (current != null) {
            if (!aliases.remove(current)) {
                throw new IllegalStateException("The keystore " + config.getPath() + " has no key " + current);
            }
            aliases.add(0, current);
        }
        return aliases;
    }
}
