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
import uk.org.hecaton.config.SecureStoreConfig;
import uk.org.hecaton.config.VaultSecureStoreConfig;

import java.util.List;

/**
 * Where the master keys of encryption at rest come from (F10): a Java KeyStore or HashiCorp Vault.
 */
public interface MasterKeySource {

    /** The master keys, the current one first, then the previous ones (for key rotation). */
    List<byte[]> keys();

    static MasterKeySource of(SecureStoreConfig config) {
        if (config instanceof JavaKeyStoreSecureStoreConfig keyStore) {
            return new KeyStoreMasterKeySource(keyStore);
        }
        if (config instanceof VaultSecureStoreConfig vault) {
            return new VaultMasterKeySource(vault);
        }
        throw new IllegalArgumentException("Encryption at rest needs a secure store (keystore or vault), not " + config);
    }
}
