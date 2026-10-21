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

package uk.org.hecaton.persistence.internal.store;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

/**
 * Encrypts the keys and values of log records (F10, encryption at rest) with AES-GCM: each field
 * gets a random 96-bit nonce, stored in front of its ciphertext, and a 128-bit tag, which detects
 * any change to it.
 */
public final class RecordCipher {

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int NONCE_SIZE = 12;
    private static final int TAG_BITS = 128;

    private final SecretKey key;
    private final SecureRandom random = new SecureRandom();

    public RecordCipher(SecretKey key) {
        this.key = key;
    }

    /** @return nonce followed by the ciphertext and tag */
    byte[] encrypt(byte[] plaintext) {
        byte[] nonce = new byte[NONCE_SIZE];
        random.nextBytes(nonce);
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, nonce));
            byte[] encrypted = new byte[NONCE_SIZE + cipher.getOutputSize(plaintext.length)];
            System.arraycopy(nonce, 0, encrypted, 0, NONCE_SIZE);
            cipher.doFinal(plaintext, 0, plaintext.length, encrypted, NONCE_SIZE);
            return encrypted;
        } catch (GeneralSecurityException e) {
            throw new PersistenceStoreException("Cannot encrypt a record", e);
        }
    }

    /** Reverses {@link #encrypt}; fails if the field was changed or encrypted with another key. */
    byte[] decrypt(byte[] encrypted) {
        if (encrypted.length < NONCE_SIZE) {
            throw new PersistenceStoreException("An encrypted record field is too short");
        }
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, encrypted, 0, NONCE_SIZE));
            return cipher.doFinal(encrypted, NONCE_SIZE, encrypted.length - NONCE_SIZE);
        } catch (GeneralSecurityException e) {
            throw new PersistenceStoreException("Cannot decrypt a record: wrong key or corrupted data", e);
        }
    }
}
