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

import uk.org.hecaton.config.EncryptionAtRestConfig;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.persistence.internal.store.AtomicFiles;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;

/**
 * The data key of a member's persisted data (F10): a random AES key that encrypts the records, kept in
 * the member directory encrypted ("wrapped") with the master key from the secure store. When the
 * master key changes, the data key is wrapped again with the new one; the data is not rewritten.
 */
public final class DataKey {

    /** The name of the data key file in the member directory. */
    public static final String FILE = "encryption.key";
    private static final int MAGIC = 0x4843454b;
    private static final int FORMAT = 1;
    private static final int NONCE_SIZE = 12;
    /** Magic, format and nonce. */
    private static final int HEADER_SIZE = Integer.BYTES * 2 + NONCE_SIZE;
    private static final int TAG_BITS = 128;
    private static final int DEFAULT_KEY_BITS = 256;
    private static final byte[] AAD = "hecaton-data-key".getBytes(StandardCharsets.UTF_8);

    private final Path file;
    private final MasterKeySource masterKeys;
    private final ILogger logger;
    private final SecretKey key;
    private byte[] wrappedWith;

    private DataKey(Path file, MasterKeySource masterKeys, ILogger logger, SecretKey key, byte[] wrappedWith) {
        this.file = file;
        this.masterKeys = masterKeys;
        this.logger = logger;
        this.key = key;
        this.wrappedWith = wrappedWith;
    }

    /** Reads the member's data key, or creates it on the member's first start. */
    public static DataKey load(Path memberDir, EncryptionAtRestConfig config, ILogger logger) {
        MasterKeySource source = MasterKeySource.of(config.getSecureStoreConfig());
        List<byte[]> masters = source.keys();
        Path file = memberDir.resolve(FILE);
        try {
            if (!Files.exists(file)) {
                KeyGenerator generator = KeyGenerator.getInstance("AES");
                generator.init(config.getKeySize() > 0 ? config.getKeySize() : DEFAULT_KEY_BITS);
                SecretKey key = generator.generateKey();
                DataKey dataKey = new DataKey(file, source, logger, key, null);
                dataKey.wrap(masters.get(0));
                return dataKey;
            }
            byte[] wrapped = AtomicFiles.read(file);
            for (int i = 0; i < masters.size(); i++) {
                SecretKey key = unwrap(wrapped, masters.get(i));
                if (key != null) {
                    DataKey dataKey = new DataKey(file, source, logger, key, masters.get(i));
                    if (i > 0) {
                        logger.info("The master key of encryption at rest changed: wrapping the data key with the new one");
                        dataKey.wrap(masters.get(0));
                    }
                    return dataKey;
                }
            }
            throw new IllegalStateException("None of the master keys of the secure store decrypts the data key in "
                    + file + ": the key it was encrypted with is missing");
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Cannot read or create the data key in " + file + ": " + e.getMessage(), e);
        }
    }

    public SecretKey key() {
        return key;
    }

    /** Wraps the data key with the current master key if it changed (called periodically). */
    public synchronized void rotateIfNeeded() {
        byte[] current = masterKeys.keys().get(0);
        if (!Arrays.equals(current, wrappedWith)) {
            logger.info("The master key of encryption at rest changed: wrapping the data key with the new one");
            wrap(current);
        }
    }

    private void wrap(byte[] master) {
        try {
            byte[] nonce = new byte[NONCE_SIZE];
            new SecureRandom().nextBytes(nonce);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(master, "AES"), new GCMParameterSpec(TAG_BITS, nonce));
            cipher.updateAAD(AAD);
            byte[] encrypted = cipher.doFinal(key.getEncoded());
            ByteBuffer out = ByteBuffer.allocate(HEADER_SIZE + encrypted.length);
            out.putInt(MAGIC).putInt(FORMAT).put(nonce).put(encrypted);
            AtomicFiles.write(file, out.array());
            wrappedWith = master;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Cannot encrypt the data key: " + e.getMessage(), e);
        }
    }

    /** @return the data key, or {@code null} if {@code master} did not wrap it */
    private static SecretKey unwrap(byte[] wrapped, byte[] master) throws GeneralSecurityException {
        ByteBuffer in = ByteBuffer.wrap(wrapped);
        if (wrapped.length < HEADER_SIZE || in.getInt() != MAGIC || in.getInt() != FORMAT) {
            throw new GeneralSecurityException("Not a data key file");
        }
        byte[] nonce = new byte[NONCE_SIZE];
        in.get(nonce);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(master, "AES"), new GCMParameterSpec(TAG_BITS, nonce));
        cipher.updateAAD(AAD);
        try {
            return new SecretKeySpec(cipher.doFinal(wrapped, in.position(), in.remaining()), "AES");
        } catch (javax.crypto.AEADBadTagException e) {
            return null;
        }
    }
}
