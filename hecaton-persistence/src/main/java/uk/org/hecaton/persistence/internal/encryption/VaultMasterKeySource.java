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

import uk.org.hecaton.config.VaultSecureStoreConfig;
import uk.org.hecaton.internal.json.Json;
import uk.org.hecaton.internal.json.JsonObject;
import uk.org.hecaton.internal.json.JsonValue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Master keys from a HashiCorp Vault secret ({@code secret-path}, read with {@code token}): the secret
 * holds one entry whose value is the Base64-encoded key. With a KV version 2 engine, the previous
 * versions of the secret are the previous keys (up to {@value #MAX_PREVIOUS_VERSIONS} of them).
 */
class VaultMasterKeySource implements MasterKeySource {

    private static final int MAX_PREVIOUS_VERSIONS = 10;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int HTTP_OK = 200;

    private final VaultSecureStoreConfig config;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();

    VaultMasterKeySource(VaultSecureStoreConfig config) {
        this.config = config;
    }

    @Override
    public List<byte[]> keys() {
        List<byte[]> keys = new ArrayList<>();
        JsonObject data = read(null);
        keys.add(key(data));
        JsonValue metadata = data.get("metadata");
        if (metadata != null && metadata.isObject()) {
            int version = metadata.asObject().getInt("version", 1);
            for (int previous = version - 1; previous >= 1 && previous >= version - MAX_PREVIOUS_VERSIONS; previous--) {
                JsonObject old = read(previous);
                if (old != null) {
                    keys.add(key(old));
                }
            }
        }
        return keys;
    }

    /** The {@code data} object of the secret (of a version), or {@code null} if that version is gone. */
    private JsonObject read(Integer version) {
        String address = config.getAddress().endsWith("/") ? config.getAddress() : config.getAddress() + "/";
        String uri = address + "v1/" + config.getSecretPath() + (version == null ? "" : "?version=" + version);
        HttpRequest request = HttpRequest.newBuilder(URI.create(uri)).timeout(TIMEOUT)
                .header("X-Vault-Token", config.getToken()).GET().build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != HTTP_OK) {
                if (version != null) {
                    return null;
                }
                throw new IllegalStateException("Vault answered " + response.statusCode() + " for " + config.getSecretPath());
            }
            JsonValue data = Json.parse(response.body()).asObject().get("data");
            if (data == null || !data.isObject()) {
                throw new IllegalStateException("The Vault secret " + config.getSecretPath() + " has no data");
            }
            return data.asObject();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read the Vault secret " + config.getSecretPath() + ": " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while reading the Vault secret", e);
        }
    }

    /** The key in a secret's data: KV version 2 nests the entries in another {@code data} object. */
    private byte[] key(JsonObject data) {
        JsonValue nested = data.get("data");
        JsonObject entries = nested != null && nested.isObject() ? nested.asObject() : data;
        if (entries.size() != 1 || !entries.get(entries.names().get(0)).isString()) {
            throw new IllegalStateException("The Vault secret " + config.getSecretPath()
                    + " must have exactly one entry, the Base64-encoded key");
        }
        return Base64.getDecoder().decode(entries.get(entries.names().get(0)).asString());
    }
}
