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
package uk.org.hecaton.security;

import javax.security.auth.callback.Callback;
import java.security.cert.Certificate;

/**
 * Gives a login module the certificate chain the connecting side presented in the TLS
 * handshake, or {@code null} if it presented none (or the connection does not use TLS).
 */
public class CertificatesCallback implements Callback {

    private Certificate[] certificates;

    public Certificate[] getCertificates() {
        return certificates;
    }

    public void setCertificates(Certificate[] certificates) {
        this.certificates = certificates;
    }
}
