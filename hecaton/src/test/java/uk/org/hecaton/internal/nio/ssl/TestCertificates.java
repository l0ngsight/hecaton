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

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Creates a certificate authority and certificates signed by it, stored in PKCS#12 files, for
 * the TLS tests.
 */
public final class TestCertificates {

    /** The password of every store this class writes. */
    public static final String PASSWORD = "secret";

    private static final AtomicLong SERIAL = new AtomicLong(System.currentTimeMillis());

    private final Path directory;
    private final KeyPair caKeys;
    private final X509Certificate caCertificate;

    /** A new certificate authority whose files go to {@code directory}. */
    public TestCertificates(Path directory, String caName) throws Exception {
        this.directory = directory;
        this.caKeys = newKeyPair();
        X500Name name = new X500Name("CN=" + caName);
        X509v3CertificateBuilder builder = builder(name, name, caKeys);
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        this.caCertificate = sign(builder, caKeys);
    }

    /** A trust store with this authority's certificate. */
    public Path trustStore() throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setCertificateEntry("ca", caCertificate);
        return write(store, "truststore-" + SERIAL.incrementAndGet() + ".p12");
    }

    /**
     * A key store with a new key and a certificate for {@code commonName} signed by this
     * authority, valid for the given IP addresses and DNS names.
     */
    public Path keyStore(String commonName, String... ipAddressesAndDnsNames) throws Exception {
        KeyPair keys = newKeyPair();
        X509v3CertificateBuilder builder = builder(new X500Name("CN=" + commonName),
                new X500Name(caCertificate.getSubjectX500Principal().getName()), keys);
        GeneralName[] names = new GeneralName[ipAddressesAndDnsNames.length];
        for (int i = 0; i < names.length; i++) {
            String value = ipAddressesAndDnsNames[i];
            names[i] = Character.isDigit(value.charAt(0))
                    ? new GeneralName(GeneralName.iPAddress, value)
                    : new GeneralName(GeneralName.dNSName, value);
        }
        if (names.length > 0) {
            builder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(names));
        }
        X509Certificate certificate = sign(builder, caKeys);
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setKeyEntry("key", keys.getPrivate(), PASSWORD.toCharArray(),
                new Certificate[]{certificate, caCertificate});
        return write(store, "keystore-" + commonName + "-" + SERIAL.incrementAndGet() + ".p12");
    }

    private Path write(KeyStore store, String fileName) throws Exception {
        Path file = directory.resolve(fileName);
        try (OutputStream out = Files.newOutputStream(file)) {
            store.store(out, PASSWORD.toCharArray());
        }
        return file;
    }

    private static X509v3CertificateBuilder builder(X500Name subject, X500Name issuer, KeyPair keys) {
        Instant now = Instant.now();
        return new JcaX509v3CertificateBuilder(issuer, BigInteger.valueOf(SERIAL.incrementAndGet()),
                Date.from(now.minus(Duration.ofDays(1))), Date.from(now.plus(Duration.ofDays(30))),
                subject, keys.getPublic());
    }

    private static X509Certificate sign(X509v3CertificateBuilder builder, KeyPair signer) throws Exception {
        ContentSigner contentSigner = new JcaContentSignerBuilder("SHA256withECDSA").build(signer.getPrivate());
        return new JcaX509CertificateConverter().getCertificate(builder.build(contentSigner));
    }

    private static KeyPair newKeyPair() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
        generator.initialize(256);
        return generator.generateKeyPair();
    }
}
