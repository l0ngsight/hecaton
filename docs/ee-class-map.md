# Hazelcast Enterprise class names in Hecaton

Configurations written for Hazelcast Enterprise name some classes that Hecaton provides itself
(spec §5.2, F2.2). After `hecaton-migrate` renames `com.hazelcast` to `uk.org.hecaton`, these
names resolve to Hecaton's own implementations. The implementations are written from Hazelcast's
public documentation only (clean-room policy, CONTRIBUTING §1); their behaviour is described here.

| Name in a Hazelcast Enterprise configuration | Hecaton class | Since | Notes |
|---|---|---|---|
| `com.hazelcast.nio.ssl.BasicSSLContextFactory` | `uk.org.hecaton.nio.ssl.BasicSSLContextFactory` | M3 | The default SSL factory when `factory-class-name` is not set. Properties: `keyStore`, `keyStorePassword`, `keyStoreType` (default `JKS`, which also reads PKCS#12), `keyManagerAlgorithm`, `trustStore`, `trustStorePassword`, `trustStoreType`, `trustManagerAlgorithm`, `protocol` (default `TLS`), `keyMaterialDuration` (ISO-8601, e.g. `PT1H`: re-read the stores after that long, for certificate renewal without restart). Per connection: `ciphersuites`, `protocols`, `mutualAuthentication` (`REQUIRED`/`OPTIONAL`), `validateIdentity`; `handshakeTimeoutSeconds` (Hecaton, default 10) closes connections that do not complete the handshake. A member needs `keyStore`. |
| `com.hazelcast.security.loginimpl.DefaultLoginModule` | `uk.org.hecaton.security.loginimpl.DefaultLoginModule` | M3 | Realms without an authentication method: the connecting side must present this member's own identity (member realm), or nothing if the member has none. |
| `com.hazelcast.security.loginimpl.SimplePropertiesLoginModule` | `uk.org.hecaton.security.loginimpl.SimplePropertiesLoginModule` | M3 | `simple-authentication`: options `password.<user>`, `roles.<user>`, `roleSeparator`. |
| `com.hazelcast.security.loginimpl.X509CertificateLoginModule` | `uk.org.hecaton.security.loginimpl.X509CertificateLoginModule` | M3 | `tls` authentication: identity is the client certificate's subject, roles the values of `roleAttribute` (default `cn`). Needs TLS with mutual authentication. |
| `com.hazelcast.security.ClusterLoginModule`, `CredentialsCallback`, `EndpointCallback`, `CertificatesCallback`, `ClusterNameCallback`, `ClusterIdentityPrincipal`, `ClusterRolePrincipal`, `ClusterEndpointPrincipal` | same names in `uk.org.hecaton.security` | M3 | For custom JAAS login modules: extend `ClusterLoginModule`, implement `onLogin()` and `getName()`, add roles with `addRole`. |
| `com.hazelcast.security.impl.DefaultPermissionPolicy` | `uk.org.hecaton.security.impl.DefaultPermissionPolicy` | M3 | The client permission policy unless `client-permission-policy` names another. |

| `com.hazelcast.security.loginimpl.BasicLdapLoginModule`, `LdapLoginModule` | same names in `uk.org.hecaton.security.loginimpl` | M9 | `ldap` realms without and with a system account; see [security.md](security.md). |
| `com.hazelcast.security.loginimpl.GssApiLoginModule` | `uk.org.hecaton.security.loginimpl.GssApiLoginModule` | M9 | `kerberos` realms. |
| `com.hazelcast.security.impl.KerberosCredentialsFactory` | `uk.org.hecaton.security.impl.KerberosCredentialsFactory` | M9 | `kerberos` identities of clients and members. |

Not provided yet: `com.hazelcast.nio.ssl.OpenSSLEngineFactory` (optional netty-tcnative).
