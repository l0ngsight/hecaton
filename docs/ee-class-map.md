# Hazelcast Enterprise class names in Hecaton

Configurations written for Hazelcast Enterprise name some classes that Hecaton provides itself
(spec §5.2, F2.2). After `hecaton-migrate` renames `com.hazelcast` to `uk.org.hecaton`, these
names resolve to Hecaton's own implementations. The implementations are written from Hazelcast's
public documentation only (clean-room policy, CONTRIBUTING §1); their behaviour is described here.

| Name in a Hazelcast Enterprise configuration | Hecaton class | Since | Notes |
|---|---|---|---|
| `com.hazelcast.nio.ssl.BasicSSLContextFactory` | `uk.org.hecaton.nio.ssl.BasicSSLContextFactory` | M3 | The default SSL factory when `factory-class-name` is not set. Properties: `keyStore`, `keyStorePassword`, `keyStoreType` (default `JKS`, which also reads PKCS#12), `keyManagerAlgorithm`, `trustStore`, `trustStorePassword`, `trustStoreType`, `trustManagerAlgorithm`, `protocol` (default `TLS`). Per connection: `ciphersuites`, `protocols`, `mutualAuthentication` (`REQUIRED`/`OPTIONAL`), `validateIdentity`. A member needs `keyStore`. |

Not provided yet: `com.hazelcast.nio.ssl.OpenSSLEngineFactory` (planned with F1, optional
netty-tcnative), the login modules and credential factories (F2, M3).
