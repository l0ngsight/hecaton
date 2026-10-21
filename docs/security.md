# TLS and security

Hecaton provides TLS (F1), authentication (F2) and client permissions (F3) in the core jar.
Configurations written for Hazelcast Enterprise work after `hecaton-migrate` has renamed them;
the class names Hecaton provides are listed in [`ee-class-map.md`](ee-class-map.md).

## TLS

Enable TLS for all connections of a member (unisocket), or per endpoint with advanced
networking. The default factory, `BasicSSLContextFactory`, reads key and trust stores:

```yaml
hecaton:
  network:
    ssl:
      enabled: true
      properties:
        keyStore: /opt/hecaton/member.p12
        keyStorePassword: changeit
        keyStoreType: PKCS12
        trustStore: /opt/hecaton/truststore.p12
        trustStorePassword: changeit
        trustStoreType: PKCS12
        mutualAuthentication: REQUIRED   # or OPTIONAL; omit for none
```

| Property | Meaning |
|---|---|
| `keyStore`, `keyStorePassword`, `keyStoreType` (default `JKS`, also reads PKCS#12), `keyManagerAlgorithm` | The certificate this side presents. A member needs one. |
| `trustStore`, `trustStorePassword`, `trustStoreType`, `trustManagerAlgorithm` | The certificates this side trusts; default: the JDK's trust store. |
| `protocol` | `SSLContext` protocol, default `TLS`. |
| `protocols`, `ciphersuites` | Comma-separated protocol versions and cipher suites to enable. By default the JDK's, without CBC cipher suites. |
| `mutualAuthentication` | `REQUIRED` or `OPTIONAL`: the accepting side asks for the connecting side's certificate. |
| `validateIdentity` | `true`: the connecting side checks that the certificate matches the host name or address it connects to. |
| `keyMaterialDuration` | ISO-8601 duration (e.g. `PT1H`): re-read the stores after that long, so renewed certificates are used without a restart. |
| `handshakeTimeoutSeconds` | Connections that do not complete the handshake within this time are closed (default 10). |

Clients set the same properties in `hecaton-client.network.ssl`. A client needs no key store
unless the member requires mutual authentication. Client-initiated TLS 1.2 renegotiation is
refused. A custom `SSLContextFactory` or `SSLEngineFactory` can be named with
`factory-class-name`.

The nightly `tls-conformance` workflow scans a TLS member with testssl.sh.

## Authentication

With `security.enabled: true`, members and clients authenticate through the realm configured
for them (`member-authentication`, `client-authentication`):

```yaml
hecaton:
  security:
    enabled: true
    realms:
      - name: memberRealm
        identity:
          username-password:
            username: member
            password: member-secret
      - name: clientRealm
        authentication:
          simple:
            users:
              - username: alice
                password: alice-secret
                roles: [admins]
    member-authentication:
      realm: memberRealm
    client-authentication:
      realm: clientRealm
```

| Authentication method | Who is authenticated |
|---|---|
| none (default) | Connecting members and clients must present this member's own identity (its member realm's `identity`), or no credentials if it has none; the cluster name is always checked. |
| `simple` | Users with passwords and roles listed in the configuration. |
| `tls` | The client certificate of a TLS connection with mutual authentication; the identity is the certificate subject, the roles the values of `roleAttribute` (default `cn`). |
| `jaas` | Your own JAAS login modules; extend `uk.org.hecaton.security.ClusterLoginModule`. |
| `ldap`, `kerberos` | Not available yet (planned for M9); a member with such a realm does not start. |

A member presents its member realm's `identity` when it joins; a member whose join is refused
shuts down.

## Client permissions

Clients may do only what their permissions allow. A permission names a type, an object name
(wildcards allowed), the principal (an identity or a role, `*` for all), optionally the client
addresses (`endpoints`), and the actions:

```yaml
hecaton:
  security:
    client-permissions:
      all:
        principal: admins
      map:
        - name: orders*
          principal: readers
          actions: [create, read]
        - name: secret
          principal: "*"
          deny: true
          actions: [all]
```

Deny permissions win over grants unless `client-permissions.priority-grant` is `true`. Permissions can be
changed at runtime (`Config.getSecurityConfig().setClientPermissionConfigs(...)` on a member),
and are synchronized when members join according to `client-permissions.on-join-operation`
(`RECEIVE`, the default: a joining member takes the cluster's; `SEND`: the cluster takes the
joining member's; `NONE`). A custom policy can be configured with `client-permission-policy`.

Security interceptors (`security-interceptors`) see every client call before and after it runs.

## Audit log

```yaml
hecaton:
  auditlog:
    enabled: true
    # factory-class-name: com.example.MyAuditlogServiceFactory
    # properties:
    #   logger: hecaton.auditlog
```

The audit log (F4) records security-relevant events: client, member and REST authentication
(with the outcome; passwords and tokens are never logged), connections, members joining, leaving,
promoted or suspected, cluster shutdown and merge, logging-level and diagnostics changes, and WAN
configuration changes. Each event has a type ID (`HZ-0501` client authentication, ...; see
`AuditlogTypeIds`), a message, parameters and a timestamp. By default the events are written to the
logger `hecaton.auditlog` (the `logger` property changes it), so the logging configuration decides
where they go; a custom `AuditlogServiceFactory` can send them elsewhere.

## Socket interceptor

A socket interceptor (F5) runs custom code on every new connection before the protocol starts,
for example to exchange a token with the other side. Configure an implementation of
`MemberSocketInterceptor` on members (`network` or each advanced-network endpoint) and of
`SocketInterceptor` on clients:

```yaml
hecaton:
  network:
    socket-interceptor:
      enabled: true
      class-name: com.example.TokenInterceptor
      properties:
        token: secret
```

`init(properties)` is called once; `onConnect(socket)` when the member or client opens a
connection, `onAccept(socket)` when a member accepts one; throwing an `IOException` closes the
connection. Interceptors see the raw socket: with TLS they run before the TLS handshake.

## Limitations

- Code that runs on members (executor tasks submitted by clients, entry processors) is not
  checked against the submitting client's permissions; `client-block-unmapped-actions` has no
  effect yet.
- LDAP and Kerberos authentication: planned for M9.
- No OpenSSL engine factory; TLS uses the JDK.
