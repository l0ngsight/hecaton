# Security review

Hecaton 1.0 needs an external review of the code that protects a cluster (plan, Phase 8). This
page is the brief for the reviewers: what to look at, how it is meant to work, and what has been
checked already. Report findings privately as [SECURITY.md](../SECURITY.md) describes.

## Threat model

A Hecaton cluster runs on a network that other parties can reach. With TLS and security
enabled ([security.md](security.md)):

- Only members with valid credentials (member realm, or a trusted certificate with mutual TLS)
  join; only authenticated clients connect, and they do only what their permissions allow.
- Traffic between members, clients and clusters cannot be read or changed.
- Code running on members (entry processors, executors, Jet jobs) is trusted: security applies
  to client operations, not to member-side code (as in Hazelcast).

Without security, any peer that can reach a member port has full access; this is the default and
the documentation says so.

## In scope

| Area | Code | What to check |
|---|---|---|
| TLS (F1) | `uk.org.hecaton.internal.nio.ssl` (`TlsChannelInitializer`, `TlsDecoder`, `TlsEncoder`, `TlsHandshake`, `SSLEngineFactories`), `nio.ssl.BasicSSLContextFactory`, client TLS in `HecatonClientExtension` | Handshake state machine and timeouts; plaintext before the handshake completes; renegotiation; cipher and protocol defaults (no CBC suites unless asked, TLS 1.2+); mutual authentication; hostname validation (`validateIdentity`); key store reload (`keyMaterialDuration`) |
| Authentication (F2) | `uk.org.hecaton.security.impl` (`SecurityContextImpl`, `RealmLoginConfiguration`, `HecatonLoginContext`), `security.loginimpl`, `AuthenticationBaseMessageTask`, member join (`MemberIdentityCallback`) | Credential checks of every realm type; failure handling and timing; identities on member join; replay of authentication messages |
| Authorization (F3) | `security.impl.DefaultPermissionPolicy`, `SecureCallableImpl`, `PermissionSyncService`, the `getRequiredPermission` of client message tasks | Every client message task checks a permission; wildcard and deny rules; permission changes at runtime; Jet, SQL and user code namespace permissions |
| REST | `internal.ascii.rest`, `RestApiFilter` | Endpoint groups off by default; authentication of the management operations (cluster state, persistence, WAN) |
| WAN (F13) | `wan.impl.batch.WanBatchOperation`, WAN endpoints of the advanced network | A target accepts WAN batches from any peer that completes the member handshake on the endpoint; the cluster name in a batch is not authentication. The documented setup is a dedicated WAN endpoint with mutual TLS: check that this is sufficient |
| Persistence (F10) | `hecaton-persistence` (`MemberDirectory`, `DataStore`, `store`) | Directory locking and permissions; handling of corrupted or hostile files in `base-dir` (records are checksummed; a corrupted closed chunk stops the member) |
| Native memory (F11) | `hecaton-hd` (`memory`, `table`) | Bounds of native accesses (FFM segments are bounds-checked; raw address arithmetic in the hash tables) |
| CP Subsystem (F7) | `uk.org.hecaton.cp` (ported from Hazelcast 5.4.0) | CP member authentication follows member authentication |

Out of scope: code inherited unchanged from Hazelcast CE 5.7 other than the entry points above
(it is maintained as Hazelcast's was), and the Jet connectors.

## Checked so far

- **Static analysis:** the `Security analysis` workflow (`.github/workflows/security.yml`) runs
  CodeQL (security-extended queries), the OWASP dependency check and SpotBugs with the FindSecBugs
  rules on `hecaton`, `hecaton-hd` and `hecaton-persistence` weekly. The SpotBugs findings of
  `hecaton-hd` and `hecaton-persistence` were triaged for 1.0: none is a defect (partition-thread
  confinement the analysis cannot see, intended `null` results, path findings on paths from the
  member's own configuration).
- **TLS conformance:** `tls-conformance.yml` runs `testssl.sh` against member and client
  endpoints nightly.
- **Tests:** `SecurityTest` sends fuzzed authentication messages; the permission semantics are
  covered per permission type; TLS and security are exercised with the official clients.
