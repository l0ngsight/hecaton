# Provenance of imported code

Hecaton is a fork of Hazelcast CE 5.7.0 (see [`changes-from-hazelcast.md`](changes-from-hazelcast.md)).
Code taken from other Hazelcast releases is listed here. All of it is Apache License 2.0; the
original copyright headers are kept, and the files went through `tools/rebrand` with the same
rename table as the fork.

| Imported | From | Commit | Files | Modifications |
|---|---|---|---|---|
| CP Subsystem: Raft engine, CP groups and sessions, `IAtomicLong`, `IAtomicReference`, `FencedLock`, `ISemaphore`, `ICountDownLatch`, their client message tasks (`hazelcast/src/main/java/com/hazelcast/cp/**`, 325 files missing from 5.7.0) | `hazelcast/hazelcast` tag `v5.4.0` | `3175d2c38b47a3de824c0072c1769a6fba22ff78` | 325 main | Renamed; `SuppressFBWarnings` annotations removed (Hecaton does not use SpotBugs annotations); adapted to the 5.7.0 CP API: three-argument `CPMemberInfo`, snapshot-chunk methods of `RaftStateStore`, `CPSubsystem#getCPDataStructureManagementService` (CPMap purge, see `docs/cp.md`), `CPSubsystemManagementService#getCurrentGroupsSnapshot` (empty). The 50 API files present in both releases keep their 5.7.0 versions. |
| `RaftInvocation`, `RaftInvocationContext` (`com/hazelcast/spi/impl/operationservice/impl`) | same | same | 2 main | Renamed. |
| CP message task registrations in `DefaultMessageTaskFactoryProvider` (`initializeCPGroupTaskFactories` … `initializeSemaphoreTaskFactories`) | same | same | 7 methods | Renamed; added to the 5.7.0 class. |
| CP entries of `META-INF/services/…DataSerializerHook` and `…ServiceDescriptorProvider` | same | same | 12 lines | Renamed. |
| CP client: proxies, `ClientRaftProxyFactory`, the client `CPSubsystemImpl` and session manager (`com/hazelcast/client/cp/**`) | same | same | 17 main | Renamed; the client `CPSubsystemImpl` is created by the client extension and initialised when the client starts; adapted to the 5.7.0 `ProxySessionManager` interface. CPMap support added (Hecaton code). |
| Management Center CP tasks: `GetCPMembersMessageTask`, `PromoteToCPMemberMessageTask`, `RemoveCPMemberMessageTask`, `ResetCPSubsystemMessageTask`, `ForceCloseCPSessionTask` (`com/hazelcast/client/impl/protocol/task/management`) | same | same | 5 main | Renamed. |
| CP tests (`hazelcast/src/test/java/com/hazelcast/cp/**`, `…/client/cp/**`, `RestCPSubsystemTest`) | same | same | 141 test | Renamed; adapted like the main code (casts to the session manager implementations, `InMemoryRaftStateStore` chunk methods, test factory access); tests that asserted CPMap is unavailable now test CPMap. |
