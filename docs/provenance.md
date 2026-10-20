# Provenance of imported code

Hecaton is a fork of Hazelcast CE 5.7.0 (see [`changes-from-hazelcast.md`](changes-from-hazelcast.md)).
Code taken from other Hazelcast releases is listed here. All of it is Apache License 2.0; the
original copyright headers are kept, and the files went through `tools/rebrand` with the same
rename table as the fork.

| Imported | From | Commit | Files | Modifications |
|---|---|---|---|---|
| CP Subsystem: Raft engine, CP groups and sessions, `IAtomicLong`, `IAtomicReference`, `FencedLock`, `ISemaphore`, `ICountDownLatch`, their client message tasks (`hazelcast/src/main/java/com/hazelcast/cp/**`, 325 files missing from 5.7.0) | `hazelcast/hazelcast` tag `v5.4.0` | `3175d2c38b47a3de824c0072c1769a6fba22ff78` | 325 main | Renamed; `SuppressFBWarnings` annotations removed (Hecaton does not use SpotBugs annotations); adapted to the 5.7.0 CP API: three-argument `CPMemberInfo`, snapshot-chunk methods of `RaftStateStore`, `CPSubsystem#getCPDataStructureManagementService` (CPMap purge refused until F8), `CPSubsystemManagementService#getCurrentGroupsSnapshot` (empty). The 50 API files present in both releases keep their 5.7.0 versions. |
| `RaftInvocation`, `RaftInvocationContext` (`com/hazelcast/spi/impl/operationservice/impl`) | same | same | 2 main | Renamed. |
| CP message task registrations in `DefaultMessageTaskFactoryProvider` (`initializeCPGroupTaskFactories` … `initializeSemaphoreTaskFactories`) | same | same | 7 methods | Renamed; added to the 5.7.0 class. |
| CP entries of `META-INF/services/…DataSerializerHook` and `…ServiceDescriptorProvider` | same | same | 12 lines | Renamed. |
