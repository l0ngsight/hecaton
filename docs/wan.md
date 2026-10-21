# WAN replication

WAN replication (F13) copies the changes made to IMaps and ICaches in one cluster to other
clusters, asynchronously, typically across data centres. Hecaton implements it in core with the
configuration, API and management operations Hazelcast has: `WanReplicationConfig`, the batch
publisher (`WanBatchPublisherConfig`), `wan-replication-ref` on maps and caches, and the WAN
operations of REST, Management Center and `WanReplicationService`. The wire protocol between
clusters is Hecaton's own; a Hazelcast cluster cannot be a source or target (spec NG2).

## Configuration

On the source cluster, a WAN replication scheme with a batch publisher per target cluster, and the
structures that use it:

```yaml
hecaton:
  wan-replication:
    to-london:
      batch-publisher:
        london:                         # publisher id; defaults to the cluster name
          cluster-name: london
          target-endpoints: 10.0.1.10:5701,10.0.1.11:5701
          batch-size: 500
          batch-max-delay-millis: 1000
          response-timeout-millis: 60000
          acknowledge-type: ACK_ON_OPERATION_COMPLETE   # or ACK_ON_RECEIPT
          queue-capacity: 10000
          queue-full-behavior: DISCARD_AFTER_MUTATION   # or THROW_EXCEPTION,
                                                        #    THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE
          initial-publisher-state: REPLICATING          # or PAUSED, STOPPED
  map:
    orders:
      wan-replication-ref:
        to-london:
          merge-policy-class-name: PassThroughMergePolicy
  cache:
    sessions:
      wan-replication-ref:
        to-london:
          merge-policy-class-name: PassThroughMergePolicy
```

The target cluster needs no WAN configuration: its members accept WAN batches on their member
port, or on a WAN endpoint of the advanced network:

```yaml
hecaton:
  advanced-network:
    enabled: true
    wan-server-socket-endpoint-config:
      wan-in:
        port:
          port: 11010
```

A source member using the advanced network names its outgoing endpoint with the publisher's
`endpoint` (a `wan-endpoint-config`), which carries its TLS and socket settings.

Instead of `target-endpoints`, a publisher can find the target members with the Discovery SPI
(`discovery-strategies`, or the Kubernetes discovery section), refreshed every
`discovery-period-seconds`; `max-target-endpoints` limits the number of endpoints used and
`use-endpoint-private-address` picks the private addresses.

Two clusters can replicate to each other (active/active): each has a scheme publishing to the
other, and the merge policy decides between concurrent changes. Changes applied by WAN replication
are not replicated again.

## How it works

- **Queues.** A change to a replicated entry adds an event to the publisher's queue for its
  partition, on the partition's owner and on its backups, so a member failure loses no events:
  the new owner sends the events its backup copy holds. Queues move with their partitions on
  migration.
- **Batches.** One thread per publisher on every member collects up to `batch-size` events (or
  what there is after `batch-max-delay-millis`) from the partitions the member owns and sends
  them to one target endpoint. Only once the target acknowledges are the events removed from the
  queue and from the backups; one batch at a time keeps the order of each partition's events.
- **Acknowledgement.** With `ACK_ON_OPERATION_COMPLETE` the target acknowledges once the events
  have been applied; with `ACK_ON_RECEIPT`, once they have arrived.
- **Failures.** A batch that fails or is not acknowledged within `response-timeout-millis` is sent
  again, to the next endpoint. Events are therefore delivered at least once.
- **Applying.** The target applies each event with the structure's merge policy
  (`wan-replication-ref`'s `merge-policy-class-name`). With `persist-wan-replicated-data` on the
  target scheme's `consumer` section, applied entries are also written to a `MapStore` or cache
  writer. A `consumer` with a `class-name` (a `WanConsumer`) is created on every member once it
  has started and shut down with it.
- **Full queues.** When the queue holds `queue-capacity` events, `DISCARD_AFTER_MUTATION` drops
  new events, `THROW_EXCEPTION` fails the change with `WanQueueFullException`, and
  `THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE` does so only while the publisher is replicating.

## Managing replication

| Operation | API (`WanReplicationService` on a member) | REST (`WAN` endpoint group) | Management Center |
|---|---|---|---|
| Pause (queue events, send none) | `pause` | `/hecaton/rest/wan/pausePublisher` | yes |
| Stop (drop new events) | `stop` | `/hecaton/rest/wan/stopPublisher` | yes |
| Resume | `resume` | `/hecaton/rest/wan/resumePublisher` | yes |
| Clear the queues | `removeWanEvents` | `/hecaton/rest/wan/clearWanQueues` | yes |
| Add a scheme or publishers | `addWanReplicationConfig`, `Config#addWanReplicationConfig` | `/hecaton/rest/wan/addWanConfig` | yes |

Pause, stop, resume and clear act on one member; adding a configuration acts on the whole cluster.
A scheme added at runtime is kept in the dynamic configuration, so members that join later have
it; publishers added to an existing scheme reach the current members only. Maps and caches whose
`wan-replication-ref` names a scheme that does not exist yet start replicating once it is added.

The publishers' state, outbound queue size and sent/dropped event counts are in
`LocalWanStats` (Management Center's WAN view) and in the metrics: `wan[<scheme>]` tagged with
`publisherId`, with `outboundQueueSize`, `totalPublishedEventCount` and `totalPublishLatency`,
and `updateCount`, `removeCount` and `droppedCount` per `map` or `cache` tag.

## Limitations

- WAN synchronization (full and Merkle-tree delta sync) and consistency checks (F13, P1) are
  planned for M9; they fail with a message naming the milestone.
- Batches are sent one at a time per publisher and member (`max-concurrent-invocations` is
  ignored), and events are not coalesced per key (`snapshot-enabled` is ignored).
- Only IMap and ICache are replicated, as in Hazelcast.

## Testing

`WanBatchReplicationTest` (active/passive and active/active clusters, member failure, pause/stop,
full queues, caches, configuration added at runtime, custom consumers, metrics) and
`WanNetworkReplicationTest` (clusters over TCP, WAN endpoints of the advanced network, failover to
the next endpoint).
