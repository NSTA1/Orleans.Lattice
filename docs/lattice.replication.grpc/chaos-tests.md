# Chaos tests

The gRPC transport package has a focused chaos suite for the network binding between the replication shipper and receiver endpoint. It complements the broader replication [chaos tests](../lattice.replication/chaos-tests.md), which cover multi-cluster convergence, WAL trim, liveness, and Azure Table WAL durability.

Every suite is tagged `[Category("Chaos")]`, so it is excluded from the fast iterative loop:

```powershell
dotnet test --filter "TestCategory!=Chaos"
```

## gRPC transport suite (`test/lattice.replication.grpc/Chaos/`)

`GrpcTransportChaosTests` hosts a real `MapLatticeReplicationGrpc` receiver on ASP.NET Core test infrastructure, sends batches through the public `IReplicationTransport` seam, injects channel faults, retries at the caller boundary, and asserts that every distinct key reaches the receiver exactly once in the applied-key set.

| Test | Fault model | What it proves |
|---|---|---|
| `Sender_retry_loop_under_15pct_channel_faults_delivers_every_entry` | Per-call HTTP request faults with 15 percent probability. | A moderate flapping channel converges after bounded caller retries. No shipped key is lost. |
| `Sender_retry_loop_under_30pct_channel_faults_delivers_every_entry` | Per-call HTTP request faults with 30 percent probability. | A severe flapping channel still converges within the retry budget. Receiver idempotency absorbs redelivery. |

### Runtime shape

| Property | Value |
|---|---|
| Receiver | ASP.NET Core test host with `AddLatticeReplicationGrpc` and `MapLatticeReplicationGrpc` |
| Sender path | Public `IReplicationTransport.SendAsync` contract |
| Workload | 10 batches, 8 records per batch |
| Fault injection | Per-call `HttpRequestException` from a test `DelegatingHandler` |
| Retry budget | 40 attempts per batch |
| Assertion | All 80 distinct keys are present in the receiver-side applied set |

### Invariants under test

1. **No batch loss.** Every record the sender keeps retrying is eventually observed by the receiver within the bounded attempt budget.
2. **No duplicate apply effect.** Redeliveries are expected under fault injection, but the receiver-side high-water-mark behaviour collapses them to one applied-key observation.
3. **Non-vacuous faults.** Each test asserts that at least one channel fault was injected before accepting the run.
4. **Real endpoint mapping.** The receiver path uses the public endpoint mapping helper rather than a fake transport.

## See also

- [Architecture](architecture.md) - the sender, endpoint, applier, and ack topology.
- [API Reference](api.md) - public registration helpers and transport seams.
- [Replication Apply](../lattice.replication/replication-apply.md) - high-water-mark dedup and causal apply.
- [Core chaos tests](../lattice/chaos-tests.md) - the single-cluster and cross-package chaos overview.
