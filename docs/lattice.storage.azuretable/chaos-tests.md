# Chaos tests

The Azure Table WAL package has a focused chaos suite that exercises the real `AzureTableWalStorageProvider` against an Azurite-backed Azure Table endpoint. It complements the core [chaos tests](../lattice/chaos-tests.md) and the replication [chaos tests](../lattice.replication/chaos-tests.md) by proving the durable WAL backend preserves its storage invariants under concurrent append pressure.

Every suite here is tagged `[Category("Chaos")]`; the Azure-backed suite is also tagged `[Category("AzureStorageEmulator")]`.

```powershell
dotnet test --filter "TestCategory!=Chaos"
```

## Azure Table WAL suite (`test/lattice.storage.azuretable/Chaos/`)

`AzureTableWalChaosTests` drives concurrent append load across multiple shards against a real Azurite endpoint. The suite creates an isolated table per run, appends fixed-size batches from one writer per shard, reads each shard back after the workload, and verifies the WAL invariants that recovery and materialization rely on.

| Suite | What it proves |
|---|---|
| Sustained concurrent appends across shards | Parallel shard writers preserve dense per-shard offset namespaces, no duplicate offsets, monotone read order, and the expected highest offset after every append batch has completed. |

### Runtime characteristics

| Property | Azure Table WAL suite |
|---|---|
| Backend | Real Azurite emulator via `AzureTableWalStorageProvider` |
| Shards | 6 |
| Workload | 10 batches per shard, 4 entries per batch |
| Total entries | 240 |
| Writers | One writer per shard, all shards active concurrently |
| Validation | Full readback, duplicate detection, gap detection, monotone offset assertion, highest-offset assertion |
| Skip behaviour | Calls `Assert.Inconclusive` when the default Azurite development endpoint is not reachable |

The suite intentionally uses the public provider surface rather than a fake backend. It does not require the full replication pipeline; the goal is to isolate the Azure Table WAL storage contract: append-batch atomicity, monotone offset assignment, ordered readback, and correct tail reporting.

## Running locally

Start Azurite on the default development endpoint, then run the chaos category for the Azure Table test project:

```powershell
dotnet test test\lattice.storage.azuretable\Orleans.Lattice.Storage.AzureTable.Tests.csproj --filter "TestCategory=Chaos"
```

If Azurite is not reachable, the suite reports inconclusive instead of failing. This keeps normal developer machines from failing only because the emulator is absent.

## See also

- [Architecture](architecture.md) - storage layout, append pipeline, and recovery behaviour under test.
- [Configuration](configuration.md) - options that affect retry, pipelining, completion timeout, saturation handling, and compression.
- [Core WAL Storage Providers](../lattice/wal-storage-providers.md) - public provider seam and durable backend catalogue.
- [WAL tuning](../lattice/wal-tuning.md) - provider pressure, batching, and saturation envelope.
- [Replication chaos tests](../lattice.replication/chaos-tests.md) - cross-cluster and transport chaos suites that can use this provider as the durable WAL backend.
