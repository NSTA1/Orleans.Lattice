# VehicleFleetSimulator

A simulated vehicle fleet that streams structured telemetry events over gRPC. Imported into this repo to drive forthcoming WAL benchmarks for `Orleans.Lattice` and `Orleans.Lattice.Replication`, and as the foundation for a future sample that bridges the simulator's event stream into a Lattice tree.

[`samples/VehicleFleetSimulator`](../VehicleFleetSimulator/) (this folder)

## Status

- **Independent.** The simulator does not currently depend on `Orleans.Lattice` or `Orleans.Lattice.Replication`. It is its own multi-project Orleans application with its own `VehicleFleetSimulator.slnx`. Building or running it has no impact on the lattice library and vice versa.
- **Imported as-is.** The source is a near-verbatim copy of the upstream simulator. The only modification on import was the addition of `[assembly: CollectionBehavior(DisableTestParallelization = true)]` in `tests/VehicleFleetSimulator.Tests/ClusterFixture.cs` to remove an xUnit collection-parallelism flake under load. See "Test parallelism" below.
- **Forward-looking.** A future sample (likely `samples/VehicleFleetSimulator.LatticeBridge/` or similar) will subscribe to the simulator's gRPC telemetry stream and project events into a Lattice tree, exercising replication WAL throughput end-to-end.

## What it does

The simulator stands up a four-tier local stack:

| Tier | Project | Purpose |
|------|---------|---------|
| Storage | Azurite (container) | Backing store for Orleans clustering, grain persistence, and stream queues. |
| Silo | `VehicleFleetSimulator.Silo` | Hosts vehicle grains, the city graph, the fan-out telemetry sink, and reminder-driven simulation ticks. |
| API | `VehicleFleetSimulator.Api` | ASP.NET Core gRPC + gRPC-Web frontend. Streams telemetry to subscribers; accepts admin commands. |
| UI | `VehicleFleetSimulator.Ui` | Blazor WebAssembly dashboard — live map, simulation-speed slider, fleet controls. |

A pluggable `ITelemetrySink` lets the silo route events to fan-out grains, a null sink, or (future) a Lattice-bridge sink. `VehicleSimulator` advances each vehicle along a city graph; `FuelModel` and `SpeedModel` are pure functions over each tick. Routes are generated on demand by `RouteGenerator`. All vehicle state is grain-resident; Azurite holds only Orleans plumbing.

## Running it

The full stack runs under Docker Compose:

```shell
./samples/VehicleFleetSimulator/run.ps1
```

The script wipes the Azurite volume, builds any stale images, and starts Azurite + Silo + API + UI in detached mode. UI is served on `http://localhost:8090`; API gRPC endpoint is `http://localhost:8080`.

```shell
./samples/VehicleFleetSimulator/run.ps1 -Down
```

Tears the stack down and removes the Azurite volume.

## Test parallelism

`tests/VehicleFleetSimulator.Tests` shares a single Orleans `TestCluster` across collection-bound tests. Six pure-unit-test classes carry no `[Collection]` attribute and were running in parallel with the cluster-fixture tests by default — that races `StreamSubscriberOrderTests` against its 15s telemetry-collection budget under load. The on-import patch:

```csharp
[assembly: CollectionBehavior(DisableTestParallelization = true)]
```

is added to `ClusterFixture.cs`. The whole suite runs in ~4s single-threaded, so there's no observable cost. CI's samples-only fast path (see `.github/workflows/ci.yml`) discovers this test project automatically and runs it after building the slnx.
