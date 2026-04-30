# Orleans.Lattice — Benchmark Stack

End-to-end benchmark suite covering the 12 scenarios in [`benchmark-plan.md`](./benchmark-plan.md).

The stack is brought up via `docker compose` and driven through the Vehicle Fleet
Simulator's HTTP API (`samples/VehicleFleetSimulator/`). A single PowerShell script
selects the scenario and runs end-to-end:

```powershell
./benchmark.ps1 B-03
```

## Layout

```
benchmark/
├── benchmark.ps1                    # Single-parameter runner (B-01 .. B-12).
├── benchmark-plan.md                # Authoritative scenario plan.
├── docker-compose.yml               # Base topology (single cluster).
├── docker-compose.replication.yml   # Replication overlay (B-04, B-06, B-07, B-08, B-09).
├── host/
│   ├── Bench.Sink/                  # LatticeSink — bounded-channel ITelemetrySink.
│   └── Bench.Silo/                  # Benchmark silo: env-driven sink switch + Lattice/Replication.
├── scenarios/B-XX.env               # Per-scenario configuration knobs.
├── prometheus/prometheus.yml        # Scrape config (single cluster).
├── prometheus/prometheus-replication.yml
└── grafana/
    ├── provisioning/                # Datasource + dashboards provider yaml.
    └── dashboards/                  # Dashboards copied from src/lattice.dashboards/Grafana/.
```

The benchmark stack does **not** modify the simulator. `host/Bench.Silo/` is a separate
silo project that references `samples/VehicleFleetSimulator/src/VehicleFleetSimulator.Grains`
and `.Abstractions` unmodified, plus the new `host/Bench.Sink/` (the LatticeSink) and the
core lattice projects under `src/`. The simulator's existing API project is reused
verbatim — the benchmark `docker-compose.yml` invokes its unmodified Dockerfile from the
simulator-local context.

## Topology

```
┌─────────────────────────────────────────────────────────────┐
│  vfs-azurite        →  Orleans clustering / reminders        │
│  vfs-silo           →  Bench.Silo + LatticeSink + :9090      │
│  vfs-api            →  ASP.NET Core HTTP load surface :8080  │
│                        (simulator's existing API, unchanged) │
│  vfs-prometheus     →  scrapes silo:9090/metrics             │
│  vfs-grafana        →  Orleans.Lattice dashboards on :3000   │
└─────────────────────────────────────────────────────────────┘
```

For replication scenarios (`B-04`, `B-06`, `B-07`, `B-08`, `B-09`),
`docker-compose.replication.yml` adds a second silo cluster
(`vfs-silo-replica` + `vfs-azurite-replica`).

## Scenarios

| Id    | Description                                    | Replication | Chaos      |
|-------|------------------------------------------------|-------------|------------|
| B-01  | Simulator baseline (NullTelemetrySink)         | n/a         | none       |
| B-02  | `ILattice` micro-benchmark (harness-only)      | n/a         | n/a        |
| B-03  | Current-state tree, replication off            | off         | none       |
| B-04  | Current-state tree, single-peer replication    | on          | none       |
| B-05  | Skewed-key variant (adaptive shard splits)     | off         | none       |
| B-06  | Replication backpressure / catch-up            | on          | pause      |
| B-07  | Receiver crash mid-stream                      | on          | kill       |
| B-08  | Two-cluster bidirectional replication          | on (both)   | none       |
| B-09  | Per-key replication filter cost                | on          | none       |
| B-10  | Event-log tree with TTL                        | off         | none       |
| B-11  | Streaming bulk-load ingest variant             | off         | none       |
| B-12  | Observer-off control (paired with B-04)        | off         | none       |

Per-scenario knobs live in `scenarios/B-XX.env`. Each file sets:

| Variable                       | Purpose                                                  |
|--------------------------------|----------------------------------------------------------|
| `BENCH_TELEMETRY_SINK`         | `null` \| `fanout` \| `lattice` (silo's `Telemetry:Sink`) |
| `BENCH_KEY_SHAPE`              | `CurrentStateByVehicleId` \| `RegionPrefixedVehicleId` \| `EventLogTimestamped` |
| `BENCH_BULK_LOAD`              | `true` (B-11) — drain via `BulkLoadAsync`                 |
| `BENCH_EVENT_LOG_TTL`          | TTL applied via `SetAsync(ttl)` for the event-log shape  |
| `BENCH_REPLICATION_ENABLED`    | `true` to call `AddLatticeReplication` on the silo       |
| `BENCH_REPLICATION_OVERLAY`    | `true` to bring up the replica cluster                    |
| `BENCH_REPLICATION_KEY_PREFIXES`| Comma-separated prefix filter (B-09)                     |
| `BENCH_FLEET_SIZE`             | Number of vehicles to seed                                |
| `BENCH_WARMUP_SECONDS`         | Settle time before measurement                            |
| `BENCH_DURATION_SECONDS`       | Measurement window                                        |
| `BENCH_CHAOS`                  | `none` \| `pause` \| `kill` (B-06, B-07)                  |
| `BENCH_CHAOS_TARGET`           | Compose service name to apply chaos to                    |
| `BENCH_CHAOS_AFTER_SECONDS`    | Delay before chaos action                                 |
| `BENCH_CHAOS_DURATION_SECONDS` | How long the disruption lasts                             |

## Running a scenario

Prerequisites: Docker Desktop (or any Compose v2-compatible daemon) and PowerShell 7+.

```powershell
# Default — bring stack up, run, tear down.
./benchmark.ps1 B-03

# Keep the stack running so Grafana stays accessible afterwards.
./benchmark.ps1 -Scenario B-04 -KeepRunning

# Tear down a -KeepRunning stack manually.
docker compose -f docker-compose.yml -f docker-compose.replication.yml down -v
```

The script:

1. Reads `scenarios/<id>.env`, exporting every key as a process env var.
2. Picks the right compose-file overlay (replication or single-cluster).
3. Syncs the Orleans.Lattice dashboards from `src/lattice.dashboards/Grafana/`
   into `benchmark/grafana/dashboards/` (substituting `${DS_PROMETHEUS}` → `prometheus`).
4. `docker compose up --build -d`.
5. Polls `/api/ping/health` until the silo + api are reachable.
6. Seeds the configured fleet size via `/api/vehicles/batch` and starts every vehicle.
7. Waits `BENCH_WARMUP_SECONDS`, runs the `BENCH_DURATION_SECONDS` measurement
   window, applies any chaos (`pause` / `kill`) at `BENCH_CHAOS_AFTER_SECONDS` in
   parallel, then `stop-all`s the fleet.
8. Prints fleet stats and (unless `-KeepRunning`) tears the stack down.

## Dashboards

Grafana provisions the embedded **Orleans.Lattice** dashboards (overview, commit
path, replication) from `src/lattice.dashboards/Grafana/` automatically. Browse to
<http://localhost:3000> — anonymous viewer access is enabled, admin
credentials are `admin/admin`.

The dashboards bind against the meters:

| Meter                              | Source                                       |
|------------------------------------|----------------------------------------------|
| `orleans.lattice`                  | core library (shard reads/writes, splits)   |
| `orleans.lattice.replication`      | replication package (WAL, ship-loop, apply) |
| `vehicle_fleet_simulator.sink`     | `LatticeSink` (publish/drop/queue depth)    |

Prometheus is at <http://localhost:9090> for raw query access.

## B-02 — micro-benchmark path

`B-02` does not stand up the docker stack. It targets `ILattice` directly through
the Orleans.TestingHost-embedded harness so the comparison surfaces the lattice
write-path cost without any simulator or HTTP overhead.

```powershell
./benchmark.ps1 B-02   # prints the harness invocation hint
```

Run the harness command shown in `scenarios/B-02.env` (the project lives under
`test/lattice/` and exposes a benchmark filter — see the test project's README
for the exact category to filter to).
