# Samples

Runnable end-to-end samples that exercise Orleans.Lattice against an in-process Orleans silo. Each sample lives under [`samples/`](../samples) and is a self-contained console application that can be launched with `dotnet run --project <path>`.

## HelloWorld

[`samples/HelloWorld`](../samples/HelloWorld)

Minimal interactive REPL over a single-silo, in-memory Orleans cluster. Starts a silo configured with `AddLattice(...)` + in-memory grain storage and reminders, then prompts for commands — `create`, `read`, `update`, `delete`, `list`, `exit` — and applies each one against a tree named `hello-world`. Every operation is timed with `Stopwatch` and reported as `[OK]` / `[FAIL]` with the elapsed milliseconds, so it doubles as a quick sanity check that a locally-built `Orleans.Lattice` package behaves correctly.

Run it with:

```shell
dotnet run --project samples/HelloWorld
```

## MultiSiteManufacturing

[`samples/MultiSiteManufacturing`](../samples/MultiSiteManufacturing)

Regulated process-engineering traceability demo built on Blazor Server + gRPC + Orleans + Orleans.Lattice, backed by Azure Table Storage and Azure Storage Queues (Azurite for local development). Models a turbine-blade lifecycle (forge → heat-treat → machining → NDT → MRB → FAI) across seven process sites, with a bulk-loaded inventory, operator-driven fact emission, a chaos fly-out for injecting site-level pause/delay/reorder, and a live divergence feed comparing a baseline LWW backend against the Orleans.Lattice fact store.

The sample runs as **two independent Orleans clusters** (`us` and `eu`), each with two silos, connected by an opt-in cross-cluster replication link over HTTP/JSON so changes in one cluster converge on the other. The dashboard, operator surface, and chaos controls work against either cluster and a cluster-wide live activity feed is fanned out over an Azure Storage Queue-backed Orleans stream so every Blazor circuit sees every fact regardless of which silo hosts it. A full `docker-compose.yml` (plus Traefik routing and Azurite) brings both clusters up locally.

Supporting documentation lives alongside the sample:

- [`plan.md`](../samples/MultiSiteManufacturing/plan.md) — design document and feature plan.
- [`approach.md`](../samples/MultiSiteManufacturing/approach.md) — implementation rationale, gotchas, and the reasoning behind each design choice.
- [`architecture.md`](../samples/MultiSiteManufacturing/architecture.md) — structural view: topology, component graph, grain interdependencies, Lattice trees, replication sequence.
- [`glossary.md`](../samples/MultiSiteManufacturing/glossary.md) — domain and implementation terms.

Run it with:

```shell
./samples/MultiSiteManufacturing/run.ps1
```

The script builds the host image if needed, starts both clusters (four silos plus two Azurites plus two Traefik proxies) under Docker Compose, and prints the per-cluster URLs — `http://localhost:5001` for `us` and `http://localhost:5002` for `eu`. Use `-Down` to tear everything back down, `-Clean` to wipe state between runs, and `-Logs` to tail silo logs.
