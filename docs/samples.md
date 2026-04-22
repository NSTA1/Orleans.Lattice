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

Regulated process-engineering traceability demo built on Blazor Server + gRPC + Orleans + Orleans.Lattice, backed by Azure Table Storage (Azurite for local development). Models a turbine-blade lifecycle (forge → heat-treat → machining → NDT → MRB → FAI) across seven process sites, with a bulk-loaded inventory, operator-driven fact emission, a chaos fly-out for injecting site-level pause/delay/reorder, and a live divergence feed comparing a baseline LWW backend against the Orleans.Lattice fact store. See [`samples/MultiSiteManufacturing/plan.md`](../samples/MultiSiteManufacturing/plan.md) for the design document.
