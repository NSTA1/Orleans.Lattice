# Orleans grain-RPC + `Task.WaitAsync` wedge repro

A minimal in-process Orleans 10.1.0 silo + console driver that probes whether
`Task.WaitAsync` correctly cancels a parked grain RPC under conditions that
incrementally approach the Lattice production wedge investigated on the
`fix/wedge` branch.

The investigation history is in the `wedge-plan.md` scratch notes; the short
version: the residual phase-1/activation WAL wedge on the azure-throughput
tier surfaces as `inFlight=8` pinned for 120+ seconds against a shipped
30-second `WalAppendDispatchTimeout` deadline that never fires. Source-walking
narrowed the suspect to the `WaitAsync` / Orleans grain-RPC return-task
interaction; this repro tests that hypothesis in isolation.

## Status

The minimal repro (no arguments) **does NOT reproduce the wedge** - all arms
fire their deadlines in ~2 s against a 2 s budget. The wedge therefore
depends on at least one additional condition the minimal repro does not have.
Each subsequent commit to this folder adds one such condition gated behind a
console-app argument, so the "smallest combination that reproduces" can be
bisected commit-by-commit.

## Layout

| File | Purpose |
|---|---|
| `Program.cs` | Console driver. Each arm exercises one combination of (caller context, deadline pattern). Wall-clock-capped per arm. |
| `Orleans.Lattice.Repro.Wedge.csproj` | net10.0, `Microsoft.Orleans.Server` + `Sdk` 10.1.0, `Microsoft.Extensions.Hosting` 10.0.3. No Lattice reference - this repro is upstream-only. |

## Running

```powershell
dotnet run --project repro/wedge-orleans -c Release
```

Default invocation runs the four baseline arms with a 2 s `WaitAsync` budget
and a 30 s wall-clock cap per arm. Future extensions will accept arguments
documented in `Program.cs` to gate additional conditions.

## Extending

Each new condition lands as its **own commit**, gated behind a new
console-app argument so prior arms remain reachable. The driver's exit code
is `0` only when every requested arm fires its deadline; an arm that hits
its wall-clock cap returns non-zero and prints `REPRO of the wedge` so the
bisect signal is obvious.

When a future commit produces a non-zero exit, that commit''s added
condition is *necessary* for the wedge; document it in the commit message
and in `wedge-plan.md` Section 6 (the "candidates not present in repro"
ranking). When no condition is sufficient even after enumeration, escalate
to a richer diagnostic (ClrMD lock-ownership, `dotnet-trace`, etc.).

## Out of scope

- This repro is intentionally upstream-only (no Lattice reference). If the
  wedge requires Lattice-specific code (singleton helper, multi-shard chain,
  reshard-rejection storm), the *shape* of that code is reconstructed inline
  here rather than referenced from `src/lattice/`, so the repro remains
  self-contained and shippable as an upstream report attachment.
- The repro intentionally leaks blocked grain turns on shutdown - the silo
  host-stop is best-effort and bounded by a small CTS.
