# Samples

Runnable end-to-end samples that exercise Orleans.Lattice against an in-process Orleans silo. Each sample lives under [`samples/`](../samples) and is a self-contained console application that can be launched with `dotnet run --project <path>`.

## HelloWorld

[`samples/HelloWorld`](../samples/HelloWorld)

Minimal interactive REPL over a single-silo, in-memory Orleans cluster. Starts a silo configured with `AddLattice(...)` + in-memory grain storage and reminders, then prompts for commands — `create`, `read`, `update`, `delete`, `list`, `exit` — and applies each one against a tree named `hello-world`. Every operation is timed with `Stopwatch` and reported as `[OK]` / `[FAIL]` with the elapsed milliseconds, so it doubles as a quick sanity check that a locally-built `Orleans.Lattice` package behaves correctly.

Run it with:

```shell
dotnet run --project samples/HelloWorld
```
