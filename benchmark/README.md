# Lattice Benchmarks

## Prerequisites

Install [bombardier](https://github.com/codesenberg/bombardier) and ensure it is on your `PATH`.

## Quick start

From the `benchmark/` directory:

```powershell
./run.ps1
```

This single script:

1. Builds the host in Release
2. Starts it as a background process
3. Runs [bombardier](https://github.com/codesenberg/bombardier) against each endpoint (SET → GET → DELETE → KEYS)
4. Samples host CPU and memory per scenario
5. Prints a summary table
6. Compares results with the previous run (if one exists)
7. Prompts to retain or discard the current results
8. Stops the host (even on error)

### Run history

Results are saved to `benchmark/results.json` (git-ignored). Each run
is compared against the previous one, showing **ΔRPS** so regressions
are immediately visible. You choose whether to keep or discard after
reviewing the numbers.

### Parameters

| Parameter        | Default | Description                                      |
|------------------|---------|--------------------------------------------------|
| `-Duration`      | 15      | Seconds each scenario runs                       |
| `-Concurrency`   | 64      | Number of concurrent bombardier connections       |
| `-KeysKeyCount`  | 100     | Number of keys seeded for the KEYS scenario       |

Example:

```powershell
./run.ps1 -Duration 30 -Concurrency 128
```

## Endpoints under test

The host exposes both standard CRUD endpoints and benchmark-specific
endpoints that use server-side atomic counters so each bombardier
request hits a different key.

### CRUD endpoints

| HTTP Method | Path                            | `ILattice` Method |
|-------------|---------------------------------|--------------------|
| GET         | `/lattice/{treeId}/keys/{key}`  | `GetAsync`         |
| PUT         | `/lattice/{treeId}/keys/{key}`  | `SetAsync`         |
| DELETE      | `/lattice/{treeId}/keys/{key}`  | `DeleteAsync`      |
| GET         | `/lattice/{treeId}/keys`        | `KeysAsync`        |

### Benchmark endpoints

| Scenario | HTTP Method | Path                                | Behaviour                              |
|----------|-------------|-------------------------------------|----------------------------------------|
| SET      | POST        | `/lattice/{treeId}/bench/set`       | Writes `k0`, `k1`, … (unbounded)      |
| GET      | GET         | `/lattice/{treeId}/bench/get`       | Reads keys cyclically                  |
| DELETE   | DELETE      | `/lattice/{treeId}/bench/delete`    | Deletes `k0`, `k1`, … (each once)     |
| KEYS     | GET         | `/lattice/{treeId}/keys`            | Scans all keys in the tree             |
| (setup)  | POST        | `/lattice/{treeId}/seed?count=N`    | Bulk-inserts `k0`…`k{N-1}`            |
| (setup)  | POST        | `/lattice/{treeId}/bench/reset`     | Resets GET/DELETE counters             |
