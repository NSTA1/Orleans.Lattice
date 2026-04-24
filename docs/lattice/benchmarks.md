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
2. For each approach, starts a **fresh host process** (clean memory baseline)
3. Runs [bombardier](https://github.com/codesenberg/bombardier) against each endpoint (SET → GET → DELETE → KEYS)
4. Samples host CPU and memory per scenario
5. Prints a summary table with resource usage
6. Compares results with the previous run (including CPU and memory)
7. Stops the host before moving to the next approach
8. After all approaches, prints a total summary and prompts to retain or discard
9. Ensures the host is always killed — even on Ctrl+C, errors, or script exit

### Run history

Results are saved per approach to separate files (git-ignored):
- `benchmark/results-ordered.json`
- `benchmark/results-random.json`
- `benchmark/results-reverse.json`
- `benchmark/results-bulkload.json`

Each run is compared against the previous run for that approach, showing
**ΔRPS** so regressions are immediately visible. You choose whether to
keep or discard after reviewing the numbers.

### Parameters

| Parameter          | Default                              | Description                                       |
|--------------------|--------------------------------------|---------------------------------------------------|
| `-Duration`        | 15                                   | Seconds each scenario runs                        |
| `-BulkLoadDuration`| 10                                   | Seconds the BULK-SET scenario runs                |
| `-Concurrency`     | 64                                   | Number of concurrent bombardier connections        |
| `-KeysKeyCount`    | 100                                  | Number of keys seeded for the KEYS scenario        |
| `-Approaches`      | ordered, random, reverse, bulkload   | Key-ordering approaches to benchmark               |

Example:

```powershell
./run.ps1 -Duration 30 -Concurrency 128
```

Run only a single approach:

```powershell
./run.ps1 -Approaches ordered
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
| GET         | `/lattice/{treeId}/keys`        | `ScanKeysAsync`        |

### Key-ordering approaches

| Approach   | Description                                                  |
|------------|--------------------------------------------------------------|
| `ordered`  | Keys inserted as `k000000`, `k000001`, … (ascending)         |
| `random`   | Keys inserted in a pre-shuffled permutation                  |
| `reverse`  | Keys inserted as `k{N-1}`, `k{N-2}`, … (descending)         |
| `bulkload` | Bulk-loads 1,000 keys per request via `BulkLoadAsync`        |

### Benchmark endpoints

| Scenario  | HTTP Method | Path                                       | Behaviour                              |
|-----------|-------------|---------------------------------------------|----------------------------------------|
| (setup)   | POST        | `/lattice/{treeId}/bench/configure`         | Set key approach and key count          |
| SET       | POST        | `/lattice/{treeId}/bench/set`               | Writes keys per configured approach     |
| GET       | GET         | `/lattice/{treeId}/bench/get`               | Reads keys cyclically                  |
| DELETE    | DELETE      | `/lattice/{treeId}/bench/delete`            | Deletes keys (each once)               |
| KEYS      | GET         | `/lattice/{treeId}/keys`                    | Scans all keys in the tree             |
| (setup)   | POST        | `/lattice/{treeId}/seed?count=N`            | Bulk-inserts `k0`…`k{N-1}`            |
| (setup)   | POST        | `/lattice/{treeId}/bench/reset`             | Resets GET/DELETE counters             |
