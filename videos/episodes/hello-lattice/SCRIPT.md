# Hello, Lattice - script

Status: fact-checked on 2026-09-27 by the Docs agent, cue by cue, against
`README.md` "Quick Start", `docs/lattice/api.md` ("Setup", "Basic usage", the
`ILattice` single-key and enumeration tables, `TypedLatticeExtensions`),
`docs/lattice/wal.md`, `videos/series.md` (B1, B2, B3, B6) and the
`npm run series -- ending B1` output, with the behaviour confirmed in source
(`AddLattice` in `src/lattice/LatticeServiceCollectionExtensions.cs`;
`GetAsync`, `GetOrSetAsync` and `DeleteAsync` in `ILattice.cs`; the JSON default
in `TypedLatticeExtensions.cs`). Four fixes applied: in memory "suits
development and tests" in place of "right for a first run"; get or set writes
when the key is "absent, or deleted"; lexicographic order attributed to scans;
and the closing cue separates the Quick start from the API reference, which it
describes as the contract for each operation. Five Sources rows corrected to
match.

Written form, in the series voice (Emma, British English). The pronunciation
lexicon (`voice/lexicon.json`) handles "Orleans.Lattice", "NuGet", "ILattice"
and, for the Kokoro engine, "idempotent"; captions keep the text exactly as
written here. The code itself is on screen, so the narration says what each
call does in words rather than reading out method names.

The first episode on Build: it starts in technical terms, with no recap.

Each paragraph under "Narration" is one cue, and each cue is a beat its scene
can move on. A `pause` comment adds that many seconds of silence before the
next cue, or after the last one.

## Narration

<!-- pause 1.0 -->

### Hello, Lattice

Hello, Lattice. In this episode you register Lattice on a silo, resolve a tree by name, and write and read typed values.

### Register on a silo

Start with the package, Orleans.Lattice, from NuGet.

Then register it on the silo. One call registers the grain catalogue, the grain storage that its callback supplies, and the log that records every write.

Here the grain storage is in memory, and so, by default, is the log. That suits development and tests.

In production, make both durable: the grain storage that holds the tree's state, and the log. Going durable, later on this path, shows how.

<!-- pause 0.6 -->

### A tree, by name

Elsewhere, on a client or inside a grain, resolve a tree by name: ask the grain factory for an ILattice, with the tree's name.

Resolution is idempotent. The same name always routes to the same tree.

<!-- pause 0.6 -->

### Typed values

At the core, keys are strings and values are byte arrays. But the typed extensions serialize for you, so application code rarely touches a byte array.

Set a record under a key, and get it back as the same type.

To choose your own format, pass a serializer of your own. Or write the raw bytes, when you want to own the encoding.

<!-- pause 0.6 -->

### When a key is not there

Reading a key that is absent, or deleted, returns null.

A delete tombstones the key, and returns whether it was live.

And get or set writes only when the key is absent, or deleted. It returns the value that is already there, or null when it wrote, with no race between the read and the write.

And a scan streams keys back in strict lexicographic order. Scans have an episode of their own.

<!-- pause 1.0 -->

### Where next

Next on Build: Values that merge. The links are on this episode's page in the documentation.

The Quick start has the setup and the typed code. The API reference has the rest, and it is the contract for each operation: what it returns and what it throws.

<!-- pause 2.0 -->

## Sources

Each line of narration traces to one of these. Where the script is narrower
than its source, or says it in plainer words, the reason is given.

| Narration | Source |
| --- | --- |
| register Lattice on a silo, resolve a tree by name, and write and read typed values | `videos/series.md`, B1's idea, word for word |
| the package, Orleans.Lattice, from NuGet | `docs/lattice/api.md`, "Setup" (`dotnet add package Orleans.Lattice`) |
| one call registers the grain catalogue, the grain storage its callback supplies, and the log that records every write | README, "Quick Start" ("`AddLattice` registers the grain catalogue, the grain storage provider (via the supplied callback), and the in-memory write-ahead-log backend in a single call"); `docs/lattice/wal.md`. "The log that records every write" says "write-ahead log" in plain words, because the voice pauses at a hyphen |
| the grain storage is in memory, and so, by default, is the log; that suits development and tests | README, "Quick Start": the snippet's `AddMemoryGrainStorage` and "AddLattice registers the in-memory WAL by default"; `docs/lattice/api.md`, "Setup" ("In-memory (development / tests)", and the in-memory provider is registered "by default (suitable for development and single-process tests)") |
| in production, make both durable: the grain storage that holds the tree's state, and the log | README, "Quick Start" ("make both storage surfaces durable: the grain-storage provider that holds tree state ... and the write-ahead log"). How is B6's subject, "Going durable" (`videos/series.md`) |
| on a client or inside a grain, resolve a tree by name; ask the grain factory for an ILattice | README, "Quick Start" ("on the client or inside a grain - resolve a tree by name"; `grainFactory.GetGrain<ILattice>("my-tree")`) |
| resolution is idempotent; the same name always routes to the same tree | `docs/lattice/api.md`, "Basic usage" ("idempotent - the same name always routes to the same tree") |
| keys are strings and values are byte arrays at the core; the typed extensions serialize for you; application code rarely touches a byte array | README, "Quick Start" ("Values are byte[] at the core, but the typed extensions serialize for you, so application code rarely touches a byte[]"); `docs/lattice/api.md`, "Basic usage" ("Keys are `string`; values are `byte[]`") |
| set a record under a key, and get it back as the same type | README, "Quick Start" (`SetAsync("user/42", new User("Ada", 36))`, `GetAsync<User>("user/42")`). That the default format is JSON is on screen, in the snippet's comment, not in the narration |
| pass a serializer of your own, or write the raw bytes when you want to own the encoding | README, "Quick Start" ("Pass an ILatticeSerializer<T> to choose your own format, or use the raw byte[] surface directly when you want to own the encoding"); `docs/lattice/api.md`, `TypedLatticeExtensions` |
| reading a key that is absent, or deleted, returns null | `docs/lattice/api.md`, `GetAsync` ("`null` when absent or tombstoned"). "Deleted" stands for "tombstoned" until the next cue names it |
| a delete tombstones the key, and returns whether it was live | `docs/lattice/api.md`, `DeleteAsync` ("Tombstones `key`. Returns `true` if the key was live") |
| get or set writes only when the key is absent, or deleted; returns the value already there, or null when it wrote; no race between the read and the write | `docs/lattice/api.md`, `GetOrSetAsync` ("Inserts `value` only when `key` is absent or tombstoned. Returns the existing value when live, or `null` when the new value was written. No read-then-write race") |
| a scan streams keys back in strict lexicographic order; scans have an episode of their own | `docs/lattice/api.md`, "Basic usage" ("Stream a key range in strict lexicographic order") and `ScanKeysAsync`; B3, "Scans, filters and cursors" (`videos/series.md`) |
| next on Build: Values that merge; the links are on this episode's page | `npm run series -- ending B1`, word for word |
| the Quick start has the setup and the typed code; the API reference has the rest, and is the contract for each operation: what it returns and what it throws | README, "Quick Start" (the setup and typed panels are its code, split into panels); `docs/lattice/api.md`, "Basic usage" (the delete and get-or-set panels); `docs/lattice/api.md`, its opening ("signature, return value, exceptions, and observable effect") |
