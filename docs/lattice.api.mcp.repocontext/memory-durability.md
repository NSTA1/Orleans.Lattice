# Memory durability

What survives destroying a repository-context deployment's state, what does not,
and which gesture to reach for when you want to start clean.

The short version: the **code index is rebuildable and agent memory is not**, and
the two share a volume. `repocontext_reset_index` respects that distinction and
loses nothing. `docker compose down -v` does not, and takes both.

## Two kinds of state, one volume

| | Trees | Where it comes from | If it is destroyed |
|---|---|---|---|
| **Rebuildable** | `repo-context-structural`, `-content`, `-symbol`, `-xref`, `-session`, and the three `repo-context-vector-*` planes | Derived by walking files on disk | Re-run `repocontext_add_repo`. Back in minutes. |
| **Irreplaceable** | `repo-context-memory` | Authored by agents through `repocontext_remember` | Gone. It derives from nothing. |

The names look separable and are not. A Lattice tree's durable state spans two
planes, and neither is addressed per tree:

* **The WAL plane.** `FileWalStorageOptions.RootDirectory` is one directory for
  the whole storage **provider**. The `repo-context-*` subdirectories under it
  are a naming convention inside that one root, not independently configurable
  locations.
* **The grain-storage plane**, which is the larger half. Every B+ tree grain
  binds `[PersistentState(..., LatticeOptions.StorageProviderName)]` - one
  provider name for all of them. Under the `local` durability profile that
  resolves to a single SQLite file, in which the memory tree's leaves, topology
  and snapshots are interleaved with every other tree's.

So there is no memory-only path on disk to isolate. Splitting the trees across
storage locations would need per-tree storage routing in the core library, which
does not exist.

## The safe gesture for each

**To rebuild an index: `repocontext_reset_index`.** It drops the structural,
symbol, content, cross-reference, session and vector trees and **preserves the
memory tree outright**. There is no window and nothing to restore afterwards -
the memory records are never touched. The repository stays registered and stays
listed by `repocontext_list_repos`, reporting no ingest and no file count, which
is exactly its state; a subsequent `repocontext_add_repo` rebuilds the index from
the working files.

**To destroy a repository's context entirely: `repocontext_remove_repo`.** It
removes structural records, memory and vectors for one repository, and is the
only verb that drops it from the listing. It requires explicit human consent
precisely because it destroys the store of record.

**`docker compose down -v` is neither of these.** It removes the volume, so it
destroys memory and index together with no distinction between them, and it is
routinely reached for as "start clean". That asymmetry - a documented,
index-shaped gesture with an undocumented, memory-shaped consequence - is issue
#2601, filed after it destroyed epic #2368's accumulated agent memory.

## The memory archive

Since the trees cannot be separated on disk, the container-level answer is to
keep a copy of the memory tree somewhere the destructive gesture does not reach,
and to put it back when the store comes up empty.

**A second named volume does not achieve this.** `docker compose down -v` removes
every named volume the project declares, so a `repocontext-memory` volume would
die in the same command as `repocontext-data`. A **bind mount** is not a
project-declared volume, so `-v` does not remove it - that difference, and not
the existence of a second mount, is the whole of the protection.

Set `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_DIR` to a directory on such a mount and
the host will:

1. **Restore at startup**, subject to `..._RESTORE` below. The import folds
   through the same CRDT merge every write uses, so restoring over live records
   converges rather than clobbering - an older archived value meeting a newer
   live one yields the newer one.
2. **Export periodically**, at `..._INTERVAL_SECONDS`.
3. **Export once more during a graceful stop**, bounded by
   `..._STOP_TIMEOUT_SECONDS`, which is the export that matters most because a
   graceful stop is exactly what `down`, `down -v` and `stop` perform.

### Settings

| Variable | Default | Meaning |
|---|---|---|
| `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_DIR` | unset | Directory the archive is written to. **Unset disables the whole mechanism**, so it is opt-in and a host that sets nothing behaves exactly as before. |
| `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_INTERVAL_SECONDS` | `300` | Export cadence. Values below 30 are raised to 30, so a misconfiguration cannot turn the exporter into a busy loop against the store. |
| `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_RESTORE` | `auto` | `auto` restores only when the store holds no memory at all; `always` restores on every start; `off` never restores and leaves the archive write-only. |
| `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_STOP_TIMEOUT_SECONDS` | `20` | Budget for the final export during a graceful stop, clamped to 1-60 seconds. It is deliberately a fraction of the container's stop grace period, which the drain also needs. |

`auto` is the useful default because it heals exactly the case this exists for -
a store that came up empty because its volume was destroyed - and does nothing
on an ordinary restart where the memory is already there.

### Files on disk

```text
<archive dir>/
  repo-context-memory.snapshot            # current
  repo-context-memory.previous.snapshot   # the generation before it
```

Each export writes a uniquely named temporary file, flushes it to disk, rotates
the current snapshot to `.previous`, and only then renames the temporary file
into place. An export interrupted at any point therefore leaves the previous
snapshot intact and importable; restore falls back to it automatically if the
current one cannot be read. Temporary files left by an interrupted process are
swept on the next export.

An export that would write **zero** records over a non-empty archive is refused.
A store that is momentarily unreadable must not be allowed to overwrite a good
archive with an empty one.

### Restoring by hand

The archive is a plain file. To restore into a fresh container, put it where the
mount points and start the box against an empty store:

```bash
ls memory-archive/                    # repo-context-memory.snapshot
docker compose up -d
docker compose logs repocontext | grep -i 'memory durability'
```

The startup report states where memory lives and what protects it, at warning
level, on every start.

## What this does not do

Read this section before relying on the archive. It is deliberately narrower
than it may look.

* **It is not a backup.** It captures agent memory and nothing else. The index
  is not in it, by design - the index rebuilds from source.
* **It does not make `down -v` lossless.** Everything written since the last
  export is lost. The exposure is the export interval, plus whatever a
  non-graceful stop discards. A graceful stop closes most of that window; a
  `SIGKILL` or a host crash gets no chance to run the final export.
* **It does not remove the co-location.** Memory still shares a volume with
  rebuildable state. Nothing configurable changes that, which is why the startup
  statement reports it every time rather than falling silent once an archive is
  configured.
* **An archive inside the data volume protects nothing.** If the configured
  directory resolves under the data root, it dies with the thing it is meant to
  outlive. The host detects that case and says so, loudly, at startup.
* **It is not the scheduled whole-store backup of issue #2602.** That mechanism
  owns manifests, retention, and operator-driven restore of the entire store.
  This one owns automatic restore-on-empty for memory alone. **Only this
  mechanism restores automatically at startup**; the backup path does not
  auto-heal, so the two cannot race to repopulate the same store.

## Reference

- [Architecture](architecture.md) - the store-of-record versus rebuildable-projection distinction this rests on.
- [Record model](record-model.md) - the named trees and the CRDT merge that makes an import converge rather than clobber.
- [Memory and TTL](memory-and-ttl.md) - what agent memory is, and how a per-entry TTL expires it deliberately rather than accidentally.
- [Tools](tools.md) - `repocontext_reset_index` and `repocontext_remove_repo` in full.
- [Container quickstart](container.md) - running the module as a single durable local container.
- [Container sample](../../samples/RepoContextContainer/README.md) - the compose file this is wired into.
