# Record model

The repository-context store is a set of named Lattice trees, one per CRDT family, addressed by a stable hierarchical key grammar. This is the layout contract: keys and tree names do not change once shipped.

## Named trees

One dedicated tree per record family, so per-tree options (replication, tombstone compaction, backup, enumeration) are tuned independently:

| Tree | Holds | Churn |
|---|---|---|
| `repo-context-structural` | Repo, package, file, and symbol nodes | Low |
| `repo-context-memory` | Agent-authored memory records | Higher (re-write / forget cycles) |
| `repo-context-vector-membership` | Vector collection membership | Low |
| `repo-context-vector-payload` | Content-addressed vector payloads | Write-once |
| `repo-context-vector-metadata` | Vector metadata (source, content address, attributes) | Higher (re-embed cycles) |

Mixing CRDT types in one tree would not be a correctness problem - each key's value is its own CRDT - but the per-tree split future-proofs selective replication (replicate the store-of-record trees; treat the rebuildable vector projections as optionally local-only), independent TTL / garbage-collection and backup policy per family, and clean single-tree enumeration for derived-projection rebuilds.

A host tunes a tree by name through `IOptionsMonitor<LatticeOptions>.Get(treeName)`. The higher-churn trees (memory, vector membership, vector metadata, and structural, which the bootstrap prunes) should carry a finite tombstone grace period and a compaction trigger so re-write and forget tombstones are reaped; the local container applies exactly that in every durability profile.

## Key grammar

Every record is addressed by a hierarchical key rooted at its repository id:

| Family | Key shape |
|---|---|
| Repo root | `repo/{repoId}` |
| Package / module / directory | `repo/{repoId}/pkg/{path}` |
| Source file | `repo/{repoId}/file/{path}` |
| Symbol | `repo/{repoId}/symbol/{fqName}` |
| Memory | `repo/{repoId}/mem/{topic}/{id}` |
| Vector metadata | `repo/{repoId}/vec/{vectorId}` |
| Vector payload | `repo/{repoId}/vpay/{contentAddress}` |
| Vector membership | `repo/{repoId}/vmem/{collection}` |

The leading segment after the repository id (`pkg`, `file`, `symbol`, `mem`, `vec`, `vpay`, `vmem`) is the family discriminator, so a key both addresses a record and routes it to the correct tree. Because keys are ordered, a scope like "every file under a repo" or "every memory entry under a topic" is a single bounded range walk.

## CRDT store of record

The store of record is the WAL-backed Lattice tree itself. Each value is a CRDT record with a static, commutative, associative, idempotent `Merge`, so:

- A write is a read-merge-write through the record's `Merge`, never a blind overwrite, and concurrent writers converge.
- Field edits (`repocontext_update`) apply each field as a last-writer-wins register at a fresh logical tick, so a later edit wins deterministically and untouched fields are preserved.
- The vector trees hold discardable, regenerable projections: payloads are content-addressed and write-once, membership is an add-wins set of stable source ids, and metadata carries the embedding-space identity so a wrong-space vector is never compared or stored.

Because the trees are ordinary Lattice trees, the whole store inherits per-entry TTL, read-time hiding of expired and tombstoned entries, background tombstone compaction, and (where enabled) cross-cluster replication - the module adds none of these itself.
