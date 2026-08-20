# Record model

The repository-context store is a set of named Lattice trees, one per CRDT family, addressed by a stable hierarchical key grammar. This is the layout contract: keys and tree names do not change once shipped.

## Named trees

One dedicated tree per record family, so per-tree options (replication, tombstone compaction, backup, enumeration) are tuned independently:

| Tree | Holds | Churn |
|---|---|---|
| `repo-context-structural` | Repo, package, and file nodes | Low |
| `repo-context-symbol` | Symbol nodes (type / member / function declarations) | Moderate (code-edit reconcile) |
| `repo-context-content` | Per-file searchable-content projection (bounded body text) | Moderate (re-write / delete on file change) |
| `repo-context-xref` | Reverse cross-reference nodes (per referenced simple type-name: its dependents and covering tests) | Moderate (code-edit reconcile) |
| `repo-context-memory` | Agent-authored memory records | Higher (re-write / forget cycles) |
| `repo-context-session` | Per-session context-bundle reuse bookkeeping (delivered-unit receipts and whole-file possession) | Higher (session create / expire cycles) |
| `repo-context-vector-membership` | Per-source vector membership (add-wins presence flags) | Low (one flag per embedded source) |
| `repo-context-vector-payload` | Content-addressed vector payloads | Write-once |
| `repo-context-vector-metadata` | Vector metadata (source, content address, attributes) | Higher (re-embed cycles) |

Mixing CRDT types in one tree would not be a correctness problem - each key's value is its own CRDT - but the per-tree split future-proofs selective replication (replicate the store-of-record trees; treat the rebuildable vector projections as optionally local-only), independent TTL / garbage-collection and backup policy per family, and clean single-tree enumeration for derived-projection rebuilds.

A host tunes a tree by name through `IOptionsMonitor<LatticeOptions>.Get(treeName)`. The higher-churn trees (memory, vector membership, vector metadata, and the structural, symbol, and content trees, which the reconcile prunes and re-writes) should carry a finite tombstone grace period and a compaction trigger so re-write and forget tombstones are reaped; the local container applies exactly that in every durability profile.

### Symbol nodes

The symbol tree holds one record per declared symbol at `repo/{repoId}/symbol/{fqName}`, extracted from source during onboarding and kept converged by the same incremental reconcile that maintains file nodes. Extraction is language-dispatched behind a per-language seam; C# is extracted with the Roslyn syntax parser today, and a file whose language has no registered extractor simply declares no symbols. A symbol's declaring files are an add-wins set, so a symbol split across several files - a C# partial type is the canonical case - survives as long as any file still declares it and is pruned only once the last one stops. Every repository-context tree, the symbol tree included, stays unversioned: each value is a byte-identical CRDT record with no per-record schema-version stamp, so a future change to the symbol record shape is an ordinary additive serialization change, not a versioned reinterpretation of stored bytes.

Symbol presence is tracked and self-healed the same way embeddings are. Each file node carries a **symbol-processed marker** set the first time the file is run through extraction, distinct from its declared-symbol set (which is legitimately empty for a supported file that declares nothing). Because content-digest change detection and symbol presence answer different questions - an unchanged file is structurally skipped on re-index, but digest equality says nothing about whether its symbols were ever extracted - a repository indexed before symbol extraction existed would otherwise never populate the symbol tree. The reconcile therefore runs an idempotent **symbol back-fill**: on every pass it selects the content-unchanged, supported-language files whose node was never marked processed, extracts them exactly like an added file, and stamps the marker. Re-running converges to zero back-fill once every eligible file is processed. The back-fill draws only from the pure-unchanged set (not anchor-refreshed files), so a back-filled node is rewritten exactly once per pass, and a file that cannot be read is left unmarked and retried, so the pass is resumable across a crash.

### Content projection

The content tree holds one record per text file at `repo/{repoId}/content/{path}`, carrying the file's bounded UTF-8 body text (capped at 64 KiB per file so a single huge generated file cannot grow the store without bound). It exists to give the **keyword / degraded search path** something to rank over besides filenames and symbol names: when no embedder is bound (or one is temporarily unavailable), keyword search folds each file's stored body text into its haystack, so a query matches file **content**, not just paths and identifiers. See [semantic-search.md](semantic-search.md) for how the keyword path consumes it.

The projection is **decoupled from the embedding provider on purpose** - its whole point is to improve the no-embedder path - so content is written during the **structural reconcile**, where the changed-file bytes are already read for hashing, and never in the embedding ingestor. Every walked file is a text file (the walk excludes binary), so all files are indexed; a file too large to read in a pass is skipped and retried later. The record is a rebuildable projection, not store-of-record: its body text is a last-writer-wins register (so concurrent replicas converge on the newest body), it is retired when its file is deleted, and it is re-derived on a rebuild.

Content presence is self-healed exactly like symbols. Each file node carries a **content-processed marker** set the first time the file's body is projected; a repository indexed before the content projection existed would otherwise never populate the content tree, because an unchanged file is structurally skipped on re-index. The reconcile runs an idempotent **content back-fill**: on every pass it selects the content-unchanged files whose node was never marked processed, projects their body exactly like an added file, and stamps the marker. The back-fill draws only from the pure-unchanged set, so a back-filled node is rewritten at most once per pass, and a file that cannot be read is left unmarked and retried, so the pass is resumable across a crash. The symbol, content, and cross-reference back-fills are unified into a single node rewrite per pass, so a file eligible for more than one is written once, not several times.

Because the content tree is a rebuildable derived projection and not a store of record, the reconcile also **tolerates a terminally-stale content tree** rather than failing the whole index. If writing the content records throws a stale-leaf-projection fault - the content leaf's durable checkpoint has fallen off its write-ahead log with no covering snapshot, so it awaits an operator-driven rebuild - the reconcile logs a warning, skips content projection for that pass, and leaves every affected file **unmarked**. Structural, symbol, and semantic ingest are independent and still run, so onboarding and re-index complete, and the content back-fill re-projects the skipped files automatically once the content tree is healed. This tolerance is scoped to the content tree alone; the same fault on a store-of-record tree still fails the pass, because that data cannot be re-derived. It pairs with the read-side isolation in [semantic-search.md](semantic-search.md#keyword-search-over-file-content): a stale content tree degrades body-text ranking but never breaks either ingest or retrieval.

### Reverse cross-reference projection

The cross-reference tree holds one reverse cross-reference projection record per referenced simple (unqualified) type-name at `repo/{repoId}/xref/{name}`, maintained incrementally by the same symbol reconcile that keeps the symbol tree converged. Each node records, as add-wins sets, the symbols that reference that name (its **dependents**) and the test types that cover it (recorded from the `{Name}Tests`/`{Name}Test` naming convention). It exists so the `repocontext_related` tool can answer "what depends on this file" and "what tests cover it" as a bounded reverse lookup instead of a whole-repository scan, and so `repocontext_changed` can report the reverse-reference impact set of a set of edits. Because C# extraction is purely syntactic, edges are keyed by simple type-name: two distinct types sharing a simple name are not disambiguated (a known limitation - semantic resolution is out of scope). Like the content and vector trees it is a rebuildable projection, not store-of-record: a node is pruned once its last inbound reference and covering test are gone, so no dependent edge outlives the code that created it.

The reverse index is self-healed like the symbol and content trees, but with one twist. Each file node carries a **cross-referenced marker** distinct from its symbol-processed marker, because a repository symbol-processed before the reverse index existed carries fully-populated outbound references on every symbol record yet has no reverse edges at all, and the incremental delta never rebuilds them: an unchanged file's freshly-extracted references equal its stored references, so the diff is empty and nothing is emitted. The reconcile therefore runs an idempotent **cross-reference back-fill**: on every pass it selects the content-unchanged, supported-language files that are symbol-processed but not yet cross-referenced, and **force-seeds** their reverse edges directly from each declared symbol's already-stored forward references (and its test-subject relationship) - reading the stored records rather than re-parsing the files, since the forward references are authoritative there - then stamps the marker. Add-wins edge adds are idempotent, so a re-driven seed converges on the identical edge set. The seed runs after the symbol reconcile and before the file nodes are rewritten, so a crash between the two leaves the file unmarked and the next pass re-selects it. A file freshly symbol-processed going forward has its reverse edges built by the incremental delta and is stamped in the same pass, so it is never a back-fill candidate.

### Session reuse bookkeeping

The session tree holds one per-session reuse-bookkeeping record per `(repoId, sessionId)` at `repo/{repoId}/session/{sessionId}`, recording what a prior `repocontext_context` call delivered to that session so a later call never re-charges for it. It carries two grow-only sets: the opaque **receipts** of units already delivered, and the whole-file **possession** versions the session has received as a complete body. Each record is a CRDT whose merge is a set union, so concurrent bundle calls sharing a session id converge; entries carry a finite time-to-live (default six hours) so an abandoned session's bookkeeping lapses on its own. The load-bearing guard is enforced on both sides: possession is recorded only for `slices` (whole-body) deliveries, and a `known` whole-file claim is honoured only against recorded possession, so partial (outline/paths) evidence can never be promoted to whole-file possession. See [retrieval-economics.md](retrieval-economics.md#reuse-economics) for the full protocol.

### File token count

Each file node carries a `TokenCount`: the exact number of BPE tokens the whole file body is worth under the configured tokenizer profile, computed once during the content reconcile from the same bytes read for hashing. It is the currency the retrieval surface budgets in - `repocontext_outline` reports it as the cost of a full read, and `repocontext_context` uses it as each candidate's `fullReadTokenCount` when deciding what fits under the bundle ceiling. It is null only for a file that was never content-processed. See [retrieval-economics.md](retrieval-economics.md#the-shared-token-counter) for the tokenizer profile.

## Key grammar

Every record is addressed by a hierarchical key rooted at its repository id:

| Family | Key shape |
|---|---|
| Repo root | `repo/{repoId}` |
| Package / module / directory | `repo/{repoId}/pkg/{path}` |
| Source file | `repo/{repoId}/file/{path}` |
| Symbol | `repo/{repoId}/symbol/{fqName}` |
| Content | `repo/{repoId}/content/{path}` |
| Cross-reference | `repo/{repoId}/xref/{name}` |
| Memory | `repo/{repoId}/mem/{topic}/{id}` |
| Session | `repo/{repoId}/session/{sessionId}` |
| Vector metadata | `repo/{repoId}/vec/{vectorId}` |
| Vector payload | `repo/{repoId}/vpay/{contentAddress}` |
| Vector membership | `repo/{repoId}/vmem/{sourceId}` |

The leading segment after the repository id (`pkg`, `file`, `symbol`, `content`, `xref`, `mem`, `session`, `vec`, `vpay`, `vmem`) is the family discriminator, so a key both addresses a record and routes it to the correct tree. Because keys are ordered, a scope like "every file under a repo" or "every memory entry under a topic" is a single bounded range walk.

## CRDT store of record

The store of record is the WAL-backed Lattice tree itself. Each value is a CRDT record with a static, commutative, associative, idempotent `Merge`, so:

- A write is a read-merge-write through the record's `Merge`, never a blind overwrite, and concurrent writers converge.
- Field edits (`repocontext_update`) apply each field as a last-writer-wins register at a fresh logical tick, so a later edit wins deterministically and untouched fields are preserved.
- The vector trees hold discardable, regenerable projections: payloads are content-addressed and write-once, membership is one add-wins presence flag (`OrFlag`) per stable source id, and metadata carries the embedding-space identity so a wrong-space vector is never compared or stored.

## Memory link staleness

A memory entry can link to a structural target (a file or symbol) through a typed knowledge-linking edge. Because that target's content drifts independently, each link would otherwise silently rot - pointing at a file whose meaning has changed since the link was authored. To make drift observable, a memory record carries a **link-digest map** (`repo/{repoId}/mem/...`, an `OrMap<string, BoundedRegister>` keyed by target key) alongside its link set: when an edge to a file or symbol is added, the store captures that target's current content digest and records it as a last-writer-wins register. A memory-to-memory edge is not a structural target and captures nothing.

On an evaluating read - `repocontext_recall` of a memory entry, and each neighbor of a `repocontext_neighbors` walk - the store compares every currently-linked captured digest against the target's present digest and reports the outcome through `stale` and `staleLinks`. A target that has drifted, or has been deleted, is stale; an unlinked-but-still-recorded digest is never evaluated, so removing an edge cannot leave a phantom flag. Bulk reads (`repocontext_scan`, semantic-search hydration) do **not** evaluate staleness and leave `stale`/`staleLinks` null ("not evaluated"), exactly as they leave the expiry fields null - `recall` a key for the authoritative answer. The captured digest is a change-detection fingerprint only, never a security boundary, and the map merges last-writer-wins so concurrent captures converge.

## Membership presence and multi-cluster replication

Vector membership is the answer to "which sources are currently embedded", read on every back-fill pass to detect gaps and written on every embed and retire. It is stored as **one add-wins presence flag per source** at `repo/{repoId}/vmem/{sourceId}`: an embed enables the source's flag; a retire disables it (a causal tombstone, not a key delete, so a delete converges add-wins against a concurrent re-embed on another cluster). A read scans the per-repo `vmem` range and keeps the sources whose flag is enabled.

The format is **always** an `OrFlag`, independent of whether replication is configured, precisely because the embedding index is expensive and must never be re-derived: coupling the on-disk value shape to a runtime replication toggle would corrupt existing membership the moment replication is enabled or disabled. A single-cluster host authors flag dots under a fixed local replica id; enabling replication later is pure configuration - the same rows keep converging, now authored under the configured cluster id, with no migration and no re-index. Because merge is a union of dots, the dot-authoring replica id may change over a repository's lifetime with no format change.

This makes the whole store **config-only multi-cluster**: the expensive index is computed once and the membership, payload, and metadata trees converge across clusters through ordinary Lattice replication when the host opts a tree in. The back-fill gap scanner is a **local heal only** - it repairs missing embeddings a cluster can recompute from its own corpus and is never relied on for cross-cluster convergence, which replication owns.

Because the trees are ordinary Lattice trees, the whole store inherits per-entry TTL, read-time hiding of expired and tombstoned entries, background tombstone compaction, and (where enabled) cross-cluster replication - the module adds none of these itself.
