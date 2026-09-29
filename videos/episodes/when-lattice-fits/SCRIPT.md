# When Lattice fits, and when it doesn't - script

Status: fact-checked against the corpus by the Docs agent; 5 narration corrections and 4 Sources table corrections applied.

Written form, in the series voice (Emma, British English). The pronunciation
lexicon (`voice/lexicon.json`) handles "Orleans"; captions keep the text
exactly as written here.

The first part is for everyone, and uses no technical terms: what an online
shop has to remember, how that memory is usually kept, what Orleans.Lattice
does instead, and the plainest case where it does not fit. "In technical
terms" marks where the rest begins. As the first episode on Evaluate, it has no
recap.

Each paragraph under "Narration" is one cue, and each cue is a beat its scene
can move on. A `pause` comment adds that many seconds of silence before the
next cue, or after the last one.

## Narration

<!-- pause 1.0 -->

### When Lattice fits

When Lattice fits, and when it doesn't: what it is for, what it takes the place of, and where to reach for something else.

### What a shop remembers

Think of an online shop. It has to remember every basket, and every order.

Usually, that memory is spread across separate products: a database to keep it, a cache to read it quickly, and a queue to pass work along.

Each one is run and repaired on its own, and each has its own idea of what is true. Something has to keep them in agreement.

Orleans.Lattice keeps that memory inside the application instead: one store, with one set of rules.

It fits when many parts of an application, perhaps in many places, change what it remembers, and every place has to reach the same answer.

It does not fit every job. When two places change the same plain value at the same moment, only one change is kept, unless the value is one of the kinds that merge.

<!-- pause 1.0 -->

### What it is

In technical terms, Orleans.Lattice is a sorted key value store, embedded in your Orleans cluster.

Keys are strings and values are byte arrays, with no external database, coordinator service or queue beside it.

A system is usually assembled from a database, a cache, a queue and a layer of glue, each with its own failure modes and its own consistency story. Against that, it takes three positions.

<!-- pause 0.6 -->

### The store lives in the cluster

First: the store lives in the cluster. State is held by grains in the same cluster as the code that uses it.

So a read is a grain call, not a network round trip to a separate tier, with its own scaling and its own failures.

And its read cache is part of the store, on each silo. By default it refreshes on every read, so a read sees the latest committed write.

<!-- pause 0.6 -->

### Conflict resolution is algebraic

Second: conflict resolution is algebraic. Merges are commutative, associative and idempotent.

So convergence needs no distributed lock manager and no consensus round trip, and any cluster can accept a write to any key.

<!-- pause 0.6 -->

### Everything else is a seam

Third: everything else is a seam. Storage, identity, governance, replication, administration and observability are companion packages, behind documented seams.

A host registers what it needs. A capability it leaves out costs nothing, and each seam can take your own implementation.

<!-- pause 0.6 -->

### What it composes into

The core alone covers point reads and writes, ordered scans, atomic writes across many keys, typed queues, a distributed lock and a saga coordinator.

Those compose into many kinds of system: knowledge systems, digital twins, distributed control planes, platforms with many tenants, and collaborative applications.

It is a platform rather than a product. It is not tied to one kind of application, and how it is deployed is a configuration decision, taken late.

<!-- pause 1.0 -->

### Where it does not fit

Now, as plainly, where it does not fit.

It lives in an Orleans cluster. Without one, it has nowhere to run.

Two concurrent writes to a plain value are not merged: the write with the later clock wins. To keep both, use one of its values that merge, such as a counter or a set.

When two atomic writes touch the same keys, the later clock wins, and there is no single order across them. And no one read can see several trees at the same instant.

A value has to fit in one entry of its log, so it is capped by the log's provider and by the log's batch limit. Store a very large value elsewhere, and keep a reference in the tree.

And it is durable only once you make it so. The default log is held in memory, and is lost when the silo restarts. Both the log and the grain storage need a durable home.

<!-- pause 1.0 -->

### Where next

Next on Evaluate: The guarantees. The links are on this episode's page in the documentation.

In the README, What it is and why it exists sets out the three positions, and A core plus seams, the packages behind them.

<!-- pause 2.0 -->

## Sources

Each line of narration traces to one of these. Where the script is narrower
than its source, or says it in plainer words, the reason is given.

| Narration | Source |
| --- | --- |
| what it is for, what it takes the place of, and where to reach for something else | the episode's own structure: "What it is", the three positions, "Where it does not fit" |
| an online shop has to remember every basket and every order | the plain-language framing of README, "Why it exists" (a durable distributed system's state); the example is illustrative, and its keys (`basket/ada`, `order/1001`) are the ones the technical half's scenes draw |
| usually spread across separate products: a database to keep it, a cache to read it quickly, a queue to pass work along; each run and repaired on its own, each with its own idea of what is true | README, "Why it exists" ("usually assembled from a database, a cache, a queue, an identity provider, and a layer of glue - each with its own operational model, its own failure modes, and its own consistency story to reconcile with the others"), in plain words |
| Orleans.Lattice keeps that memory inside the application: one store, one set of rules | README, "What is it?" ("embedded in your Orleans cluster ... No external database, no coordinator service, no external queue"); "one set of rules" is the plain form of the single consistency contract (`docs/lattice/consistency.md`) set against the several stories the README says must be reconciled |
| it fits when many parts of an application, perhaps in many places, change what it remembers, and every place has to reach the same answer | README, "Why it exists" (algebraic merges, "what makes active-active writes across regions tractable") and "What you can build", Collaborative applications ("any cluster may write any key, with deterministic convergence") |
| when two places change the same plain value at the same moment, only one change is kept, unless the value is one of the kinds that merge | README, "What is it?" ("provided you use its CRDT Primitives"); `docs/lattice/consistency.md`, "Clock skew" ("Two concurrent writes resolve by HLC: the write with the later wall-clock tick wins"); `docs/lattice/state-primitives.md`, "Opt-in CRDT values" |
| a sorted key value store, embedded in your Orleans cluster | README, "What is it?". "Durable" is left off the store, as the README's own opening summary of it leaves it off ("At its centre is a sorted, horizontally-scalable, conflict-free key-value store"), because the default write-ahead log is in memory (`docs/lattice/wal-storage-providers.md`, `InMemoryWalStorageProvider`), which the last limit says |
| keys are strings and values are byte arrays; no external database, coordinator service or queue beside it | README, "What is it?" ("Keys are `string`, values are `byte[]` ... No external database, no coordinator service, no external queue") |
| a system is usually assembled from a database, a cache, a queue and a layer of glue, each with its own failure modes and its own consistency story; three positions | README, "Why it exists" |
| the store lives in the cluster; state is held by grains in the same cluster as the code that uses it; a read is a grain call, not a network round trip to a separate tier with its own scaling and failures | README, "Why it exists", first position ("State is held by grains in the same cluster as the code using it, so a read is a grain call rather than a network round trip to a separate tier with its own scaling and failure envelope") |
| its read cache is part of the store, on each silo; by default it refreshes on every read, so a read sees the latest committed write | `docs/lattice/consistency.md`, "Read-cache staleness" ("the per-silo read cache ... default `TimeSpan.Zero` - refresh on every read") and "Single-key operations" (`GetAsync` is linearizable under the default: "the call observes the latest committed value"); `docs/lattice/caching.md` |
| conflict resolution is algebraic; commutative, associative and idempotent; no distributed lock manager and no consensus round trip; any cluster can accept a write to any key | README, "What is it?" and "Why it exists", second position |
| everything else is a seam; storage, identity, governance, replication, administration and observability are companion packages behind documented seams | README, "Why it exists", third position ("companion packages that plug into documented extension points"), and "Architecture: a core plus seams" |
| a host registers what it needs; a capability it leaves out costs nothing; each seam can take your own implementation | README, "Architecture: a core plus seams" ("A host composes the platform it needs by registering packages"; "Opt-in cost"; "Substitutable implementations") |
| the core alone covers point reads and writes, ordered scans, atomic writes across many keys, typed queues, a distributed lock and a saga coordinator | README, "What is it?", "The core alone supports"; each is in the released core (`lattice-v9.8.0`: `docs/lattice/queues.md`, `distributed-lock.md`, `atomic-action.md`) |
| knowledge systems, digital twins, distributed control planes, platforms with many tenants, collaborative applications | README, "What you can build". AI memory is left out because it rests on vector search, which has not shipped, and search and indexing for length; "platforms with many tenants" is "Multi-tenant SaaS platforms", said without the hyphen the voice pauses at, and rests on the released tenancy package |
| a platform rather than a product; not tied to one kind of application; how it is deployed is a configuration decision taken late | README, "Why it exists" ("a platform rather than a product: it is not tied to one application category, and the deployment topology is a configuration decision taken late, not an architecture decision taken up front") |
| it lives in an Orleans cluster; without one, it has nowhere to run | README, "What is it?" ("embedded in your Orleans cluster") and "Why it exists" ("an ordered, durable, shardable store to put underneath" Orleans grains) |
| two concurrent writes to a plain value are not merged; the write with the later clock wins; to keep both, use one of its values that merge, such as a counter or a set | `docs/lattice/consistency.md`, "Clock skew"; `docs/lattice/state-primitives.md`, "Last-Writer-Wins Register (LWW)", "Opt-in CRDT values", and its counters and sets; README, "What is it?" ("provided you use its CRDT Primitives") |
| when two atomic writes touch the same keys, the later clock wins, and there is no single order across them; no one read can see several trees at the same instant | `docs/lattice/consistency.md`, "What Lattice does not guarantee" ("Global transaction ordering. Two concurrent `SetManyAtomicAsync` calls touching overlapping keys resolve pairwise by LWW; there is no serializable global order across sagas"); `docs/lattice/atomic-writes.md` and "Cross-tree (multi-tree) atomic visibility" ("Lattice has no single read operation spanning multiple trees") |
| a value has to fit in one entry of its log, capped by the log's provider and by its batch limit; store a very large value elsewhere and keep a reference in the tree | `docs/lattice/tree-storage.md`, "Sizing surface 2 - WAL row", and "Sizing the WAL row for a provider" (one WAL record per mutation, sized against the WAL provider's per-row limit - 64 KiB per binary property on Azure Table; "either pick a higher-capacity WAL provider ... or store the large value out-of-band and write only a reference to the tree"). The batch-limit clause is not borne out, and needs a re-cut: `LatticeOptions.WalMaxBatchBytes` "does not cap a single record" - "a record larger than the whole budget is flushed alone rather than refused" (the same page, and the option's own documentation) |
| durable only once you make it so; the default log is held in memory and is lost when the silo restarts; both the log and the grain storage need a durable home | `docs/lattice/wal-storage-providers.md`, "Registering a provider" (the default registration is `InMemoryWalStorageProvider`) and `InMemoryWalStorageProvider` ("State is kept entirely in process memory and is lost on silo restart"); `docs/lattice/wal.md` ("not crash-safe"); README, "The deployment journey", Local ("The in-memory WAL is the default if you do not need durability yet"); README, "Quick start" (make "both storage surfaces durable") |
| Next on Evaluate: The guarantees; the links are on this episode's page | the plan's ending for E1 (`npm run series -- ending E1`) |
| What it is and why it exists sets out the three positions; A core plus seams, the packages behind them | the two pages the plan says this episode introduces: README, "What is it?" with "Why it exists", and "Architecture: a core plus seams" |
