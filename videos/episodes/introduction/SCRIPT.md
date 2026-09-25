# Orleans.Lattice in three minutes - script

Status: fact-checked on 2026-09-24 by the Docs agent, cue by cue, against the
README, the reference architecture, the site's home page, the G-Counter join
figure, `PACKAGES.md`, the package documentation, the source and the release
tags: the technical half first, then the plain-language opening. Its fixes are
applied to both.

Written form, in the series voice (Emma, British English). The pronunciation
lexicon (`voice/lexicon.json`) handles "Orleans" and "ILattice", and, for the
Kokoro engine, "Cluster A", "idempotent" and "lives"; captions keep the text
exactly as written here.

The first minute is for everyone, and uses no technical terms: what state is,
why keeping it in many places at once is hard, and what Orleans.Lattice does
differently. "In technical terms" marks where the rest begins.

Each paragraph under "Narration" is one cue, and each cue is a beat its scene
can move on. A `pause` comment adds that many seconds of silence before the
next cue, or after the last one.

## Narration

<!-- pause 1.0 -->

### Opening

Orleans.Lattice, in three minutes: why it exists, what it is, and where to start.

### Software that remembers

Almost every application has to remember things: what is in a basket, who may open a document, how often a page is viewed.

Developers call that memory state.

### In more than one place

Many applications run in more than one place at once, so that they keep going if one place fails.

Then two places can change the same thing at the same moment.

The usual answer is to make them take turns, with a lock or a vote, and to keep the memory in a separate database, beside the application.

Orleans.Lattice takes a different approach. It keeps the working memory inside the application.

And it records each change so that every place reaches the same answer, in whatever order the changes arrive. Neither waits its turn.

It runs on one machine with no cloud account, and grows to many regions without a rewrite.

### What it is

In technical terms, Orleans.Lattice is a platform for building durable, distributed state systems on Microsoft Orleans.

At its centre is a sorted key-value store that runs inside your own cluster, with no external database, coordinator or queue beside it.

It takes three positions.

<!-- pause 1.2 -->

### The store lives in the cluster

First: the store lives in the cluster. State is held by grains, in the same cluster as the code that uses it.

So a read is a grain call, not a round trip to a separate database tier with its own scaling and its own failures.

### Conflict resolution is algebraic

Second: conflict resolution is algebraic.

Two clusters write at the same time. Cluster A adds three, and cluster B, concurrently, adds five, each to its own count. Neither state is above the other.

Each merges the other's update by taking the larger value per replica, so both arrive at the same state: their join.

Deliver that update twice, and nothing changes.

Merges are commutative, associative and idempotent, so any cluster can accept a write to any key, with no lock manager and no consensus round trip. For plain values, the last writer wins.

### Everything else is a seam

Third: everything else is a seam. Storage, identity, governance, replication, administration and observability are companion packages behind documented seams.

A host takes only what it registers, and a capability it leaves out costs nothing.

### Local to Global

So a deployment can grow without a rewrite. Start local: one machine, no cloud account.

Add a team: identity, authorization, schemas and tenants.

Go global: active-active across regions, with backup and an autoscaling signal.

At every stage, the programming model is the same. Your code resolves ILattice, and calls it.

### Three ways in

The documentation has three ways in.

Build, if you are writing code against ILattice.

Evaluate, if you are deciding whether it fits.

Operate, if you are running an estate. Pick your way in.

<!-- pause 2.0 -->

## Sources

Each line of narration traces to one of these. Where the script is narrower
than its source, or says it in plainer words, the reason is given.

| Narration | Source |
| --- | --- |
| why it exists, what it is, and where to start | the episode's own structure: "In more than one place", "What it is", "Three ways in" |
| an application remembers things; that memory is its state | the plain-language framing of README, opening paragraph ("durable, distributed state systems") and "Why it exists" ("State is held by grains"); the examples are illustrative, and the third is the count the next scene uses. A count of page views has no limit to overshoot, which a count of tickets sold would (`docs/lattice.tenancy/README.md`, "bounded, quantified overshoot") |
| many applications run in more than one place at once, so that they keep going if one place fails | README, "The deployment journey", Global ("Multiple regions, each serving reads and writes"); reference-architecture.md, "Disaster recovery" ("Live peers keep serving") |
| two places can change the same thing at the same moment | README, "Why it exists" ("active-active writes across regions"); `docs-site/figures/join-figures.json`, `gcounter` ("cluster B, concurrently") |
| the usual answer is to take turns, with a lock or a vote, and to keep the memory in a separate database beside the application | `docs/crdt/readme.md` ("never needs a lock, a coordinator, or a vote"); README, "What is it?" ("lock-based or consensus-based") and "Why it exists" ("usually assembled from a database, a cache, a queue ..."; "a network round trip to a separate tier") |
| Orleans.Lattice keeps the working memory inside the application | the site's thesis ("State that lives in your Orleans cluster"); README, "What is it?" ("embedded in your Orleans cluster", "No external database"). "Working" is deliberate: a durable deployment writes each change to a log on local disk or in an Azure storage account (`docs/lattice.storage.file/README.md`; reference-architecture.md, "Durable WAL"), and the working copy is held in memory and rebuilt from that log (`docs/lattice/architecture.md`, `docs/lattice/wal.md`) |
| it records each change; every place reaches the same answer, in whatever order the changes arrive; neither waits its turn | `docs/lattice/wal.md` (an append-only log, which replication also consumes); reference-architecture.md, "Consistency scoping" ("Convergence is deterministic and independent of message arrival order", for plain values as well as CRDT values); README, "Why it exists" ("convergence needs no distributed lock manager and no consensus round trip"); `gcounter` ("Either order of delivery reaches the same join"). The picture's 28 is a counter's answer: with plain values both places settle on the later write, as the technical half says |
| it runs on one machine with no cloud account, and grows to many regions without a rewrite | README, opening paragraphs ("A complete deployment runs on a single machine with no cloud dependency") and "The deployment journey" ("One machine, no cloud account"; "not a rewrite"; Global, "Multiple regions") |
| a platform for building durable, distributed state systems on Microsoft Orleans | README, opening paragraph |
| a sorted key-value store that runs inside your own cluster; no external database, coordinator or queue | README, "What is it?". "Durable" is kept for the platform and left off the store, because the default write-ahead log is in memory and not crash-safe (README, "The deployment journey"; `docs/lattice/wal.md`) |
| three positions | README, "Why it exists" |
| state is held by grains in the same cluster as the code that uses it; a read is a grain call, not a round trip to a separate database tier | README, "Why it exists", first position. The README says "the same process"; the script says "the same cluster", because the shard and leaf grains may be placed on any silo (`docs/lattice/architecture.md`) |
| conflict resolution is algebraic; commutative, associative and idempotent; no lock manager and no consensus round trip; any cluster can accept a write to any key | README, "What is it?" and "Why it exists" |
| Cluster A adds three and cluster B five, each to its own count; neither state is above the other; the larger value per replica; the join; delivering that update twice changes nothing | `docs-site/figures/join-figures.json`, `gcounter`, mirroring `docs/crdt/gcounter.md` |
| for plain values, the last writer wins | README, "What is it?" ("provided you use its CRDT Primitives"); `docs/crdt/readme.md`; `docs/lattice/state-primitives.md` |
| storage, identity, governance, replication, administration and observability are companion packages behind documented seams; a host takes only what it registers; a capability it leaves out costs nothing | the site's "A core plus seams" lede, word for word; README, "Architecture: a core plus seams" |
| Local: one machine, no cloud account; Team: identity, authorization, schemas, tenants; Global: active-active across regions, backup, an autoscaling signal | README, "The deployment journey". Lattice publishes the scaling signal; an external autoscaler such as KEDA acts on it (`docs/lattice.scaling/README.md`) |
| the programming model is the same at every stage; your code resolves ILattice and calls it | the site's "One programming model, Local to Global" ("ILattice unchanged at every stage"); README, "The deployment journey" |
| the documentation has three ways in: Build, Evaluate and Operate, and who each is for | the site's "Three ways in" |
