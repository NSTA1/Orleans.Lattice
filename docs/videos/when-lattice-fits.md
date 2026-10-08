---
agent_spec: "docs/agents/concepts.yaml"
---

# When Lattice fits, and when it doesn't

What Orleans.Lattice takes the place of in a system, the three positions it
takes, the kinds of system it composes into, and, as plainly, where it does not
fit. This is the first episode on the Evaluate path, for architects and tech
leads deciding whether it belongs in a system. It begins in plain words, with
an online shop and what it has to remember, and then says it in technical
terms: a store embedded in the cluster, conflict resolution that is algebraic,
and everything else a seam. It ends at the next episode on Evaluate, and the
two pages it introduces.

<!-- The video block is written by `npm run companions` in videos/, and the documentation site replaces it with the player. -->
<!-- video:begin episode="when-lattice-fits" path="evaluate" order="1" length="3:56" cut="0f0e8f63b0e4" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/when-lattice-fits.html),
> or [download it](../../docs-site/media/when-lattice-fits-0f0e8f63b0e4.mp4)
> (MP4, 3:56, 13.7 MB).

<!-- video:end -->

## Transcript

<!-- The transcript is written from the episode's script by `npm run companions` in videos/; edit the script, not this block. -->
<!-- transcript:begin -->

### When Lattice fits

When Lattice fits, and when it doesn't: what it is for, what it takes the place of, and where to reach for something else.

### What a shop remembers

Think of an online shop. It has to remember every basket, and every order.

Usually, that memory is spread across separate products: a database to keep it, a cache to read it quickly, and a queue to pass work along.

Each one is run and repaired on its own, and each has its own idea of what is true. Something has to keep them in agreement.

Orleans.Lattice keeps that memory inside the application instead: one store, with one set of rules.

It fits when many parts of an application, perhaps in many places, change what it remembers, and every place has to reach the same answer.

It does not fit every job. When two places change the same plain value at the same moment, only one change is kept, unless the value is one of the kinds that merge.

### What it is

In technical terms, Orleans.Lattice is a sorted key value store, embedded in your Orleans cluster.

Keys are strings and values are byte arrays, with no external database, coordinator service or queue beside it.

A system is usually assembled from a database, a cache, a queue and a layer of glue, each with its own failure modes and its own consistency story. Against that, it takes three positions.

### The store lives in the cluster

First: the store lives in the cluster. State is held by grains in the same cluster as the code that uses it.

So a read is a grain call, not a network round trip to a separate tier, with its own scaling and its own failures.

And its read cache is part of the store, on each silo. By default it refreshes on every read, so a read sees the latest committed write.

### Conflict resolution is algebraic

Second: conflict resolution is algebraic. Merges are commutative, associative and idempotent.

So convergence needs no distributed lock manager and no consensus round trip, and any cluster can accept a write to any key.

### Everything else is a seam

Third: everything else is a seam. Storage, identity, governance, replication, administration and observability are companion packages, behind documented seams.

A host registers what it needs. A capability it leaves out costs nothing, and each seam can take your own implementation.

### What it composes into

The core alone covers point reads and writes, ordered scans, atomic writes across many keys, typed queues, a distributed lock and a saga coordinator.

Those compose into many kinds of system: knowledge systems, digital twins, distributed control planes, platforms with many tenants, and collaborative applications.

It is a platform rather than a product. It is not tied to one kind of application, and how it is deployed is a configuration decision, taken late.

### Where it does not fit

Now, as plainly, where it does not fit.

It lives in an Orleans cluster. Without one, it has nowhere to run.

Two concurrent writes to a plain value are not merged: the write with the later clock wins. To keep both, use one of its values that merge, such as a counter or a set.

When two atomic writes touch the same keys, the later clock wins, and there is no single order across them. And no one read can see several trees at the same instant.

A value has to fit in one entry of its log, so it is capped by the log's provider and by the log's batch limit. Store a very large value elsewhere, and keep a reference in the tree.

And it is durable only once you make it so. The default log is held in memory, and is lost when the silo restarts. Both the log and the grain storage need a durable home.

### Where next

Next on Evaluate: The guarantees. The links are on this episode's page in the documentation.

In the README, What it is and why it exists sets out the three positions, and A core plus seams, the packages behind them.

<!-- transcript:end -->

## Where next

<!-- The list is written from the series plan (videos/series.json) by `npm run companions` in videos/; edit the plan, not this block. -->
<!-- where-next:begin -->

- **Next on Evaluate:** The guarantees
- **The pages it introduces:** [What it is and why it exists](../../README.md#what-is-it) and [A core plus seams](../../README.md#architecture-a-core-plus-seams)

<!-- where-next:end -->
