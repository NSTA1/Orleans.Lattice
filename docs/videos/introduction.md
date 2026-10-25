---
agent_spec: "docs/agents/concepts.yaml"
---

# Orleans.Lattice in three minutes

Why Orleans.Lattice exists, what it is, and where to start. This is the front
door of the video series. Its first minute is for everyone and uses no
technical terms: what state is, why keeping it in more than one place is hard,
and what Orleans.Lattice does differently. The rest is for developers,
architects and operators: what the platform is, the three positions it takes,
and how a deployment grows from one machine to many regions with the same
programming model. It ends at the three ways into the documentation.

<!-- The video block is written by `npm run companions` in videos/, and the documentation site replaces it with the player. -->
<!-- video:begin episode="introduction" path="front-door" order="1" length="2:56" cut="9580eb3dabaa" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/introduction.html),
> or [download it](../../docs-site/media/introduction-9580eb3dabaa.mp4)
> (MP4, 2:56, 11.0 MB).

<!-- video:end -->

## Transcript

<!-- The transcript is written from the episode's script by `npm run companions` in videos/; edit the script, not this block. -->
<!-- transcript:begin -->

### Opening

Orleans.Lattice, in three minutes: why it exists, what it is, and where to start.

### Software that remembers

Almost every application has to remember things: what is in a basket, who may open a document, how often a page is viewed.

Developers call that memory state.

### In more than one place

Many applications run in more than one place at once, so that they keep going if one place fails.

Then two places can change the same thing at the same moment.

Usually, changes take turns, with a lock or a vote, and memory lives in a separate database.

Orleans.Lattice takes a different approach. It keeps the working memory inside the application.

And it records each change so that every place reaches the same answer, in whatever order the changes arrive. Neither waits its turn.

It runs on one machine with no cloud account, and grows to many regions without a rewrite.

### What it is

In technical terms, Orleans.Lattice is a platform for building durable, distributed state systems on Microsoft Orleans.

At its centre is a sorted key-value store inside your own cluster: no external database, coordinator or queue.

It takes three positions.

### The store lives in the cluster

First: state lives in grains, in the same cluster as the code that uses it.

A read is a grain call, not a round trip to a separate database tier with its own scaling and failures.

### Conflict resolution is algebraic

Second: conflict resolution is algebraic.

Cluster A adds three, cluster B concurrently adds five, each to its own count. Neither state is above the other.

Each merges the other's update by taking the larger value per replica, so both arrive at the same state: their join.

Deliver that update twice, and nothing changes.

Merges are commutative, associative and idempotent.

So any cluster can accept a write to any key, with no lock manager and no consensus round trip. For plain values, the last writer wins.

### Everything else is a seam

Third: everything else is a seam. Storage, identity, governance, replication, administration and observability are companion packages behind documented seams.

A host takes only what it registers, and a capability it leaves out costs nothing.

### Local to Global

Start local: one machine, no cloud account.

Add a team: identity, authorization, schemas and tenants.

Go global: active in every region at once, with backup and an autoscaling signal.

At every stage, the programming model is the same. Your code resolves ILattice, and calls it.

### Three ways in

The documentation has three ways in.

Build, if you are writing code against ILattice: start with Hello, Lattice.

Evaluate, if you are deciding whether it fits: start with When Lattice fits, and when it doesn't.

Operate, if you are running an estate. You choose.

<!-- transcript:end -->

## The code on screen

The episode shows the typed write and read from the
[Quick Start](../../README.md#quick-start). It is compiled with the rest of the
documentation, so the video cannot show code that does not build.

<!-- video-snippet: introduction/same-code -->
```csharp verify
var lattice = grainFactory.GetGrain<ILattice>("my-tree");
await lattice.SetAsync("user/42", new User("Ada", 36));
var user = await lattice.GetAsync<User>("user/42");
```

## Where next

<!-- The list is written from the series plan (videos/series.json) by `npm run companions` in videos/; edit the plan, not this block. -->
<!-- where-next:begin -->

- **Build**, if you are writing code against `ILattice`: watch [Hello, Lattice](hello-lattice.md), then read [Quick start](../../README.md#quick-start) and [API reference](../lattice/api.md).
- **Evaluate**, if you are deciding whether it fits: watch [When Lattice fits, and when it doesn't](when-lattice-fits.md), then read [What it is and why it exists](../../README.md#what-is-it) and [A core plus seams](../../README.md#architecture-a-core-plus-seams).
- **Operate**, if you are running an estate: read [Configuration](../lattice/configuration.md), [WAL](../lattice/wal.md) and [WAL storage providers](../lattice/wal-storage-providers.md).

<!-- where-next:end -->
