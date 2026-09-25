# Orleans.Lattice in three minutes

Why Orleans.Lattice exists, what it is, and where to start. This is the front
door of the video series. Its first minute is for everyone and uses no
technical terms: what state is, why keeping it in more than one place is hard,
and what Orleans.Lattice does differently. The rest is for developers,
architects and operators: what the platform is, the three positions it takes,
and how a deployment grows from one machine to many regions with the same
programming model. It ends at the three ways into the documentation.

<!-- The video block is written by `npm run companions` in videos/, and the documentation site replaces it with the player. -->
<!-- video:begin episode="introduction" path="front-door" order="1" length="3:01" cut="231f8425d398" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/introduction.html),
> or [download it](../../docs-site/media/introduction-231f8425d398.mp4)
> (MP4, 3:01, 11.6 MB).

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

The usual answer is to make them take turns, with a lock or a vote, and to keep the memory in a separate database, beside the application.

Orleans.Lattice takes a different approach. It keeps the working memory inside the application.

And it records each change so that every place reaches the same answer, in whatever order the changes arrive. Neither waits its turn.

It runs on one machine with no cloud account, and grows to many regions without a rewrite.

### What it is

In technical terms, Orleans.Lattice is a platform for building durable, distributed state systems on Microsoft Orleans.

At its centre is a sorted key-value store that runs inside your own cluster, with no external database, coordinator or queue beside it.

It takes three positions.

### The store lives in the cluster

First: the store lives in the cluster. State is held by grains, in the same cluster as the code that uses it.

So a read is a grain call, not a round trip to a separate database tier with its own scaling and its own failures.

### Conflict resolution is algebraic

Second: conflict resolution is algebraic.

Two clusters write at the same time. Cluster A adds three, and cluster B, concurrently, adds five, each to its own count. Neither state is above the other.

Each merges the other's update by taking the larger value per replica, so both arrive at the same state: their join.

Deliver that update twice, and nothing changes.

Merges are commutative, associative and idempotent.

So any cluster can accept a write to any key, with no lock manager and no consensus round trip. For plain values, the last writer wins.

### Everything else is a seam

Third: everything else is a seam. Storage, identity, governance, replication, administration and observability are companion packages behind documented seams.

A host takes only what it registers, and a capability it leaves out costs nothing.

### Local to Global

So a deployment can grow without a rewrite. Start local: one machine, no cloud account.

Add a team: identity, authorization, schemas and tenants.

Go global: active in every region at once, with backup and an autoscaling signal.

At every stage, the programming model is the same. Your code resolves ILattice, and calls it.

### Three ways in

The documentation has three ways in.

Build, if you are writing code against ILattice.

Evaluate, if you are deciding whether it fits.

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

- **Build**, if you are writing code against `ILattice`: start with the
  [Quick Start](../../README.md#quick-start) and the
  [API reference](../lattice/api.md).
- **Evaluate**, if you are deciding whether it fits: start with
  [What is it?](../../README.md#what-is-it) and
  [Architecture: a core plus seams](../../README.md#architecture-a-core-plus-seams).
- **Operate**, if you are running an estate: start with
  [Configuration](../lattice/configuration.md) and
  [Troubleshooting](../lattice/troubleshooting.md).
