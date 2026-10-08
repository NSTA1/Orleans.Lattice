---
agent_spec: "docs/agents/concepts.yaml"
---

# Hello, Lattice

Register Lattice on a silo, resolve a tree by name, and write and read typed
values. This is the first episode on the Build path, for developers writing
code against `ILattice`. It adds the package, registers Lattice with in-memory
storage, resolves a tree from a client or a grain, writes and reads a typed
record, and shows what a read, a delete and a get-or-set do when a key is not
there. It ends at the next episode on Build, and the two pages it introduces.

<!-- The video block is written by `npm run companions` in videos/, and the documentation site replaces it with the player. -->
<!-- video:begin episode="hello-lattice" path="build" order="1" length="1:58" cut="01bfa4728958" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/hello-lattice.html),
> or [download it](../../docs-site/media/hello-lattice-01bfa4728958.mp4)
> (MP4, 1:58, 6.5 MB).

<!-- video:end -->

## Transcript

<!-- The transcript is written from the episode's script by `npm run companions` in videos/; edit the script, not this block. -->
<!-- transcript:begin -->

### Hello, Lattice

Hello, Lattice. In this episode you register Lattice on a silo, resolve a tree by name, and write and read typed values.

### Register on a silo

Start with the package, Orleans.Lattice, from NuGet.

Then register it on the silo. One call registers the grain catalogue, the grain storage that its callback supplies, and the log that records every write.

Here the grain storage is in memory, and so, by default, is the log. That suits development and tests.

In production, make both durable: the grain storage that holds the tree's state, and the log. Going durable, later on this path, shows how.

### A tree, by name

Elsewhere, on a client or inside a grain, resolve a tree by name: ask the grain factory for an ILattice, with the tree's name.

Resolution is idempotent. The same name always routes to the same tree.

### Typed values

At the core, keys are strings and values are byte arrays. But the typed extensions serialize for you, so application code rarely touches a byte array.

Set a record under a key, and get it back as the same type.

To choose your own format, pass a serializer of your own. Or write the raw bytes, when you want to own the encoding.

### When a key is not there

Reading a key that is absent, or deleted, returns null.

A delete tombstones the key, and returns whether it was live.

And get or set writes only when the key is absent, or deleted. It returns the value that is already there, or null when it wrote, with no race between the read and the write.

And a scan streams keys back in strict lexicographic order. Scans have an episode of their own.

### Where next

Next on Build: Values that merge. The links are on this episode's page in the documentation.

The Quick start has the setup and the typed code. The API reference has the rest, and it is the contract for each operation: what it returns and what it throws.

<!-- transcript:end -->

## The code on screen

The episode shows the registration and the typed write and read from the
[Quick Start](../../README.md#quick-start), and the absent-key behaviour of
`GetAsync`, `DeleteAsync` and `GetOrSetAsync` from the
[API reference](../lattice/api.md#basic-usage). Each panel is compiled with
the rest of the documentation, so the video cannot show code that does not
build.

Register Lattice on a silo, after `dotnet add package Orleans.Lattice`:

<!-- video-snippet: hello-lattice/register -->
```csharp verify
siloBuilder.AddLattice((silo, storageName) =>
    silo.AddMemoryGrainStorage(storageName));

// The write-ahead log is in memory by default.
// In production, make both storage surfaces durable.
```

Resolve a tree by name:

<!-- video-snippet: hello-lattice/resolve -->
```csharp verify
// On a client or inside a grain:
var lattice = grainFactory.GetGrain<ILattice>("my-tree");
```

Write and read typed values:

<!-- video-snippet: hello-lattice/typed -->
```csharp verify
var lattice = grainFactory.GetGrain<ILattice>("my-tree");

// The typed overloads default to JSON:
await lattice.SetAsync("user/42", new User("Ada", 36));
var user = await lattice.GetAsync<User>("user/42");

// Or own the encoding, with the raw byte[] surface:
await lattice.SetAsync("hello", "world"u8.ToArray());
```

When a key is not there:

<!-- video-snippet: hello-lattice/absent -->
```csharp verify
var lattice = grainFactory.GetGrain<ILattice>("my-tree");
// null when absent or deleted
byte[]? value = await lattice.GetAsync("user/7");
// tombstones the key; true if it was live
bool deleted = await lattice.DeleteAsync("hello");
// writes only when absent; null when it wrote
byte[]? existing = await lattice.GetOrSetAsync(
    "hello", "again"u8.ToArray());
```

## Where next

<!-- The list is written from the series plan (videos/series.json) by `npm run companions` in videos/; edit the plan, not this block. -->
<!-- where-next:begin -->

- **Next on Build:** Values that merge
- **The pages it introduces:** [Quick start](../../README.md#quick-start) and [API reference](../lattice/api.md)

<!-- where-next:end -->
