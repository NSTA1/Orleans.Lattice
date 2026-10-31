---
agent_spec: "docs/agents/concepts.yaml"
---

# Clocks and version vectors

A version vector tracks causal history per replica, so you can tell whether histories are ordered or concurrent.

<!-- The video block is written by `npm run companions` in videos/; the documentation site replaces it with the player. -->
<!-- video:begin episode="clocks-and-version-vectors" path="how-it-works" order="2" length="2:01" cut="6892ad9e514e" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/clocks-and-version-vectors.html),
> or [download it](../../docs-site/media/clocks-and-version-vectors-6892ad9e514e.mp4)
> (MP4, 2:01, 5.8 MB).

<!-- video:end -->

## Transcript

<!-- The transcript is written from the episode's script by `npm run companions` in videos/. -->
<!-- transcript:begin -->

### Opening

The guarantees showed what a caller can observe; this episode asks how replicas compare histories.

This is Clocks and version vectors: causal order without a shared clock.

### Causal history

Local wall clocks can be skewed. A timestamp alone cannot prove one replica has seen another's write.

A version vector records causal history, not application data. It keeps one logical clock for each replica.

Each replica advances its own entry when it records a noteworthy event. It also carries forward clocks from histories it has observed.

For example, A's vector can read A two, B one; B's can read A one, B three.

Compare each entry. A is ahead on A, and B is ahead on B. Each has information the other lacks, so the histories are concurrent.

Merge by taking the maximum clock in every replica entry: A two, B three. Deliver that vector again, and it does not change.

One vector dominates another when it is at least as high in every entry. It has seen all the history the other contains.

The check is component-wise: compare A with A and B with B, not the total of their numbers. A large value in one entry cannot make up for a smaller one in another. The per-replica positions preserve the causal frontier.

The entries use hybrid logical clocks, but the vector compares each replica's own history rather than relying on one shared wall-clock reading.

In Orleans.Lattice, the typed accessor can tick a named replica and read the vector; the returned vector can test that relation with `DominatesOrEquals`.

The vector gives the application a causal signal; the application still chooses how to resolve concurrent values.

### Where next

Back on Evaluate, next: The evidence. The links are on this episode's page in the documentation.

<!-- transcript:end -->

## The code on screen

The episode shows how to tick a replica's entry and test whether one version vector contains another's causal history. The example is compiled with the documentation.

<!-- video-snippet: clocks-and-version-vectors/frontier-check -->
```csharp verify
var history = tree.VersionVector("doc:12:history");
await history.TickAsync("replica-A", cancellationToken);
VersionVector current = await history.GetAsync(cancellationToken);
var other = new VersionVector();
other.Tick("replica-C");
bool seenEverythingInOther = current.DominatesOrEquals(other);
```

## Where next

<!-- The list is written from the series plan by `npm run companions` in videos/. -->
<!-- where-next:begin -->

- **Back to Evaluate:** The evidence
- **The pages it introduces:** [Version vector](../crdt/versionvector.md)

<!-- where-next:end -->
