# Clocks and version vectors - script

Status: Fact-checked cue by cue by the Docs agent; API attribution corrected.
The dominance explanation is verified. Timing and delivery still require
narration and review.

Written form, in the series voice (Emma, British English). This episode follows
"The guarantees" and opens with a one-line recap. The version-vector example
uses the notation in the CRDT guide.

## Narration

<!-- pace 0.92 -->

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

## Sources

| Narration | Source |
| --- | --- |
| The guarantees showed what a caller can observe | [The guarantees](../guarantees/SCRIPT.md), operation-by-operation consistency guarantees; H2 is reached from E2 in `videos/series.json`. |
| A local wall-clock timestamp alone cannot prove that one replica saw another's write | [Version Vector](../../../docs/crdt/versionvector.md), "Semantics"; `src/lattice/Primitives/VersionVector.cs`, per-replica clock entries and `DominatesOrEquals`; `src/lattice/Primitives/HybridLogicalClock.cs`, hybrid clock structure. |
| A version vector records causal history, not application data, with one logical clock per replica | [Version Vector](../../../docs/crdt/versionvector.md), "Semantics". |
| Each replica advances its own entry and a vector carries the clocks it has observed | [Version Vector](../../../docs/crdt/versionvector.md), "Semantics" and "Example"; `VersionVector.Tick` in `src/lattice/Primitives/VersionVector.cs`. |
| The example vectors `{A:2,B:1}` and `{A:1,B:3}` are concurrent because each has an entry ahead of the other | [Version Vector](../../../docs/crdt/versionvector.md), "Behaviour". |
| Merge takes the per-entry maximum and duplicate delivery is harmless | [Version Vector](../../../docs/crdt/versionvector.md), "Semantics"; `VersionVector.Merge` in `src/lattice/Primitives/VersionVector.cs`. |
| A vector dominates when every entry is less than or equal to its corresponding entry | `VersionVector.DominatesOrEquals` in `src/lattice/Primitives/VersionVector.cs`. |
| Dominance compares corresponding replica entries, not a total across the vector | `VersionVector.DominatesOrEquals` in `src/lattice/Primitives/VersionVector.cs`; [Version Vector](../../../docs/crdt/versionvector.md), "Behaviour". |
| Entries use hybrid logical clocks; the vector compares per-replica histories | `HybridLogicalClock` in `src/lattice/Primitives/HybridLogicalClock.cs`; `VersionVector.DominatesOrEquals` in `src/lattice/Primitives/VersionVector.cs`. |
| The typed accessor ticks a named replica and reads the vector; the returned vector supports the causal comparison | [Version Vector](../../../docs/crdt/versionvector.md), "Example"; `VersionVector.DominatesOrEquals` in `src/lattice/Primitives/VersionVector.cs`. |
| The application decides how to resolve concurrent changes | [Version Vector](../../../docs/crdt/versionvector.md), "Semantics", which identifies concurrency as a signal for higher-level reconciliation. |
| The ending names the next Evaluate episode | `npm run series -- ending H2`; `videos/series.json`. |
