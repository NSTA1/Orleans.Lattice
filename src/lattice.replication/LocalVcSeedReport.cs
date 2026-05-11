using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Diagnostic result of a single
/// <see cref="IReplicationLocalVcSeeder.SeedFromTreeAsync"/> call.
/// Returned to the operator (or admin tooling) for observability;
/// the durable side effect of a successful seed is the
/// <see cref="Grains.IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
/// call against the per-tree HWM grain plus the in-memory
/// <see cref="LocalVectorClockCache"/> prime, so this struct is
/// strictly for human / dashboard consumption and is not
/// round-tripped through Orleans serialization.
/// </summary>
/// <param name="TreeName">The tree the seeder targeted.</param>
/// <param name="Frontier">
/// The pointwise-max <see cref="VersionVector"/> computed from every
/// non-<see langword="null"/>
/// <see cref="BPlusTree.LwwEntry.VectorClock"/> slot scanned during
/// the walk, or <see langword="null"/> when
/// <see cref="SeedApplied"/> is <see langword="false"/> (the tree is
/// not replicated). The returned vector is a defensive copy: a
/// caller that mutates it does not affect the
/// <see cref="LocalVectorClockCache"/> the seeder primed nor the
/// HWM grain''s persistent state.
/// </param>
/// <param name="EntriesScanned">
/// Total number of live <see cref="BPlusTree.LwwEntry"/> rows
/// observed across every shard of the tree, including entries whose
/// <see cref="BPlusTree.LwwEntry.VectorClock"/> slot was
/// <see langword="null"/> (legacy persisted state, pre-causal+
/// entries). Provides an order-of-magnitude signal for how much
/// scan work a restore-and-seed pass cost.
/// </param>
/// <param name="SeedApplied">
/// <see langword="true"/> when the seeder pinned the computed
/// frontier on the per-tree HWM grain and primed the producer-side
/// <see cref="LocalVectorClockCache"/>. <see langword="false"/>
/// when the tree is not configured for replication
/// (<see cref="ILatticeMergeModeResolver.Resolve"/> returned
/// <see langword="null"/>); in that case
/// <see cref="EntriesScanned"/> is <c>0</c> and
/// <see cref="Frontier"/> is <see langword="null"/> - the seeder
/// short-circuits before any leaf walk.
/// </param>
public readonly record struct LocalVcSeedReport(
    string TreeName,
    VersionVector? Frontier,
    long EntriesScanned,
    bool SeedApplied);