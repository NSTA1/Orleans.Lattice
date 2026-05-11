using Orleans.Lattice.BPlusTree.Grains;
namespace Orleans.Lattice.Replication;

/// <summary>
/// Public seam for the intra-cluster snapshot/restore vector-clock
/// reconstruction pass. Operators (or admin tooling) call
/// <see cref="SeedFromTreeAsync"/> once per restored tree to seed the
/// per-tree <c>LocalVectorClock</c> from the
/// <see cref="BPlusTree.LwwEntry.VectorClock"/> slots carried on the
/// restored values.
/// <para>
/// The seam is intra-cluster only. It complements - and is mutually
/// exclusive with - the cross-cluster bootstrap path
/// (<see cref="ILatticeBootstrapCoordinator"/> +
/// <see cref="ISnapshotProvider"/>), which generates a
/// <c>causalStableFrontier</c> from the live high-water-mark table
/// and pins it on the receiver via
/// <see cref="Grains.IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>.
/// The cross-cluster path never reads the value-side
/// <see cref="BPlusTree.LwwEntry.VectorClock"/> slot and therefore is
/// unaffected by the seeder.
/// </para>
/// <para>
/// <b>When to call.</b> An operator snapshots a tree (e.g. via the
/// core library''s <c>TreeSnapshotGrain</c>) and later restores it
/// into the same cluster (possibly at a different timestamp). The
/// restore wipes the per-tree
/// <see cref="Grains.IReplicationHighWaterMarkGrain"/>'s persistent
/// state - the receiver-side per-origin diagonal table that the
/// inbound dependency check reads from - but the restored values
/// still carry their commit-time
/// <see cref="BPlusTree.LwwEntry.VectorClock"/> slot. Without a seed
/// pass, the next inbound apply runs the dependency check against a
/// zeroed local vector and either re-parks legitimate replays
/// against unsatisfied (already-merged) dependencies or accepts
/// out-of-order replays the writer did not author.
/// </para>
/// <para>
/// <b>Side effects.</b> A successful call walks every shard of the
/// tree (via <see cref="BPlusTree.IShardRootGrain"/> and the leaf
/// chain), accumulates the pointwise-max
/// <see cref="Primitives.VersionVector"/> across every non-null
/// <see cref="BPlusTree.LwwEntry.VectorClock"/> slot, then pins the
/// computed frontier on the per-tree HWM grain
/// (<see cref="Grains.IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
/// with <see cref="Primitives.HybridLogicalClock.Zero"/> as the
/// snapshot as-of HLC, since the intra-cluster path has no
/// cross-cluster snapshot HLC concept) and primes the
/// producer-side <see cref="LocalVectorClockCache"/> per-origin so
/// outbound emits read the seeded frontier without a fresh
/// cold-start RPC.
/// </para>
/// <para>
/// <b>No-op for non-replicated trees.</b> When
/// <see cref="ILatticeMergeModeResolver.Resolve"/> returns
/// <see langword="null"/> for the supplied tree (the tree is not
/// listed in
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/>), the call
/// returns immediately with
/// <see cref="LocalVcSeedReport.SeedApplied"/> set to
/// <see langword="false"/>; no leaf walk is performed and no grain
/// state is mutated.
/// </para>
/// </summary>
public interface IReplicationLocalVcSeeder
{
    /// <summary>
    /// Seeds the per-tree local vector clock for
    /// <paramref name="treeName"/> from the values currently
    /// resident on the tree. Returns a
    /// <see cref="LocalVcSeedReport"/> describing the work
    /// performed.
    /// </summary>
    /// <param name="treeName">
    /// The restored tree id. Must be non-null and non-empty. A
    /// tree id that is not configured for replication produces a
    /// no-op report
    /// (<see cref="LocalVcSeedReport.SeedApplied"/> ==
    /// <see langword="false"/>).
    /// </param>
    /// <param name="cancellationToken">
    /// Cancellation token. Observed before any grain dispatch and
    /// flowed through every leaf walk and HWM grain call.
    /// </param>
    Task<LocalVcSeedReport> SeedFromTreeAsync(string treeName, CancellationToken cancellationToken = default);
}