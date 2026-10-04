using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The receiver bootstrap read fence operations the bootstrap coordinator drives
/// (issue #4526): resolve the shards of the copy a tree routes to, arm or lift
/// the fence on them, and learn whether a migration or resize holds the drain.
/// A seam so the coordinator's state machine is testable without a cluster; the
/// default implementation is <see cref="GrainBootstrapReadFence"/>.
/// </summary>
internal interface IBootstrapReadFence
{
    /// <summary>Resolves the copy <paramref name="treeName"/> routes to and its routed shards.</summary>
    Task<TreeBootstrapReadFence.Shards> ResolveAsync(string treeName);

    /// <summary>Arms or lifts the fence on every shard of <paramref name="shards"/>.</summary>
    Task SetAsync(TreeBootstrapReadFence.Shards shards, bool fenced);

    /// <summary>
    /// Why the drain must wait, or <see langword="null"/> when nothing holds it.
    /// Called only after the fence is armed. Fails closed.
    /// </summary>
    Task<string?> FindBlockerAsync(string treeName, TreeBootstrapReadFence.Shards shards);
}
