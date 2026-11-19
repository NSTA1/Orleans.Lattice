using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests.Fakes;

/// <summary>
/// Test-only per-shard projection-digest source. The digest probe reads each
/// shard's digest straight from its <see cref="IShardRootGrain"/> rather than
/// through the tree's <see cref="ILattice"/> forwarder, so probe fixtures stub
/// this seam and wire every shard-root substitute to delegate to it by shard
/// index (see <see cref="ShardRootDigestStubs"/>). Keeping it distinct from the
/// tree's <see cref="ILattice"/> substitute lets a test assert the forwarder is
/// never asked for a digest.
/// </summary>
public interface IShardDigestStub
{
    /// <summary>Returns the projection digest of physical shard <paramref name="shardIndex"/>.</summary>
    /// <param name="shardIndex">The physical shard index parsed from the shard-root key.</param>
    /// <param name="cancellationToken">Cancellation token passed by the probe.</param>
    Task<LeafProjectionDigest> GetLeafProjectionDigestAsync(int shardIndex, CancellationToken cancellationToken);
}
