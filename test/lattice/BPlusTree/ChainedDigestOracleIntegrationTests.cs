using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration oracle for the chained-fold digest path: a shard's
/// <see cref="ILattice.GetLeafProjectionDigestAsync"/> result, which is
/// served by the internal-node subtree aggregate, must match a fresh walk
/// over every leaf of the same shard that XOR-folds each leaf's
/// <c>ProjectionHash</c> the same way the legacy implementation did.
/// <para>
/// This is the regression that catches "running internal hash got out of
/// sync" bugs in the propagation path - the failure mode the chained-fold
/// design explicitly calls out.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class ChainedDigestOracleIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<ILattice> NewTreeAsync(string prefix)
        => await _fixture.CreateTreeAsync($"{prefix}-{Guid.NewGuid():N}");

    /// <summary>
    /// Walks the leaf chain of <paramref name="shardIndex"/> directly,
    /// computing the same XOR-fold over every leaf's
    /// <c>ChildDigestSnapshot</c> the internal-node aggregator is
    /// supposed to maintain incrementally. Returns the running hash,
    /// total entry count, and max-reduced checkpoint offset.
    /// </summary>
    private async Task<(byte[] Hash, long EntryCount, long CheckpointOffset)> WalkLeafChainAsync(
        ILattice tree,
        int shardIndex)
    {
        var grainFactory = _fixture.Cluster.GrainFactory;
        var treeId = tree.GetPrimaryKeyString();
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(treeId);
        var shardKey = $"{physicalTreeId}/{shardIndex}";
        var shard = grainFactory.GetGrain<IShardRootGrain>(shardKey);

        var hash = new byte[16];
        long entryCount = 0;
        long maxCheckpoint = 0;

        var leafId = await shard.GetLeftmostLeafIdAsync();
        while (leafId is { } id)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(id.GetGuidKey());
            var snapshot = await leaf.GetChildDigestSnapshotAsync();
            if (snapshot.Hash is { Length: 16 } leafHash)
            {
                for (var i = 0; i < 16; i++) hash[i] ^= leafHash[i];
            }
            entryCount += snapshot.EntryCount;
            if (snapshot.CheckpointOffset > maxCheckpoint)
                maxCheckpoint = snapshot.CheckpointOffset;

            leafId = await leaf.GetNextSiblingAsync();
        }
        return (hash, entryCount, maxCheckpoint);
    }

    [Test]
    public async Task ChainedDigest_matches_fresh_leaf_walk_after_small_writes()
    {
        var tree = await NewTreeAsync("oracle-small");
        for (var i = 0; i < 8; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        for (var shardIndex = 0; shardIndex < FourShardClusterFixture.TestShardCount; shardIndex++)
        {
            var chained = await tree.GetLeafProjectionDigestAsync(shardIndex);
            var (walkHash, walkEntries, walkOffset) = await WalkLeafChainAsync(tree, shardIndex);

            Assert.That(chained.EntryCount, Is.EqualTo(walkEntries),
                $"shard {shardIndex} entry count must match a fresh walk");
            Assert.That(chained.CheckpointOffset, Is.EqualTo(walkOffset),
                $"shard {shardIndex} checkpoint offset must match a fresh walk");

            // The chained digest folds the aggregated XOR hash plus the
            // entry count and checkpoint into XxHash128. We verify the
            // pre-XxHash aggregate by recomputing the same outer fold
            // from the leaf walk and comparing the final fingerprint.
            var oracle = ComputeOuterDigest(walkHash, walkEntries, walkOffset);
            Assert.That(chained.Hash, Is.EqualTo(oracle),
                $"shard {shardIndex} chained digest must be bit-identical to a fresh leaf walk");
        }
    }

    [Test]
    public async Task ChainedDigest_matches_fresh_leaf_walk_after_splits()
    {
        // FourShardClusterFixture pins MaxLeafKeys=4, so writing 30+
        // keys to a single shard guarantees multiple splits and a
        // non-trivial internal-node topology to exercise multi-level
        // propagation. We write deterministically so most keys hash
        // into the same shard.
        var tree = await NewTreeAsync("oracle-splits");
        for (var i = 0; i < 50; i++)
        {
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"val-{i}"));
        }

        for (var shardIndex = 0; shardIndex < FourShardClusterFixture.TestShardCount; shardIndex++)
        {
            var chained = await tree.GetLeafProjectionDigestAsync(shardIndex);
            var (walkHash, walkEntries, walkOffset) = await WalkLeafChainAsync(tree, shardIndex);
            var oracle = ComputeOuterDigest(walkHash, walkEntries, walkOffset);

            Assert.That(chained.EntryCount, Is.EqualTo(walkEntries),
                $"shard {shardIndex} entry count must match after splits");
            Assert.That(chained.Hash, Is.EqualTo(oracle),
                $"shard {shardIndex} chained digest must match leaf walk after splits");
        }
    }

    [Test]
    public async Task ChainedDigest_matches_fresh_leaf_walk_after_deletes()
    {
        var tree = await NewTreeAsync("oracle-delete");
        for (var i = 0; i < 30; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }
        for (var i = 0; i < 15; i++)
        {
            await tree.DeleteAsync($"k{i:D3}");
        }

        for (var shardIndex = 0; shardIndex < FourShardClusterFixture.TestShardCount; shardIndex++)
        {
            var chained = await tree.GetLeafProjectionDigestAsync(shardIndex);
            var (walkHash, walkEntries, walkOffset) = await WalkLeafChainAsync(tree, shardIndex);
            var oracle = ComputeOuterDigest(walkHash, walkEntries, walkOffset);

            Assert.That(chained.EntryCount, Is.EqualTo(walkEntries),
                $"shard {shardIndex} entry count must match after deletes");
            Assert.That(chained.Hash, Is.EqualTo(oracle),
                $"shard {shardIndex} chained digest must match leaf walk after deletes");
        }
    }

    [Test]
    public async Task ChainedDigest_repeated_polls_remain_stable()
    {
        var tree = await NewTreeAsync("oracle-stable");
        for (var i = 0; i < 20; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        for (var shardIndex = 0; shardIndex < FourShardClusterFixture.TestShardCount; shardIndex++)
        {
            var d1 = await tree.GetLeafProjectionDigestAsync(shardIndex);
            var d2 = await tree.GetLeafProjectionDigestAsync(shardIndex);

            Assert.That(d1.Hash, Is.EqualTo(d2.Hash),
                $"shard {shardIndex} digest must be stable across back-to-back polls");
            Assert.That(d1.EntryCount, Is.EqualTo(d2.EntryCount));
            Assert.That(d1.CheckpointOffset, Is.EqualTo(d2.CheckpointOffset));
        }
    }

    /// <summary>
    /// Recomputes the public digest's outer XxHash128 framing from the
    /// pre-fold aggregate. Mirrors the format <c>BPlusInternalGrain.Digest.ComputePublishedDigest</c>
    /// uses so the oracle compares apples to apples.
    /// </summary>
    private static byte[] ComputeOuterDigest(byte[] xorHash, long entryCount, long checkpointOffset)
    {
        var hasher = new System.IO.Hashing.XxHash128();
        Span<byte> scratch = stackalloc byte[8];
        hasher.Append(xorHash);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(scratch, entryCount);
        hasher.Append(scratch[..8]);
        System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(scratch, checkpointOffset);
        hasher.Append(scratch[..8]);
        return hasher.GetHashAndReset();
    }
}
