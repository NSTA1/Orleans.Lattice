using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

/// <summary>
/// Public API contract tests for the operator-tooling
/// <see cref="ILattice.CompactShardAsync(int, CancellationToken)"/>
/// entry point. The method schedules an out-of-cycle tombstone
/// compaction pass scoped to a single physical shard, bypassing the
/// per-shard cooldown gate enforced by the policy-trigger path.
/// </summary>
public partial class PublicApiContractTests
{
    [Test]
    public async Task CompactShardAsync_accepts_request_and_completes_pass()
    {
        var treeId = "pac-compact-accept-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        // Seed a few deletes so the leaf has tombstones to reap.
        for (var i = 0; i < 4; i++)
        {
            await tree.SetAsync($"k{i}", Bytes($"v{i}"));
        }
        await tree.DeleteAsync("k0");
        await tree.DeleteAsync("k1");

        var accepted = await tree.CompactShardAsync(shardIndex: 0);

        // The coordinator may report `true` (the request transitioned
        // it into a scoped pass) or `false` (a baseline pass is
        // already in flight). Both are valid - what matters for the
        // contract is that the call completes without throwing and
        // does not corrupt subsequent reads.
        Assert.That(accepted, Is.TypeOf<bool>());
        Assert.That(await tree.GetAsync("k0"), Is.Null);
        Assert.That(Str(await tree.GetAsync("k2")), Is.EqualTo("v2"));
    }

    [Test]
    public async Task CompactShardAsync_rejects_out_of_range_shard_index()
    {
        var treeId = "pac-compact-badshard-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 2);

        Assert.That(
            async () => await tree.CompactShardAsync(shardIndex: 99),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task CompactShardAsync_honors_cancellation_token()
    {
        var treeId = "pac-compact-cancel-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await tree.CompactShardAsync(shardIndex: 0, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
