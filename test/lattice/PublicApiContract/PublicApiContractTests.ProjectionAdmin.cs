using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

/// <summary>
/// Public API contract tests for the operator tooling: the
/// shard-scoped projection rebuild and the tree-wide
/// materialiser-lag accessor.
/// <para>
/// <c>RebuildLeafProjectionAsync</c> is verified by writing data into
/// the tree, rebuilding the shard, and asserting the
/// <see cref="ILattice.GetLeafProjectionDigestAsync"/> digest matches
/// the pre-rebuild digest. Two leaves that have applied the same
/// prefix of the same per-shard WAL produce byte-identical digests, so
/// a successful rebuild is observable through that invariant without
/// inspecting internal state.
/// </para>
/// <para>
/// <c>GetMaterialiserLagAsync</c> is verified by asserting that a
/// quiescent tree (every mutation has flushed its projection
/// checkpoint to the WAL head) reports a non-negative lag, that a
/// tree with no shards reports zero, and that the API rejects system
/// trees.
/// </para>
/// </summary>
public partial class PublicApiContractTests
{
    // ── RebuildLeafProjectionAsync ─────────────────────────────────────

    [Test]
    public async Task RebuildLeafProjectionAsync_with_no_data_is_noop()
    {
        var treeId = "pac-rebuild-empty-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        // No writes - shard is empty. The rebuild walks an empty
        // leaf chain (or a single empty root leaf) and returns
        // without throwing.
        Assert.That(async () => await tree.RebuildLeafProjectionAsync(shardIndex: 0),
            Throws.Nothing);
    }

    [Test]
    public async Task RebuildLeafProjectionAsync_preserves_entries_via_WAL_replay()
    {
        var treeId = "pac-rebuild-preserve-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        // Seed enough keys to populate the shard's leaf state and
        // force the projection-digest aggregates to non-zero.
        for (var i = 0; i < 8; i++)
        {
            await tree.SetAsync($"k{i:D2}", Bytes($"v{i:D2}"));
        }

        var preDigest = await tree.GetLeafProjectionDigestAsync(shardIndex: 0);
        var preCount = await tree.CountAsync();

#if LATTICE_DIAG
        var preKeys = new System.Collections.Generic.List<string>();
        await foreach (var k in tree.KeysAsync())
            preKeys.Add(k);
        Orleans.Lattice.BPlusTree.Grains.DiagSink.Write($"[DIAG test-pre] preCount={preCount} preKeys=[{string.Join(',', preKeys)}]");
#endif

        await tree.RebuildLeafProjectionAsync(shardIndex: 0);

        // After rebuild, the materialiser has re-replayed the WAL
        // from offset 0 against a cleared projection. Every key the
        // shard owned pre-rebuild must be visible again and the
        // digest must be byte-identical to its pre-rebuild value
        // (same WAL prefix applied through the same Apply seam).
        var postCount = await tree.CountAsync();
#if LATTICE_DIAG
        var postKeys = new System.Collections.Generic.List<string>();
        await foreach (var k in tree.KeysAsync())
            postKeys.Add(k);
        Orleans.Lattice.BPlusTree.Grains.DiagSink.Write($"[DIAG test-post] postCount={postCount} postKeys=[{string.Join(',', postKeys)}]");
#endif
        Assert.That(postCount, Is.EqualTo(preCount));

        var postDigest = await tree.GetLeafProjectionDigestAsync(shardIndex: 0);
        Assert.That(postDigest.Hash, Is.EqualTo(preDigest.Hash));
        Assert.That(postDigest.EntryCount, Is.EqualTo(preDigest.EntryCount));
    }

    [Test]
    public async Task RebuildLeafProjectionAsync_preserves_individual_reads()
    {
        var treeId = "pac-rebuild-reads-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        await tree.SetAsync("alpha", Bytes("a1"));
        await tree.SetAsync("beta", Bytes("b1"));
        await tree.SetAsync("alpha", Bytes("a2"));

        await tree.RebuildLeafProjectionAsync(shardIndex: 0);

        // LWW semantics: the latest write for alpha must survive
        // the rebuild because the materialiser replays in HLC order.
        Assert.That(Str(await tree.GetAsync("alpha")), Is.EqualTo("a2"));
        Assert.That(Str(await tree.GetAsync("beta")), Is.EqualTo("b1"));
    }

    [Test]
    public async Task RebuildLeafProjectionAsync_rejects_out_of_range_shard_index()
    {
        var treeId = "pac-rebuild-badshard-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 2);

        Assert.That(
            async () => await tree.RebuildLeafProjectionAsync(shardIndex: 99),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task RebuildLeafProjectionAsync_honors_cancellation_token()
    {
        var treeId = "pac-rebuild-cancel-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await tree.RebuildLeafProjectionAsync(shardIndex: 0, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task RebuildLeafProjectionAsync_independently_per_shard()
    {
        var treeId = "pac-rebuild-pershard-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 2);

        for (var i = 0; i < 12; i++)
        {
            await tree.SetAsync($"k{i:D2}", Bytes($"v{i}"));
        }

        var pre0 = await tree.GetLeafProjectionDigestAsync(shardIndex: 0);
        var pre1 = await tree.GetLeafProjectionDigestAsync(shardIndex: 1);

        // Rebuilding shard 0 must not perturb shard 1's projection.
        await tree.RebuildLeafProjectionAsync(shardIndex: 0);

        var post0 = await tree.GetLeafProjectionDigestAsync(shardIndex: 0);
        var post1 = await tree.GetLeafProjectionDigestAsync(shardIndex: 1);

        Assert.That(post0.Hash, Is.EqualTo(pre0.Hash));
        Assert.That(post1.Hash, Is.EqualTo(pre1.Hash));
    }

    // ── GetMaterialiserLagAsync ────────────────────────────────────────

    [Test]
    public async Task GetMaterialiserLagAsync_returns_non_negative_value()
    {
        var treeId = "pac-lag-basic-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 2);

        // Drive a handful of writes so the WAL head advances past
        // zero on at least one shard.
        for (var i = 0; i < 4; i++)
        {
            await tree.SetAsync($"k{i}", Bytes($"v{i}"));
        }

        var lag = await tree.GetMaterialiserLagAsync();
        Assert.That(lag, Is.GreaterThanOrEqualTo(0));
    }

    [Test]
    public async Task GetMaterialiserLagAsync_for_empty_tree_returns_zero()
    {
        var treeId = "pac-lag-empty-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        // No writes at all - WAL head is zero, every leaf's
        // checkpoint is zero, so lag is zero.
        var lag = await tree.GetMaterialiserLagAsync();
        Assert.That(lag, Is.EqualTo(0));
    }

    [Test]
    public async Task GetMaterialiserLagAsync_honors_cancellation_token()
    {
        var treeId = "pac-lag-cancel-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await tree.GetMaterialiserLagAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
