using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end coverage for issue #4383: removing a leaf through the real tree
/// paths must leave none of its snapshot rows - neither the manifest nor any
/// segment - in grain storage.
/// <para>
/// Before the fix, purge and empty-leaf reclaim cleared each removed leaf's own
/// row and nothing else. Measured on a deployment after an index reset, 18,026
/// manifests had no leaf row: 1.34 GB that nothing references and nothing would
/// ever delete. These tests count rows in the silo's grain storage rather than
/// asking a grain, because a tombstoned manifest reads back as "no snapshot"
/// while its row is still there.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LeafSnapshotStorageCleanupIntegrationTests
{
    private const string ManifestState = "leaf-snapshot";
    private const string SegmentState = "leaf-snapshot-segment";
    private const string LeafState = "leaf";

    private SnapshotStorageCleanupClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SnapshotStorageCleanupClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    /// <summary>
    /// Values large enough that a full leaf's capture exceeds the fixture's
    /// minimum segment window, so the capture is segmented.
    /// </summary>
    private static byte[] Value(int i)
    {
        var value = new byte[24 * 1024];
        Array.Fill(value, (byte)(i % 251));
        return value;
    }

    private async Task<List<GrainId>> WalkChainAsync(string treeName)
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0");
        var chain = new List<GrainId>();
        var leafId = await shard.GetLeftmostLeafIdAsync();
        while (leafId is { } id && chain.Count < 5_000)
        {
            chain.Add(id);
            leafId = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(id).GetNextSiblingAsync();
        }

        return chain;
    }

    private async Task CaptureEveryLeafAsync(IEnumerable<GrainId> leaves)
    {
        foreach (var leaf in leaves)
        {
            await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leaf).CaptureSnapshotAsync();
        }
    }

    /// <summary>
    /// The manifest rows and segment rows stored for <paramref name="leaves"/>.
    /// A manifest is keyed by the leaf's Guid; a segment by the manifest's key
    /// followed by <c>/</c>.
    /// </summary>
    private (GrainId[] Manifests, GrainId[] Segments) SnapshotRowsFor(IEnumerable<GrainId> leaves, IReadOnlyCollection<string> segmentPrefixes)
    {
        var guids = leaves.Select(l => l.TryGetGuidKey(out var g, out _) ? g : Guid.Empty).ToHashSet();
        var manifests = _fixture.Storage.GrainIds(ManifestState)
            .Where(id => id.TryGetGuidKey(out var g, out _) && guids.Contains(g))
            .ToArray();
        var segments = _fixture.Storage.GrainIds(SegmentState)
            .Where(id => segmentPrefixes.Any(p => id.Key.ToString()!.StartsWith(p, StringComparison.Ordinal)))
            .ToArray();
        return (manifests, segments);
    }

    private string[] SegmentPrefixesFor(IEnumerable<GrainId> leaves)
        => PrefixByLeaf(leaves).Values.ToArray();

    /// <summary>
    /// Each captured leaf's segment-key prefix, by leaf Guid. Read while the
    /// manifests still exist, because a segment is found by its manifest's key.
    /// </summary>
    private Dictionary<Guid, string> PrefixByLeaf(IEnumerable<GrainId> leaves)
    {
        var guids = leaves.Select(l => l.TryGetGuidKey(out var g, out _) ? g : Guid.Empty).ToHashSet();
        return _fixture.Storage.GrainIds(ManifestState)
            .Select(id => (Ok: id.TryGetGuidKey(out var g, out _), Guid: g, Id: id))
            .Where(x => x.Ok && guids.Contains(x.Guid))
            .ToDictionary(x => x.Guid, x => x.Id.Key.ToString() + "/");
    }

    [Test]
    public async Task PurgeTreeAsync_leaves_no_snapshot_manifest_or_segment_rows()
    {
        var treeName = $"snapshot-cleanup-purge-{Guid.NewGuid():N}";
        var router = await _fixture.CreateTreeAsync(treeName);
        for (var i = 0; i < 32; i++)
        {
            await router.SetAsync($"k{i:D3}", Value(i));
        }

        var leaves = await WalkChainAsync(treeName);
        await CaptureEveryLeafAsync(leaves);
        var prefixes = SegmentPrefixesFor(leaves);
        var before = SnapshotRowsFor(leaves, prefixes);
        Assert.Multiple(() =>
        {
            Assert.That(leaves, Has.Count.GreaterThan(4), "precondition: the seed must have split the tree");
            Assert.That(before.Manifests, Is.Not.Empty, "precondition: the leaves must hold snapshot manifests");
            Assert.That(before.Segments, Is.Not.Empty,
                "precondition: at least one capture must be segmented, or segment cleanup is never exercised");
        });

        await router.DeleteTreeAsync();
        await router.PurgeTreeAsync();
        var status = await _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeName).GetDeletionStatusAsync();
        Assert.That(status.PurgeComplete, Is.True, "precondition: the purge must have completed");

        var after = SnapshotRowsFor(leaves, prefixes);
        Assert.Multiple(() =>
        {
            Assert.That(after.Manifests, Is.Empty,
                "THE ASSERTION. A purged tree's snapshot manifests must be deleted; before the fix every one stayed");
            Assert.That(after.Segments, Is.Empty, "and so must every snapshot segment row");
        });

        // Issue #4419: the deactivation each clear requests must not write the
        // leaf's row back.
        await AssertLeafRowsStayDeletedAsync(leaves);
    }

    [Test]
    public async Task ReclaimEmptyLeavesAsync_deletes_the_snapshot_rows_of_every_folded_leaf_and_only_those()
    {
        var treeName = $"snapshot-cleanup-reclaim-{Guid.NewGuid():N}";
        var router = await _fixture.CreateTreeAsync(treeName);
        for (var i = 0; i < 48; i++)
        {
            await router.SetAsync($"k{i:D3}", Value(i));
        }

        var grown = await WalkChainAsync(treeName);
        await CaptureEveryLeafAsync(grown);
        var prefixByLeaf = PrefixByLeaf(grown);
        var prefixes = prefixByLeaf.Values.ToArray();
        Assert.That(SnapshotRowsFor(grown, prefixes).Manifests, Is.Not.Empty,
            "precondition: the leaves must hold snapshot manifests");

        await router.DeleteRangeAsync("k012", "k036");
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{treeName}/0");
        var reclaimed = await shard.ReclaimEmptyLeavesAsync(int.MaxValue);
        Assert.That(reclaimed, Is.GreaterThan(0), "precondition: the emptied range must yield reclaimable leaves");

        var surviving = await WalkChainAsync(treeName);
        var folded = grown.Except(surviving).ToArray();
        Assert.That(folded, Has.Length.EqualTo(reclaimed), "precondition: the chain shrank by the reported count");

        var foldedPrefixes = folded
            .Select(f => f.TryGetGuidKey(out var g, out _) && prefixByLeaf.TryGetValue(g, out var p) ? p : null)
            .OfType<string>()
            .ToArray();
        Assert.That(foldedPrefixes, Is.Not.Empty, "precondition: at least one folded leaf had a captured snapshot");
        var foldedRows = SnapshotRowsFor(folded, foldedPrefixes);
        var survivingRows = SnapshotRowsFor(surviving, prefixes);
        Assert.Multiple(() =>
        {
            Assert.That(foldedRows.Manifests, Is.Empty,
                "THE ASSERTION. A folded leaf's snapshot manifest must be deleted with it");
            Assert.That(foldedRows.Segments, Is.Empty, "and so must its segment rows");
            Assert.That(survivingRows.Manifests, Is.Not.Empty,
                "a leaf still in the tree keeps its snapshot: the fix must not over-delete");
        });

        // Issue #4419: a folded leaf's deactivation must not write its row back.
        await AssertLeafRowsStayDeletedAsync(folded);
    }

    /// <summary>
    /// Waits for the deactivations the clears requested to finish, then checks
    /// that none of <paramref name="removed"/> has a <c>leaf</c> row (issue #4419).
    /// </summary>
    private async Task AssertLeafRowsStayDeletedAsync(IReadOnlyCollection<GrainId> removed)
    {
        var removedSet = removed.ToHashSet();
        var management = _cluster.GrainFactory.GetGrain<IManagementGrain>(0);
        await TestPoll.UntilAsync(
            async () => !(await management.GetActiveGrains(removed.First().Type)).Any(removedSet.Contains),
            "every removed leaf's activation to finish deactivating",
            timeout: TimeSpan.FromSeconds(60));
        Assert.That(_fixture.Storage.GrainIds(LeafState).Where(removedSet.Contains), Is.Empty,
            "a removed leaf's own row must stay deleted; the deactivation after the clear used to write it back "
            + "as a stub with no tree id");
    }
}
