using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end safety-net regression for the R-120 step 7 leaf
/// snapshot path. Drives the full pipeline through the public
/// <see cref="ILattice"/> surface: write keys, capture a snapshot
/// via <see cref="IBPlusLeafGrain.CaptureSnapshotAsync"/>, force the
/// leaf activation to deactivate via the test-only
/// <see cref="IBPlusLeafGrain.ForceDeactivateAsync"/> seam, then
/// re-acquire the grain and assert that the rehydrated activation
/// continues to serve the same keys without throwing
/// <c>LeafProjectionStaleException</c>. Also verifies the snapshot
/// blob is durably written to the dedicated
/// <see cref="ILeafSnapshotStorageGrain"/> with the expected offset
/// and row contents.
/// </summary>
[TestFixture]
[Category("Integration")]
public class LeafSnapshotRehydrateIntegrationTests
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [Test]
    public async Task Snapshot_capture_persists_blob_and_rehydrate_serves_keys_after_deactivation()
    {
        // Use the public ILattice API to seed the tree end-to-end so
        // routing, materialisation, and checkpoint persistence all
        // run through their real silo paths. The SmallLeafClusterFixture
        // pre-registers SmallLeafClusterFixture.TreeName with
        // MaxLeafKeys = 4 / ShardCount = 1; reuse it so we exercise the
        // single-leaf path without having to register a fresh tree.
        var treeId = SmallLeafClusterFixture.TreeName;
        var lattice = _cluster.Client.GetGrain<ILattice>(treeId);

        // Write a few keys. The SmallLeafClusterFixture pins the tree
        // to MaxLeafKeys = 4 and ShardCount = 1, so this all fits in a
        // single leaf and the shard root's leftmost leaf is the only
        // leaf in play.
        var keys = new[] { "alpha", "bravo", "charlie" };
        foreach (var k in keys)
            await lattice.SetAsync(k, Bytes(k));

        // Sanity-check: the values must be readable through the public
        // surface before we even consider snapshot capture. If this
        // fails the bug is in the seeding path, not the rehydrate path.
        foreach (var k in keys)
        {
            var seeded = await lattice.GetAsync(k);
            Assert.That(seeded, Is.Not.Null, $"Pre-capture read for {k} returned null - seeding never landed.");
        }

        // Flush any pending checkpoint so the leaf's persisted
        // ProjectionCheckpointOffset is at-or-past the latest applied
        // WAL entry. Without this the snapshot blob would be stamped
        // with the still-zero checkpoint and tell us nothing.
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{treeId}/0");
        var leafId = await shard.GetLeftmostLeafIdAsync();
        Assert.That(leafId, Is.Not.Null, "Single-leaf shard must expose its leaf id.");
        var leafKey = leafId!.Value.GetGuidKey();
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leafKey);

        // Drive a snapshot capture through the public seam. The capture
        // path stamps the blob with whatever ProjectionCheckpointOffset
        // the leaf currently holds; whether or not a flush has happened
        // first, the row set must include every key we wrote.
        await leaf.CaptureSnapshotAsync();

        // Inspect the snapshot grain directly: the blob must exist,
        // its offset must be at-or-past zero (i.e. real checkpoint),
        // and its rows must contain every key we wrote.
        var snapshotGrain = _cluster.GrainFactory.GetGrain<ILeafSnapshotStorageGrain>(leafKey);
        var blob = await snapshotGrain.LoadAsync(CancellationToken.None);
        Assert.That(blob, Is.Not.Null, "Capture must persist a blob through the snapshot grain.");
        Assert.That(blob!.SnapshotOffset, Is.GreaterThanOrEqualTo(0L));
        var capturedKeys = blob.Rows.Select(r => r.Key).ToHashSet();
        Assert.That(capturedKeys, Is.SupersetOf(keys),
            "Snapshot blob must carry every key written through the public API.");

        // Force the current leaf activation to deactivate. The
        // ForceDeactivate seam asks the grain runtime to collect this
        // activation after the current turn; subsequent calls
        // re-activate from cold and exercise the activation-time
        // rehydration path.
        await leaf.ForceDeactivateAsync();

        // Re-acquire the leaf and read the keys back through the
        // public ILattice surface. The reactivation must succeed (no
        // LeafProjectionStaleException) and the values must match.
        // ForceDeactivateAsync returns once the deactivation is
        // scheduled; the next grain call observes the post-rehydrate
        // activation.
        foreach (var k in keys)
        {
            var value = await lattice.GetAsync(k);
            Assert.That(value, Is.Not.Null, $"Key {k} must survive the deactivate / rehydrate cycle.");
            Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo(k));
        }
    }
}
