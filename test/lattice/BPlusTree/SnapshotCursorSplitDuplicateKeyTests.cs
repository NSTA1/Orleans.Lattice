using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression for issue #907: snapshot-isolated scans
/// (<c>OpenSnapshotEntryCursorAsync</c> / <c>OpenSnapshotKeyCursorAsync</c>)
/// must not surface donor-orphan keys after an adaptive shard split.
/// <para>
/// A split drains a range of virtual slots from the source shard to a new
/// target shard, swaps the routing map, and leaves the source shard in a
/// permanent reject phase while physically retaining its pre-split copy of
/// every migrated key. The live scan path reconciles those orphans against
/// the source shard's <c>MovedAwaySlots</c>; before the fix the snapshot
/// path did not, so every migrated key was surfaced twice - once by the
/// donor's snapshot leaf (its stale pre-split copy) and once by the target's
/// (the live copy) - and a key updated or deleted on the target after the
/// split could be masked by the donor's stale orphan.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public class SnapshotCursorSplitDuplicateKeyTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private async Task CommitSplitAsync(string treeId, int sourceShardIndex = 0)
    {
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>(
            $"{treeId}/{sourceShardIndex}");
        await split.SplitAsync(sourceShardIndex);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True,
            $"Manual split of shard {sourceShardIndex} must complete.");
    }

    /// <summary>
    /// Finds the seeded keys whose owning physical shard changed across the
    /// split - i.e. the keys for which the donor shard now retains an orphan
    /// copy. Drives the value-correctness assertions below.
    /// </summary>
    private async Task<List<string>> FindMovedKeysAsync(string treeId, IEnumerable<string> seededKeys)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var postMap = await registry.GetShardMapAsync(treeId);
        Assert.That(postMap, Is.Not.Null, "The split must publish a routing map.");
        var preMap = ShardMap.CreateDefault(
            LatticeConstants.DefaultVirtualShardCount, FourShardClusterFixture.TestShardCount);

        var moved = new List<string>();
        foreach (var key in seededKeys)
        {
            if (preMap.Resolve(key) != postMap!.Resolve(key))
                moved.Add(key);
        }
        return moved;
    }

    [Test]
    public async Task Snapshot_entry_cursor_returns_no_duplicate_keys_after_shard_split()
    {
        var treeId = $"snap-split-dup-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        const int keyCount = 600;
        var expected = new Dictionary<string, string>(keyCount);
        for (var i = 0; i < keyCount; i++)
        {
            var key = $"k-{i:D4}";
            var value = $"v-{i}";
            await tree.SetAsync(key, Bytes(value));
            expected[key] = value;
        }

        await CommitSplitAsync(treeId);

        // Sanity: the split must actually have migrated slots, otherwise the
        // donor-orphan path is never exercised and the test is vacuous.
        var moved = await FindMovedKeysAsync(treeId, expected.Keys);
        Assert.That(moved, Is.Not.Empty,
            "The split must migrate at least one seeded key for this regression to be meaningful.");

        var cursorId = await tree.OpenSnapshotEntryCursorAsync();
        var collected = new List<KeyValuePair<string, byte[]>>();
        while (true)
        {
            var page = await tree.NextEntriesAsync(cursorId, 50);
            collected.AddRange(page.Entries);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        var keys = collected.Select(kv => kv.Key).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.Unique,
                "Snapshot entry cursor must never surface a donor-orphan duplicate after a split.");
            Assert.That(keys, Is.EquivalentTo(expected.Keys),
                "Snapshot entry cursor must surface every seeded key exactly once after a split.");
            foreach (var kv in collected)
            {
                Assert.That(Encoding.UTF8.GetString(kv.Value), Is.EqualTo(expected[kv.Key]),
                    $"Snapshot returned the wrong value for '{kv.Key}' after a split.");
            }
        });
    }

    [Test]
    public async Task Snapshot_entry_cursor_reflects_post_split_lww_update_and_delete_for_moved_keys()
    {
        var treeId = $"snap-split-lww-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        const int keyCount = 600;
        var expected = new Dictionary<string, string>(keyCount);
        for (var i = 0; i < keyCount; i++)
        {
            var key = $"k-{i:D4}";
            var value = $"v-{i}";
            await tree.SetAsync(key, Bytes(value));
            expected[key] = value;
        }

        await CommitSplitAsync(treeId);

        var moved = await FindMovedKeysAsync(treeId, expected.Keys);
        Assert.That(moved, Has.Count.GreaterThanOrEqualTo(2),
            "Need at least two migrated keys to exercise both the post-split update and delete.");

        // updateKey: a migrated key re-written on the target shard AFTER the
        // split. The donor still holds the stale pre-split copy as an orphan;
        // the snapshot must surface the new value, not the orphan's.
        // deleteKey: a migrated key deleted on the target AFTER the split. The
        // donor still holds a live orphan copy; the snapshot must not surface
        // it as live.
        var updateKey = moved[0];
        var deleteKey = moved[1];

        await tree.SetAsync(updateKey, Bytes("UPDATED-ON-TARGET"));
        await tree.DeleteAsync(deleteKey);

        // The snapshot is opened after the post-split mutations, so its
        // point-in-time view must include the update and the delete.
        expected[updateKey] = "UPDATED-ON-TARGET";
        expected.Remove(deleteKey);

        var cursorId = await tree.OpenSnapshotEntryCursorAsync();
        var collected = new List<KeyValuePair<string, byte[]>>();
        while (true)
        {
            var page = await tree.NextEntriesAsync(cursorId, 50);
            collected.AddRange(page.Entries);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        var keys = collected.Select(kv => kv.Key).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(keys, Is.Unique,
                "Snapshot entry cursor must never surface a donor-orphan duplicate after a split.");
            Assert.That(keys, Does.Not.Contain(deleteKey),
                "A key deleted on the target after the split must not be resurrected by the donor's orphan.");
            Assert.That(keys, Does.Contain(updateKey));
            Assert.That(keys, Is.EquivalentTo(expected.Keys),
                "Snapshot must surface exactly the live key set at open time.");

            var updatedEntry = collected.Single(kv => kv.Key == updateKey);
            Assert.That(Encoding.UTF8.GetString(updatedEntry.Value), Is.EqualTo("UPDATED-ON-TARGET"),
                "A key updated on the target after the split must show the new value, not the donor's stale orphan.");

            foreach (var kv in collected)
            {
                Assert.That(Encoding.UTF8.GetString(kv.Value), Is.EqualTo(expected[kv.Key]),
                    $"Snapshot returned the wrong value for '{kv.Key}'.");
            }
        });
    }
}
