using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for stateful cursors (F-033) across adaptive shard
/// splits (F-011). Proves that a cursor paused mid-scan still returns the
/// complete, correctly-ordered, de-duplicated key / entry set when a
/// manual split commits between two cursor steps.
/// <para>
/// A resumable-delete-across-split test is tracked separately: test coverage
/// authoring surfaced a pre-existing bug in
/// <c>ShardRootGrain.DeleteRangeAsync</c> (audit <c>FX-011</c>) that causes
/// under-deletion when a shard's start-leaf has no range-matching keys but
/// later leaves do. That bug is unrelated to cursors and is not exercised
/// here.
/// </para>
/// </summary>
[TestFixture]
public class LatticeCursorSplitTests
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

    private async Task<(ILattice tree, string treeId, Dictionary<string, string> expected)>
        SeedAsync(string prefix, int keyCount)
    {
        var treeId = $"{prefix}-{Guid.NewGuid():N}";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeId);
        var expected = new Dictionary<string, string>(keyCount);
        for (int i = 0; i < keyCount; i++)
        {
            var key = $"k-{i:D4}";
            var value = $"v-{i}";
            await tree.SetAsync(key, Bytes(value));
            expected[key] = value;
        }
        return (tree, treeId, expected);
    }

    private async Task CommitSplitAsync(string treeId, int sourceShardIndex = 0)
    {
        var split = _cluster.GrainFactory.GetGrain<ITreeShardSplitGrain>(
            $"{treeId}/{sourceShardIndex}");
        await split.SplitAsync(sourceShardIndex);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsCompleteAsync(), Is.True,
            $"Manual split of shard {sourceShardIndex} must complete.");
    }

    // --- Key cursor across split ---

    [Test]
    public async Task Key_cursor_yields_every_key_exactly_once_when_split_commits_between_steps()
    {
        var (tree, treeId, expected) = await SeedAsync("cur-split-keys", 250);

        var cursorId = await tree.OpenKeyCursorAsync();

        var first = await tree.NextKeysAsync(cursorId, 25);
        Assert.That(first.Keys, Is.Not.Empty);
        Assert.That(first.HasMore, Is.True);

        await CommitSplitAsync(treeId);

        var collected = new List<string>(first.Keys);
        while (true)
        {
            var page = await tree.NextKeysAsync(cursorId, 25);
            collected.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.Multiple(() =>
        {
            Assert.That(collected, Is.Ordered.Using<string>(StringComparer.Ordinal),
                "Cursor output must remain strictly ascending across a split.");
            Assert.That(collected, Is.Unique,
                "Cursor must never yield the same key twice across a split.");
            Assert.That(new HashSet<string>(collected), Is.EquivalentTo(expected.Keys),
                "Cursor must yield every seeded key exactly once across a split.");
        });
    }

    [Test]
    public async Task Reverse_key_cursor_yields_every_key_exactly_once_when_split_commits_between_steps()
    {
        var (tree, treeId, expected) = await SeedAsync("cur-split-rev", 200);

        var cursorId = await tree.OpenKeyCursorAsync(reverse: true);
        var first = await tree.NextKeysAsync(cursorId, 20);
        Assert.That(first.Keys, Is.Not.Empty);
        Assert.That(first.HasMore, Is.True);

        await CommitSplitAsync(treeId);

        var collected = new List<string>(first.Keys);
        while (true)
        {
            var page = await tree.NextKeysAsync(cursorId, 20);
            collected.AddRange(page.Keys);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.Multiple(() =>
        {
            Assert.That(collected, Is.Ordered.Descending.Using<string>(StringComparer.Ordinal),
                "Reverse cursor must remain strictly descending across a split.");
            Assert.That(collected, Is.Unique);
            Assert.That(new HashSet<string>(collected), Is.EquivalentTo(expected.Keys));
        });
    }

    // --- Entry cursor across split ---

    [Test]
    public async Task Entry_cursor_yields_every_entry_with_correct_value_when_split_commits_between_steps()
    {
        var (tree, treeId, expected) = await SeedAsync("cur-split-entries", 250);

        var cursorId = await tree.OpenEntryCursorAsync();
        var collected = new List<KeyValuePair<string, byte[]>>();

        var first = await tree.NextEntriesAsync(cursorId, 25);
        collected.AddRange(first.Entries);
        Assert.That(first.HasMore, Is.True);

        await CommitSplitAsync(treeId);

        while (true)
        {
            var page = await tree.NextEntriesAsync(cursorId, 25);
            collected.AddRange(page.Entries);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.Multiple(() =>
        {
            Assert.That(collected.Select(kv => kv.Key),
                Is.Ordered.Using<string>(StringComparer.Ordinal));
            Assert.That(collected.Select(kv => kv.Key), Is.Unique);
            Assert.That(collected.Select(kv => kv.Key),
                Is.EquivalentTo(expected.Keys));
            foreach (var kv in collected)
            {
                Assert.That(Encoding.UTF8.GetString(kv.Value),
                    Is.EqualTo(expected[kv.Key]),
                    $"Entry cursor yielded wrong value for '{kv.Key}' across split.");
            }
        });
    }
}
