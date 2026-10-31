using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5460: every leaf a bulk load or bulk append creates is born declaring the
/// range it owns, and the rightmost leaf an append grafts behind is narrowed to the
/// first appended separator. The WAL materialiser replays into a leaf every record
/// its declared range covers, so a leaf declaring no range re-materialised every
/// later key of its shard when it next activated.
/// </summary>
public sealed partial class ShardRootGrainBulkLoadGraftTests
{
    private static List<(string? Low, string? High)> BirthRanges(Harness h) =>
        h.Leaves.Values
            .SelectMany(l => l.ReceivedCalls()
                .Where(c => c.GetMethodInfo().Name == nameof(IBPlusLeafGrain.InitializeSiblingAsync))
                .Select(c => (SiblingInitialization)c.GetArguments()[0]!))
            .Select(i => (i.LowKeyInclusive, i.HighKeyExclusive))
            .OrderBy(r => r.LowKeyInclusive ?? string.Empty, StringComparer.Ordinal)
            .ToList();

    [Test]
    public async Task BulkLoadRaw_births_each_leaf_declaring_the_range_it_owns()
    {
        var h = CreateHarness(maxLeafKeys: 2);

        await h.Grain.BulkLoadRawAsync("op-1", RawEntries(5));

        Assert.That(BirthRanges(h), Is.EqualTo(new List<(string?, string?)>
        {
            (null, "k0002"),
            ("k0002", "k0004"),
            ("k0004", null),
        }), "leaf ranges tile the shard: open at the two ends, separator to separator between");
    }

    [Test]
    public async Task BulkAppend_births_each_appended_leaf_declaring_its_range_and_narrows_the_old_rightmost_first()
    {
        var h = CreateHarness(maxLeafKeys: 2);
        var existing = h.AddLeaf();
        h.State.State.RootNodeId = existing;
        h.State.State.RootIsLeaf = true;
        var tail = h.Leaf(existing);
        tail.GetAppendTailAsync().Returns((2, "a"));

        await h.Grain.BulkAppendAsync("append-ranges", Pairs(5));

        Assert.That(BirthRanges(h), Is.EqualTo(new List<(string?, string?)>
        {
            ("k0000", "k0002"),
            ("k0002", "k0004"),
            ("k0004", null),
        }));
        Received.InOrder(() =>
        {
            tail.NarrowHighKeyAsync("k0000");
            tail.SetNextSiblingAsync(Arg.Any<GrainId?>());
        });
    }

    [Test]
    public async Task A_resumed_graft_narrows_the_rightmost_leaf_it_names()
    {
        var h = CreateHarness();
        var existing = SeedPendingGraft(h, "op-1", rootWasLeaf: false);
        var separator = h.State.State.PendingBulkGraft!.NewLeaves[0].SeparatorKey;

        await h.Grain.BulkAppendAsync("op-1", Pairs(1));

        await h.Leaf(existing).Received(1).NarrowHighKeyAsync(separator);
    }
}
