using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class ShardRootGrainCountWorkBoundTests
{
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    public async Task Ownership_filtered_counts_resume_without_duplicating_or_skipping_keys(int path)
    {
        var h = CreateChain(leafCount: 40, keysPerLeaf: 2, maxLeavesPerBatch: 4, movedOwnership: true);
        var movedSlot = ShardMap.GetVirtualSlot("k000-000", 16);
        var slots = Enumerable.Range(0, 16).Where(s => s != movedSlot).ToArray();
        var start = path == 1 ? null : "k003-001";
        var end = path == 1 ? null : "k015-001";
        var expected = Enumerable.Range(0, 40)
            .SelectMany(i => Enumerable.Range(0, 2).Select(j => $"k{i:D3}-{j:D3}"))
            .Count(k => (start is null || string.CompareOrdinal(k, start) >= 0)
                && (end is null || string.CompareOrdinal(k, end) < 0)
                && ShardMap.GetVirtualSlot(k, 16) != movedSlot);
        var total = 0;
        var batches = 0;
        var cursor = start;
        var reported = new HashSet<int>();
        do
        {
            if (path == 1)
            {
                var page = await h.Grain.CountWithMovedAwayBoundedAsync(cursor);
                total += page.Count;
                cursor = page.ResumeFromInclusive;
                if (page.MovedAwaySlots is { } moved) reported.UnionWith(moved);
            }
            else
            {
                var page = path == 0
                    ? await h.Grain.CountBoundedAsync(cursor, end)
                    : await h.Grain.CountForSlotsBoundedAsync(slots, 16, cursor, end);
                total += page.Count;
                cursor = page.ResumeFromInclusive;
            }
            Assert.That(++batches, Is.LessThan(20), "the key-based continuation must terminate");
        }
        while (cursor is not null);

        Assert.That(batches, Is.GreaterThan(1), "the test must cross real continuation boundaries");
        Assert.That(total, Is.EqualTo(expected));
        if (path == 1) Assert.That(reported, Is.EquivalentTo(new[] { movedSlot }));
    }
}
