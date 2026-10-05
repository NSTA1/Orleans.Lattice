using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Issue #4621: the in-memory provider's trim watermark. A trim raises it before
/// deleting anything; nothing at or below it is read, reported as the lowest
/// offset, or accepted as a new append; and allocation stays above it.
/// </summary>
[TestFixture]
public class InMemoryWalStorageProviderTrimWatermarkTests
{
    private const string Tree = "tree";

    private static WalEntry Entry(long offset) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = [1],
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };

    private static async Task<List<long>> ReadAllAsync(InMemoryWalStorageProvider sut)
    {
        var offsets = new List<long>();
        await foreach (var entry in sut.ReadAsync(Tree, 0, -1, 64, CancellationToken.None))
        {
            offsets.Add(entry.Offset);
        }

        return offsets;
    }

    [Test]
    public async Task GetTrimWatermarkAsync_reports_minus_one_before_any_trim_and_the_trim_point_after()
    {
        var sut = new InMemoryWalStorageProvider();
        Assert.That(await sut.GetTrimWatermarkAsync(Tree, 0, CancellationToken.None), Is.EqualTo(-1L));

        await sut.AppendBatchAsync(Tree, 0, [Entry(0), Entry(1)], CancellationToken.None);
        await sut.AppendBatchAsync(Tree, 0, [Entry(3)], CancellationToken.None);
        await sut.TrimAsync(Tree, 0, 1, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await sut.GetTrimWatermarkAsync(Tree, 0, CancellationToken.None), Is.EqualTo(1L),
                "the watermark is the trim point, not the offset below the lowest stored entry");
            Assert.That(await sut.GetLowestOffsetAsync(Tree, 0, CancellationToken.None), Is.EqualTo(3L));
        });
    }

    [Test]
    public async Task A_watermark_raised_before_the_delete_hides_the_entries_it_covers()
    {
        // The state a crash between raising the watermark and deleting leaves.
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(Tree, 0, [Entry(0), Entry(1), Entry(2)], CancellationToken.None);
        sut.RaiseTrimWatermarkForTesting(Tree, 0, 1);

        var filtered = new List<long>();
        await foreach (var entry in sut.ReadFilteredAsync(Tree, 0, -1, 10, 64, new WalKeyFilter(null, null), CancellationToken.None))
        {
            filtered.Add(entry.Offset);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await ReadAllAsync(sut), Is.EqualTo(new long[] { 2 }), "a read never returns an entry at or below the watermark");
            Assert.That(filtered, Is.EqualTo(new long[] { 2 }), "nor does a filtered read");
            Assert.That(await sut.GetLowestOffsetAsync(Tree, 0, CancellationToken.None), Is.EqualTo(2L));
            Assert.That(await sut.GetTrimWatermarkAsync(Tree, 0, CancellationToken.None), Is.EqualTo(1L));
        });
    }

    [Test]
    public async Task An_append_at_or_below_the_watermark_is_refused_and_allocation_stays_above_it()
    {
        var sut = new InMemoryWalStorageProvider();
        await sut.AppendBatchAsync(Tree, 0, [Entry(0)], CancellationToken.None);
        await sut.AppendBatchAsync(Tree, 0, [Entry(2)], CancellationToken.None);
        await sut.TrimAsync(Tree, 0, 2, CancellationToken.None);

        // A late landing at the trimmed hole would be invisible to every read.
        Assert.That(
            async () => await sut.AppendBatchAsync(Tree, 0, [Entry(1)], CancellationToken.None),
            Throws.InvalidOperationException);
        Assert.That(await sut.GetHighestOffsetAsync(Tree, 0, CancellationToken.None), Is.EqualTo(2L),
            "a recovering allocator resumes above the watermark");
    }
}
