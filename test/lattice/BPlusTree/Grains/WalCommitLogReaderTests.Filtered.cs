using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// <see cref="WalCommitLogReader.ReadFilteredAsync"/>, which pages a filtered
/// window through the WAL grain (issue #3565). Two properties matter: the whole
/// read examines no more than its budget - each page is charged for the offset
/// span it covers - and only the window's final routing-only entry reaches the
/// caller, however many pages ended on one.
/// </summary>
public sealed partial class WalCommitLogReaderTests
{
    private static readonly WalKeyFilter OwnsM = new("m", "n");

    private static WalShardSequencedEntry SequencedKey(long sequence, string key) => new()
    {
        Sequence = sequence,
        Entry = WalRecordConverter.ToWalRecord(
            new LatticeMutation
            {
                TreeId = TreeId,
                Kind = MutationKind.Set,
                Key = key,
                Value = [(byte)sequence],
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            },
            LatticeMergeMode.LwwRegister,
            string.Empty),
    };

    private static async Task<List<(long Offset, LatticeMutation Mutation)>> ReadFilteredAllAsync(
        WalCommitLogReader reader,
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int maxExamined)
    {
        var collected = new List<(long, LatticeMutation)>();
        await foreach (var entry in reader.ReadFilteredAsync(TreeId, 0, fromOffsetExclusive, toOffsetInclusive, maxExamined, OwnsM))
        {
            collected.Add(entry);
        }

        return collected;
    }

    [Test]
    public async Task ReadFilteredAsync_reads_nothing_for_an_empty_window_without_polling_the_grain()
    {
        var (reader, grain) = CreateReader();

        var fromMax = await ReadFilteredAllAsync(reader, long.MaxValue, long.MaxValue, 10);
        var inverted = await ReadFilteredAllAsync(reader, 10, 10, 10);

        Assert.Multiple(() =>
        {
            Assert.That(fromMax, Is.Empty);
            Assert.That(inverted, Is.Empty);
        });
        await grain.DidNotReceive().ReadFilteredAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReadFilteredAsync_asks_each_page_for_no_more_than_the_budget_that_remains()
    {
        var (reader, grain) = CreateReader();
        var requested = new List<(long From, int Max)>();
        grain
            .ReadFilteredAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var from = call.ArgAt<long>(0);
                var max = call.ArgAt<int>(2);
                requested.Add((from, max));

                // A dense page that examined exactly what it was allowed to and
                // kept only its last entry.
                var last = from + max - 1;
                return new ValueTask<WalShardPage>(new WalShardPage
                {
                    Entries = [SequencedKey(last, "m" + last)],
                    NextSequence = last + 1,
                });
            });

        var read = await ReadFilteredAllAsync(reader, -1, 10_000, maxExamined: 600);

        Assert.Multiple(() =>
        {
            Assert.That(requested, Is.EqualTo(new[] { (0L, 256), (256L, 256), (512L, 88) }),
                "Three pages examine 600 entries between them and no more.");
            Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 255L, 511L, 599L }));
        });
        await grain.Received(3).ReadFilteredAsync(
            Arg.Any<long>(), 10_000, Arg.Any<int>(), OwnsM, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReadFilteredAsync_charges_a_page_for_the_whole_offset_span_it_covers()
    {
        // A gap in the log lets one page cover far more offsets than it
        // examined. Charging the span is conservative, which is the safe
        // direction: the read may stop early, but can never examine more than
        // its budget.
        var (reader, grain) = CreateReader();
        grain
            .ReadFilteredAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<WalShardPage>(new WalShardPage
            {
                Entries = [SequencedKey(999, "m999")],
                NextSequence = 1000,
            }));

        var read = await ReadFilteredAllAsync(reader, -1, 10_000, maxExamined: 600);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 999L }));
        await grain.Received(1).ReadFilteredAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReadFilteredAsync_delivers_only_the_final_pages_routing_only_entry()
    {
        var (reader, grain) = CreateReader();
        var pages = new Queue<WalShardPage>(
        [
            new WalShardPage { Entries = [SequencedKey(0, "m0"), SequencedKey(1, "a1")], NextSequence = 2 },
            new WalShardPage { Entries = [SequencedKey(2, "m2"), SequencedKey(3, "z3")], NextSequence = 4 },
            WalShardPage.Empty(4),
        ]);
        grain
            .ReadFilteredAsync(Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask<WalShardPage>(pages.Dequeue()));

        var read = await ReadFilteredAllAsync(reader, -1, 100, maxExamined: 1000);

        Assert.Multiple(() =>
        {
            Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 2L, 3L }),
                "Offset 1 ended the first page but not the window, so it is superseded by the second page.");
            Assert.That(read[^1].Mutation.Key, Is.EqualTo("z3"));
        });
    }

    [Test]
    public void ReadFilteredAsync_validates_its_arguments()
    {
        var (reader, _) = CreateReader();

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await ReadFilteredAllAsync(reader, -1, 10, maxExamined: 0),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(
                async () =>
                {
                    await foreach (var _ in reader.ReadFilteredAsync(string.Empty, 0, -1, 10, 1, OwnsM))
                    {
                    }
                },
                Throws.InstanceOf<ArgumentException>());
            Assert.That(
                async () =>
                {
                    await foreach (var _ in reader.ReadFilteredAsync(TreeId, -1, -1, 10, 1, OwnsM))
                    {
                    }
                },
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }
}
