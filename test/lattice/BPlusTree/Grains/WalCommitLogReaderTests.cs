using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="WalCommitLogReader"/>, the paginated
/// <see cref="ICommitLogReader"/> adapter over <see cref="IWalShardGrain.ReadAsync"/>.
/// Pins the boundary contract that an exclusive lower bound at
/// <see cref="long.MaxValue"/> selects nothing: the naive
/// <c>fromOffsetExclusive + 1</c> overflows to <see cref="long.MinValue"/> and,
/// once clamped to zero, would wrongly replay the whole log from the head.
/// </summary>
[TestFixture]
public sealed class WalCommitLogReaderTests
{
    private const string TreeId = "tree-reader";

    private static (WalCommitLogReader reader, IWalShardGrain grain) CreateReader()
    {
        var grain = Substitute.For<IWalShardGrain>();
        grain
            .ReadAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(new ValueTask<WalShardPage>(WalShardPage.Empty(0)));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(grain);

        return (new WalCommitLogReader(factory), grain);
    }

    private static async Task<List<(long Offset, LatticeMutation Mutation)>> ReadAllAsync(
        WalCommitLogReader reader,
        long fromOffsetExclusive)
    {
        var collected = new List<(long, LatticeMutation)>();
        await foreach (var entry in reader.ReadAsync(TreeId, 0, fromOffsetExclusive))
        {
            collected.Add(entry);
        }

        return collected;
    }

    [Test]
    public async Task ReadAsync_yields_nothing_from_an_exclusive_lower_bound_at_long_max_value()
    {
        var (reader, grain) = CreateReader();

        var collected = await ReadAllAsync(reader, fromOffsetExclusive: long.MaxValue);

        Assert.That(collected, Is.Empty);

        // The distinguishing assertion: the overflow guard must short-circuit
        // before the grain is polled. Without it the reader would compute an
        // inclusive cursor of zero and replay the entire log from the head.
        await grain.DidNotReceive().ReadAsync(
            Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReadAsync_polls_the_grain_for_an_in_range_lower_bound()
    {
        var (reader, grain) = CreateReader();

        // A normal cursor drains the grain (which returns an empty page here),
        // proving the guard above is specific to the long.MaxValue boundary and
        // not a blanket short-circuit.
        var collected = await ReadAllAsync(reader, fromOffsetExclusive: 41);

        Assert.That(collected, Is.Empty);
        await grain.Received(1).ReadAsync(42, Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task ReadAsync_resumes_across_short_pages_without_skipping_or_duplicating()
    {
        // Caller-side guard for the read byte bound added in issue #2689.
        // That bound truncates a WAL read page when its payloads exceed the
        // byte budget, so pages shorter than the requested count became
        // routine rather than exceptional. This fixture pins the property
        // that makes such truncation safe: the reader resumes from
        // page.NextSequence, which the shard derives from the last entry
        // actually RETURNED, so a short page is a resumption and never a
        // skip.
        //
        // The pages below are deliberately ragged (3, then 1, then 2) - the
        // shape a byte budget produces when record sizes vary - and the
        // stub asserts each poll arrives at the cursor the previous page
        // reported, so a reader that resumed from what it REQUESTED instead
        // of what it received would fail here.
        var (reader, grain) = CreateReader();
        var pages = new Queue<long[]>(new[]
        {
            new[] { 0L, 1L, 2L },
            new[] { 3L },
            new[] { 4L, 5L },
        });

        var expectedFrom = 0L;
        grain
            .ReadAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var from = call.ArgAt<long>(0);
                Assert.That(
                    from,
                    Is.EqualTo(expectedFrom),
                    "the reader must resume from the last sequence actually returned");

                if (pages.Count == 0)
                {
                    return new ValueTask<WalShardPage>(WalShardPage.Empty(from));
                }

                var sequences = pages.Dequeue();
                expectedFrom = sequences[^1] + 1;
                return new ValueTask<WalShardPage>(new WalShardPage
                {
                    Entries = sequences.Select(Sequenced).ToArray(),
                    NextSequence = expectedFrom,
                });
            });

        var collected = await ReadAllAsync(reader, fromOffsetExclusive: -1L);

        Assert.That(
            collected.Select(e => e.Offset),
            Is.EqualTo(new[] { 0L, 1L, 2L, 3L, 4L, 5L }),
            "a ragged, short-paged drain must be lossless, duplicate-free, and ordered");
    }

    private static WalShardSequencedEntry Sequenced(long sequence) => new()
    {
        Sequence = sequence,
        Entry = WalRecordConverter.ToWalRecord(
            new LatticeMutation
            {
                TreeId = TreeId,
                Kind = MutationKind.Set,
                Key = "k" + sequence.ToString(System.Globalization.CultureInfo.InvariantCulture),
                Value = new byte[] { (byte)sequence },
                Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
                OriginClusterId = "site-a",
            },
            LatticeMergeMode.LwwRegister,
            "site-a"),
    };
}
