using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;

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
}
