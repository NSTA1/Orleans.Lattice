using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The filtered slice overload of <see cref="LeafReplayCoordinatorGrain"/>
/// (issue #3565): the leaf's ownership rides down to the commit-log reader, the
/// budget becomes the reader's examined bound, and the slice cache keys on the
/// filter so one leaf's filtered window is never served to another.
/// </summary>
public partial class LeafReplayCoordinatorGrainTests
{
    private static readonly WalKeyFilter OwnsM = new("m", "n");

    private static void StubFilteredRead(
        ICommitLogReader reader,
        List<long> pulled,
        params long[] offsets) =>
        reader.ReadFilteredAsync(
                Arg.Any<string>(),
                Arg.Any<int>(),
                Arg.Any<long>(),
                Arg.Any<long>(),
                Arg.Any<int>(),
                Arg.Any<WalKeyFilter>(),
                Arg.Any<CancellationToken>())
            .Returns(_ => Feed(pulled, offsets));

    [Test]
    public async Task Filtered_slice_hands_the_window_the_budget_and_the_filter_to_the_reader()
    {
        var (grain, reader) = CreateGrain();
        var pulled = new List<long>();
        StubFilteredRead(reader, pulled, 5, 9);

        var slice = await grain.ReadSliceAsync(4, 20, budget: 8, OwnsM);

        Assert.That(slice.Select(e => e.Offset), Is.EqualTo(new[] { 5L, 9L }));
        reader.Received(1).ReadFilteredAsync(TreeId, 3, 4, 20, 8, OwnsM, Arg.Any<CancellationToken>());
        reader.DidNotReceive().ReadAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Filtered_slice_with_an_unbounded_filter_takes_the_unfiltered_read()
    {
        var (grain, reader) = CreateGrain();
        var pulled = new List<long>();
        StubRead(reader, pulled, 0, 1);

        var slice = await grain.ReadSliceAsync(-1, 10, budget: 8, filter: default);

        Assert.That(slice.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
        reader.DidNotReceive().ReadFilteredAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(),
            Arg.Any<WalKeyFilter>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Filtered_slices_are_cached_per_filter()
    {
        var (grain, reader) = CreateGrain();
        var pulled = new List<long>();
        StubFilteredRead(reader, pulled, 5);

        await grain.ReadSliceAsync(4, 20, budget: 8, OwnsM);
        await grain.ReadSliceAsync(4, 20, budget: 8, new WalKeyFilter("m", "n"));
        await grain.ReadSliceAsync(4, 20, budget: 8, new WalKeyFilter("a", "b"));

        // The second read carries an equal filter, so it is a cache hit; the
        // third owns different keys, and serving it the first leaf's window
        // would hand it a slice with its own records missing.
        reader.Received(1).ReadFilteredAsync(TreeId, 3, 4, 20, 8, OwnsM, Arg.Any<CancellationToken>());
        reader.Received(1).ReadFilteredAsync(
            TreeId, 3, 4, 20, 8, new WalKeyFilter("a", "b"), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Filtered_and_unfiltered_reads_of_one_window_do_not_share_a_cache_entry()
    {
        var (grain, reader) = CreateGrain();
        var pulled = new List<long>();
        StubRead(reader, pulled, 5, 6);
        StubFilteredRead(reader, pulled, 6);

        var unfiltered = await grain.ReadSliceAsync(4, 20, budget: 8);
        var filtered = await grain.ReadSliceAsync(4, 20, budget: 8, OwnsM);

        Assert.Multiple(() =>
        {
            Assert.That(unfiltered.Select(e => e.Offset), Is.EqualTo(new[] { 5L, 6L }));
            Assert.That(filtered.Select(e => e.Offset), Is.EqualTo(new[] { 6L }));
        });
    }

    [Test]
    public async Task Filtered_slice_stops_at_the_inclusive_ceiling_even_when_the_reader_runs_past_it()
    {
        var (grain, reader) = CreateGrain();
        var pulled = new List<long>();
        StubFilteredRead(reader, pulled, 5, 6, 30);

        var slice = await grain.ReadSliceAsync(4, 20, budget: 8, OwnsM);

        Assert.That(slice.Select(e => e.Offset), Is.EqualTo(new[] { 5L, 6L }));
    }

    [Test]
    public async Task Filtered_slice_of_a_window_the_reader_finds_empty_is_empty()
    {
        var (grain, reader) = CreateGrain();
        StubFilteredRead(reader, new List<long>());

        var slice = await grain.ReadSliceAsync(4, 20, budget: 8, OwnsM);

        Assert.That(slice, Is.Empty, "An empty slice must still mean the window holds nothing.");
    }

    [Test]
    public async Task Filtered_slice_over_the_default_reader_drops_excluded_records_and_keeps_the_trailing_one()
    {
        // FakeCommitLogReader does not override ReadFilteredAsync, so this
        // drives the interface default - the path a reader that never learned
        // the push-down takes - end to end through the coordinator.
        var fake = new FakeCommitLogReader();
        foreach (var key in new[] { "a0", "m1", "b2", "m3", "z4", "c5" })
        {
            fake.Append(TreeId, 3, Mutation(key));
        }

        var (grain, _) = CreateGrain(reader: fake);

        var slice = await grain.ReadSliceAsync(-1, 5, budget: 5, OwnsM);

        Assert.Multiple(() =>
        {
            Assert.That(slice.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 3L, 4L }),
                "Five entries examined: the two owned ones, then offset 4 as the routing-only last examined entry.");
            Assert.That(slice[^1].Mutation.Key, Is.EqualTo("z4"));
        });
    }
}
