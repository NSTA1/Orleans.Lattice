using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Detectors for the replay page ring as the coordinator serves it (issue
/// #5896): the incarnation check on every serve (I3, A-c), page immutability
/// (A-d), fill sharing and its aging bound (A-e), and the byte budget (A-f).
/// </summary>
public partial class LeafReplayCoordinatorGrainTests
{
    /// <summary>A WAL partition whose log and incarnation a test controls, counting storage reads.</summary>
    private sealed class ControlledLog
    {
        public List<(long Offset, LatticeMutation Mutation)> Entries { get; } = [];

        public long Incarnation { get; set; }

        public int Reads;

        public TaskCompletionSource? Gate { get; set; }

        public ICommitLogReader Reader()
        {
            var reader = Substitute.For<ICommitLogReader>();
            reader.ReadAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>())
                .Returns(call => Feed(call.ArgAt<long>(2)));
            reader.GetIncarnationAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
                .Returns(_ => ValueTask.FromResult(Incarnation));
            reader.GetHeadOffsetAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
                .Returns(_ => Task.FromResult(Entries.Count == 0 ? -1L : Entries.Max(e => e.Offset)));
            return reader;
        }

        private async IAsyncEnumerable<(long, LatticeMutation)> Feed(long fromExclusive)
        {
            Interlocked.Increment(ref Reads);
            if (Gate is { } gate)
                await gate.Task;
            foreach (var entry in Entries.Where(e => e.Offset > fromExclusive).ToList())
                yield return entry;
            await Task.Yield();
        }

        public void Append(long offset, string key, byte value) =>
            Entries.Add((offset, new LatticeMutation { TreeId = TreeId, Kind = MutationKind.Set, Key = key, Value = [value] }));
    }

    private static (LeafReplayCoordinatorGrain Grain, ControlledLog Log) RingGrain(
        ControlledLog? log = null, long ringBytes = 1L << 20, LeafResidentWorkingSet? workingSet = null)
    {
        log ??= new ControlledLog();
        var (grain, _) = CreateGrain(reader: log.Reader(), ringBytes: ringBytes, workingSet: workingSet);
        return (grain, log);
    }

    [Test]
    public async Task Leaves_replaying_one_partition_share_one_storage_read_per_page()
    {
        var (grain, log) = RingGrain();
        for (var i = 0; i < 10; i++)
            log.Append(i, "k" + i, (byte)i);

        var a = await grain.ReadSliceAsync(-1, 9, 100, new WalKeyFilter("k0", "k5"));
        var b = await grain.ReadSliceAsync(-1, 9, 100, new WalKeyFilter("k5", null));

        Assert.That(log.Reads, Is.EqualTo(1), "the second leaf is served from the page the first one read");
        Assert.That(a.Where(e => e.Mutation.Value is not null).Select(e => e.Offset), Is.EqualTo(new long[] { 0, 1, 2, 3, 4 }));
        Assert.That(b.Select(e => e.Offset), Is.EqualTo(new long[] { 5, 6, 7, 8, 9 }));
    }

    [Test]
    public async Task A_same_id_reset_then_regrow_serves_requests_inside_the_old_coverage_from_storage()
    {
        var (grain, log) = RingGrain();
        for (var i = 0; i < 6; i++)
            log.Append(i, "old" + i, 1);
        Assert.That((await grain.ReadSliceAsync(-1, 5, 100)).Count, Is.EqualTo(6));
        var readsBefore = log.Reads;

        // Purge and recreate under the same id: the log is emptied, the
        // incarnation advances, and the new tree regrows past the old high
        // without the head ever falling below it.
        log.Entries.Clear();
        log.Incarnation++;
        for (var i = 0; i < 8; i++)
            log.Append(i, "new" + i, 2);

        var slice = await grain.ReadSliceAsync(-1, 5, 100);

        Assert.That(log.Reads, Is.GreaterThan(readsBefore), "a request inside the old coverage must read storage");
        Assert.That(slice.Select(e => e.Mutation.Key), Is.All.StartsWith("new"),
            "a page of the purged log must never be served after the reset");
    }

    [Test]
    public async Task A_duplicate_coordinator_activation_never_serves_pre_reset_pages_after_a_reset()
    {
        var log = new ControlledLog();
        for (var i = 0; i < 6; i++)
            log.Append(i, "old" + i, 1);
        var (first, _) = RingGrain(log);
        var (duplicate, _) = RingGrain(log);
        await first.ReadSliceAsync(-1, 5, 100);
        await duplicate.ReadSliceAsync(-1, 5, 100);

        // Only one instance hears about the reset; the other never does.
        log.Entries.Clear();
        log.Incarnation++;
        for (var i = 0; i < 6; i++)
            log.Append(i, "new" + i, 2);
        first.RingForTest!.Reset(log.Incarnation);

        var slice = await duplicate.ReadSliceAsync(-1, 5, 100);

        Assert.That(slice.Select(e => e.Mutation.Key), Is.All.StartsWith("new"));
    }

    [Test]
    public async Task A_reset_that_lands_while_a_page_is_read_never_inserts_that_page()
    {
        var (grain, log) = RingGrain();
        for (var i = 0; i < 4; i++)
            log.Append(i, "old" + i, 1);
        log.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var read = grain.ReadSliceAsync(-1, 3, 100);
        log.Incarnation++;
        log.Gate.SetResult();
        await read;

        Assert.That(grain.RingForTest!.Pages.Where(p => p.Incarnation != log.Incarnation), Is.Empty,
            "a page read across a reset must not be cached at the old incarnation");
    }

    [Test]
    public async Task One_leafs_apply_leaves_another_leafs_slice_byte_identical()
    {
        var (grain, log) = RingGrain();
        for (var i = 0; i < 4; i++)
            log.Append(i, "k" + i, (byte)(10 + i));

        var first = await grain.ReadSliceAsync(-1, 3, 100);
        var snapshot = first.Select(e => (e.Offset, e.Mutation.Key, Value: e.Mutation.Value!.ToArray())).ToList();

        // A leaf that rewrites what it was handed - the slice array and the
        // value buffers - must not change what the next leaf receives.
        var array = (CommitLogSliceEntry[])first;
        array[0] = array[^1];
        foreach (var entry in first)
            entry.Mutation.Value![0] ^= 0xFF;

        var second = await grain.ReadSliceAsync(-1, 3, 100);
        Assert.That(second.Select(e => e.Offset), Is.EqualTo(snapshot.Select(s => s.Offset)));
        Assert.That(second.Select(e => e.Mutation.Key), Is.EqualTo(snapshot.Select(s => s.Key)));
        Assert.That(second.Select(e => e.Mutation.Value), Is.EqualTo(snapshot.Select(s => s.Value)));
    }

    [Test]
    public async Task A_waiter_past_the_aging_bound_issues_its_own_fill()
    {
        var (grain, log) = RingGrain();
        log.Append(0, "k0", 1);
        var ring = new ReplayPageRing(new LeafResidentWorkingSet(1L << 30).Pages);
        ring.Reset(0);

        var stuck = new TaskCompletionSource<ReplayPage?>();
        ring.RegisterFill(-1, stuck.Task, now: 0);
        var aging = (long)(LeafReplayCoordinatorGrain.ReplayPageFillAgingBound.TotalSeconds * System.Diagnostics.Stopwatch.Frequency);

        Assert.That(ring.JoinableFill(-1, aging, aging), Is.SameAs(stuck.Task), "inside the bound a request joins");
        Assert.That(ring.JoinableFill(-1, aging + 1, aging), Is.Null, "past the bound it issues its own fill");
        stuck.SetResult(null);
        await Task.CompletedTask;
        _ = grain;
        _ = log;
    }

    [Test]
    public async Task The_ring_never_exceeds_its_byte_budget_under_random_fills()
    {
        var rng = new Random(5896);
        var log = new ControlledLog();
        for (var i = 0; i < 400; i++)
            log.Append(i, "key" + rng.Next(1000), (byte)i);

        var workingSet = new LeafResidentWorkingSet(1L << 30);
        var grains = Enumerable.Range(0, 3).Select(_ => RingGrain(log, ringBytes: 8_000, workingSet: workingSet).Grain).ToList();
        for (var round = 0; round < 300; round++)
        {
            var grain = grains[rng.Next(grains.Count)];
            var from = rng.Next(-1, 399);
            await grain.ReadSliceAsync(from, from + rng.Next(1, 60), rng.Next(1, 40));

            foreach (var g in grains)
                Assert.That(g.RingForTest?.Bytes ?? 0, Is.LessThanOrEqualTo(8_000));
            Assert.That(workingSet.Pages.Bytes, Is.LessThanOrEqualTo(8_000),
                "all rings of one process share the one budget");
        }
    }
}
