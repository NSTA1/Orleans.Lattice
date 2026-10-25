using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class ReplicationShipperGrainTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Pipeline_enforces_window_and_Fifo_cursor_across_out_of_order_completion(bool rejectHead)
    {
        var (grain, state, feed, transport, _, _, _) = Create(PipelinedOptions(2, 1));
        feed.Append(MakeEntry("first", ticks: 10));
        feed.Append(MakeEntry("second", ticks: 20));
        feed.Append(MakeEntry("beyond-window", ticks: 30));
        var first = new TaskCompletionSource<ReplicationAck>();
        var second = new TaskCompletionSource<ReplicationAck>();
        var calls = 0;
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(_ => ++calls switch
            {
                1 => first.Task,
                2 => second.Task,
                _ => Task.FromResult(new ReplicationAck { Accepted = true }),
            });
        var pump = grain.PumpForTestingAsync();
        try
        {
            Assert.That(calls, Is.EqualTo(2), "the real pump fills exactly its configured window");
            Assert.That(pump.IsCompleted, Is.False);
            second.SetResult(new ReplicationAck { Accepted = true });
            Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero),
                "a later completed send cannot skip the unresolved head");
            first.SetResult(new ReplicationAck { Accepted = !rejectHead });
            await pump;
            Assert.That(state.State.Cursor.WallClockTicks, Is.EqualTo(rejectHead ? 0 : 30));
            Assert.That(state.State.PartitionCursors.GetValueOrDefault(0), Is.EqualTo(rejectHead ? 0 : 3));
        }
        finally
        {
            first.TrySetResult(new ReplicationAck { Accepted = false });
            second.TrySetResult(new ReplicationAck { Accepted = false });
            await pump;
        }
    }

    [TestCase(1)]
    [TestCase(2)]
    public async Task Retry_without_jitter_preserves_cursor_then_success_resets_backoff(int window)
    {
        var options = PipelinedOptions(window, 1);
        options.ShipBackoffInitial = TimeSpan.FromDays(1);
        options.ShipBackoffMax = TimeSpan.FromDays(1);
        options.ShipBackoffJitter = 0;
        var (grain, state, feed, transport, _, _, _) = Create(options);
        feed.Append(MakeEntry("first", ticks: 20));
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = false });
        await grain.PumpForTestingAsync();
        Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero));
        Assert.That(state.State.ConsecutiveFailures, Is.EqualTo(1));
        await grain.PumpForTestingAsync();
        Assert.That(SendAsyncCallCount(transport), Is.EqualTo(1), "unexpired backoff blocks retry");
        grain.ResetBackoffForTesting();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true });
        await grain.PumpForTestingAsync();
        Assert.That(state.State.Cursor.WallClockTicks, Is.EqualTo(20));
        Assert.That(state.State.ConsecutiveFailures, Is.Zero);
        feed.Append(MakeEntry("lower-clock", ticks: 10));
        await grain.PumpForTestingAsync();
        Assert.That(state.State.Cursor.WallClockTicks, Is.EqualTo(20), "the HLC cursor never rewinds");
        Assert.That(state.State.PartitionCursors[0], Is.EqualTo(2), "partition progress still consumes the lower clock");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Pipeline_stops_launching_after_receiver_feedback(bool pause)
    {
        var (grain, state, feed, transport, _, _, _) = Create(PipelinedOptions(2, 2));
        for (var i = 1; i <= 12; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i));
        }

        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                SuggestedBatchSize = pause ? null : 1,
                PauseForMs = pause ? 60_000 : null,
            });

        await grain.PumpForTestingAsync();

        Assert.Multiple(() =>
        {
            Assert.That(SendAsyncCallCount(transport), Is.EqualTo(2),
                "already-launched batches may settle, but feedback must stop new launches in this pump");
            Assert.That(state.State.PartitionCursors[0], Is.EqualTo(4),
                "only the two acknowledged batches may be consumed");
        });
    }

    [Test]
    public async Task Pipeline_preserves_pause_when_a_later_inflight_ack_is_healthy()
    {
        var (grain, _, feed, transport, _, _, _) = Create(PipelinedOptions(2, 1));
        feed.Append(MakeEntry("first", ticks: 1));
        feed.Append(MakeEntry("second", ticks: 2));
        var sends = 0;
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(_ => new ReplicationAck
            {
                Accepted = true,
                PauseForMs = ++sends == 1 ? 60_000 : null,
            });

        await grain.PumpForTestingAsync();
        feed.Append(MakeEntry("third", ticks: 3));
        await grain.PumpForTestingAsync();

        Assert.That(sends, Is.EqualTo(2),
            "a later success must not erase an unexpired pause from an earlier in-flight acknowledgement");
    }

    [Test]
    public async Task Released_terminal_waits_for_next_pump_after_mixed_window_feedback()
    {
        var (grain, state, feed, transport, _, _, _) = Create(PipelinedOptions(3, 2));
        var tx = Guid.NewGuid();
        feed.Append(PreparedEntry("prepare-one", tx, 0, 2, 1));
        feed.Append(PreparedEntry("prepare-two", tx, 1, 2, 2));
        feed.Append(MakeEntry("point", ticks: 3));
        feed.Append(CommitEntry(tx, 0, 1, 4));
        var sends = 0;
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(_ => new ReplicationAck
            {
                Accepted = true,
                SuggestedBatchSize = ++sends == 1 ? 1 : null,
            });

        await grain.PumpForTestingAsync();
        Assert.That(sends, Is.EqualTo(2),
            "draining mixed acknowledgements must not erase feedback before releasing a held terminal");
        Assert.That(state.State.PartitionCursors[0], Is.EqualTo(3),
            "the held terminal is retained after the accepted prepare and point batches");

        feed.Append(MakeEntry("next-tick", ticks: 5));
        await grain.PumpForTestingAsync();
        Assert.That(sends, Is.EqualTo(3));
        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
            "the null hint permits re-acceleration on the next pump, not during the prior one");
        Assert.That(state.State.PartitionCursors[0], Is.EqualTo(5));
    }
}
