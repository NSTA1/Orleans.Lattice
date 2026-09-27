using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the id sequence surviving a crash once the row
/// holding the highest issued id has been deleted. Each cold start below is
/// built on the same backing store without calling
/// <see cref="LatticeQueueCore.FlushHeadCursorAsync"/>, which is exactly what
/// a silo crash leaves behind: the coalesced cursor flush never ran.
/// </summary>
public partial class LatticeQueueCoreTests
{
    [Test]
    public async Task Cold_start_after_a_crash_that_beat_the_cursor_flush_does_not_reissue_drained_ids()
    {
        var backing = FakeSystemLattice.Create();
        var (first, _) = await CreateAsync(persistHeadCursor: true, backing: backing);
        for (var i = 0; i < 3; i++)
        {
            await EnqueueAsync(first, $"v{i}");
        }
        while (await first.TryDequeueAsync(CancellationToken.None) is not null)
        {
        }

        // Three dequeues sit far below HeadCursorFlushInterval, so before the
        // fix no cursor row existed here and the cold start re-issued id 1.
        var (second, _) = await CreateAsync(persistHeadCursor: true, backing: backing);
        var next = await EnqueueAsync(second, "after-crash");

        Assert.That(next, Is.EqualTo(4L));
    }

    [Test]
    public async Task Removing_the_tail_entry_does_not_reissue_its_id_after_a_crash()
    {
        var backing = FakeSystemLattice.Create();
        var (first, _) = await CreateAsync(persistHeadCursor: true, backing: backing);
        await EnqueueAsync(first, "a");
        await EnqueueAsync(first, "b");
        await EnqueueAsync(first, "c");

        Assert.That(await first.RemoveAsync(3, CancellationToken.None), Is.True);

        // Rows 1 and 2 survive, so max(stored id) + 1 alone would hand out 3
        // again: only the cursor's next id can say 3 was already issued.
        var (second, _) = await CreateAsync(persistHeadCursor: true, backing: backing);
        Assert.That(second.Snapshot().Select(e => e.Id), Is.EqualTo(new[] { 1L, 2L }));
        Assert.That(await EnqueueAsync(second, "d"), Is.EqualTo(4L));
    }

    [Test]
    public async Task Evicting_the_only_entry_at_capacity_does_not_reissue_its_id_when_the_append_is_lost()
    {
        var (store, data) = FakeSystemLattice.Create();
        var (first, _) = await CreateAsync(persistHeadCursor: true, backing: (store, data));
        await EnqueueAsync(first, "a", capacity: 1);

        // The eviction deletes row 1 and then the append of row 2 is lost, as a
        // crash between the two would leave it.
        var loseAppend = true;
        var row2 = LatticeQueueCore.FormatEntryKey("e/", 2);
        store.SetAsync(row2, Arg.Any<byte[]>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                if (loseAppend)
                {
                    throw new TimeoutException("silo went away");
                }
                data[row2] = ci.Arg<byte[]>();
                return Task.CompletedTask;
            });
        Assert.That(async () => await EnqueueAsync(first, "b", capacity: 1), Throws.TypeOf<TimeoutException>());
        loseAppend = false;

        var (second, _) = await CreateAsync(persistHeadCursor: true, backing: (store, data));
        Assert.That(second.Count, Is.Zero);

        // Id 1 was issued and evicted; handing it out again would be a reissue.
        Assert.That(await EnqueueAsync(second, "c", capacity: 1), Is.EqualTo(2L));
    }

    [Test]
    public async Task The_next_id_is_made_durable_before_the_tail_row_is_deleted_so_a_failed_delete_loses_nothing()
    {
        var (store, data) = FakeSystemLattice.Create();
        var (first, _) = await CreateAsync(persistHeadCursor: true, backing: (store, data));
        await EnqueueAsync(first, "only");

        store.DeleteAsync(LatticeQueueCore.FormatEntryKey("e/", 1), Arg.Any<CancellationToken>())
            .Returns<Task<bool>>(_ => throw new TimeoutException("silo went away"));
        Assert.That(async () => await first.TryDequeueAsync(CancellationToken.None), Throws.TypeOf<TimeoutException>());

        Assert.That(LatticeQueueCore.TryDecodeHeadCursor(data[LatticeQueueCore.HeadCursorKey], out var floor, out var next), Is.True);
        Assert.Multiple(() =>
        {
            // Written first, and with the still-live head as its floor, so the
            // undeleted row stays inside the cold-start scan.
            Assert.That(floor, Is.EqualTo(1L));
            Assert.That(next, Is.EqualTo(2L));
        });

        var (second, _) = await CreateAsync(persistHeadCursor: true, backing: (store, data));
        Assert.Multiple(() =>
        {
            Assert.That(second.Snapshot().Select(e => e.Id), Is.EqualTo(new[] { 1L }), "the entry whose dequeue failed must still be served");
            Assert.That(second.Peek()!.Value.Value, Is.EqualTo(Payload("only")));
        });
    }

    [Test]
    public async Task Draining_a_backlog_writes_the_cursor_once_at_the_tail_not_per_dequeue()
    {
        var (store, data) = FakeSystemLattice.Create();
        var (core, _) = await CreateAsync(persistHeadCursor: true, backing: (store, data));
        for (var i = 0; i < 5; i++)
        {
            await EnqueueAsync(core, $"v{i}");
        }

        for (var i = 0; i < 4; i++)
        {
            await core.TryDequeueAsync(CancellationToken.None);
        }

        // A queue still holding a later row pays nothing extra.
        await store.DidNotReceive().SetAsync(LatticeQueueCore.HeadCursorKey, Arg.Any<byte[]>(), Arg.Any<CancellationToken>());

        await core.TryDequeueAsync(CancellationToken.None);

        await store.Received(1).SetAsync(LatticeQueueCore.HeadCursorKey, Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task No_cursor_row_is_written_when_the_head_cursor_is_disabled_even_when_the_queue_drains()
    {
        var (core, data) = await CreateAsync(persistHeadCursor: false);
        await EnqueueAsync(core, "a");
        await core.TryDequeueAsync(CancellationToken.None);

        Assert.That(data, Is.Empty);
    }

    [Test]
    public void EncodeHeadCursor_round_trips_the_floor_and_the_next_id()
    {
        var encoded = LatticeQueueCore.EncodeHeadCursor(7, 42);

        Assert.That(encoded, Has.Length.EqualTo(LatticeQueueCore.HeadCursorLength));
        Assert.That(LatticeQueueCore.TryDecodeHeadCursor(encoded, out var floor, out var next), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(floor, Is.EqualTo(7L));
            Assert.That(next, Is.EqualTo(42L));
        });
    }

    [Test]
    public void TryDecodeHeadCursor_reads_a_legacy_floor_only_row_as_its_own_next_id()
    {
        Assert.That(LatticeQueueCore.TryDecodeHeadCursor(BitConverter.GetBytes(9L), out var floor, out var next), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(floor, Is.EqualTo(9L));
            Assert.That(next, Is.EqualTo(9L));
        });
    }

    [TestCase(null)]
    [TestCase(0)]
    [TestCase(4)]
    [TestCase(24)]
    public void TryDecodeHeadCursor_rejects_a_row_of_any_other_length(int? length)
    {
        var cursor = length is { } n ? new byte[n] : null;

        Assert.That(LatticeQueueCore.TryDecodeHeadCursor(cursor, out _, out _), Is.False);
    }

    [Test]
    public async Task Cold_start_from_a_legacy_cursor_still_seeds_the_id_sequence()
    {
        var backing = FakeSystemLattice.Create();
        backing.data[LatticeQueueCore.HeadCursorKey] = BitConverter.GetBytes(11L);

        var (core, _) = await CreateAsync(persistHeadCursor: true, backing: backing);

        Assert.That(await EnqueueAsync(core, "a"), Is.EqualTo(11L));
    }
}
