using System.Text;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class LatticeQueueCoreTests
{
    private static byte[] Payload(string s) => Encoding.UTF8.GetBytes(s);

    private static async Task<(LatticeQueueCore core, SortedDictionary<string, byte[]> data)> CreateAsync(
        bool persistHeadCursor = true,
        Action<long>? onEvicted = null,
        (Orleans.Lattice.BPlusTree.Grains.ISystemLattice store, SortedDictionary<string, byte[]> data)? backing = null)
    {
        var (store, data) = backing ?? FakeSystemLattice.Create();
        var core = new LatticeQueueCore(store, "e/", persistHeadCursor, onEvicted);
        await core.InitializeAsync(CancellationToken.None);
        return (core, data);
    }

    private static Task<long> EnqueueAsync(LatticeQueueCore core, string value, int? capacity = null) =>
        core.EnqueueAsync(_ => Payload(value), capacity, CancellationToken.None);

    [Test]
    public void Constructor_throws_on_empty_prefix()
    {
        var (store, _) = FakeSystemLattice.Create();
        Assert.That(() => new LatticeQueueCore(store, "", persistHeadCursor: true), Throws.ArgumentException);
    }

    [Test]
    public async Task InitializeAsync_on_empty_store_yields_empty_queue()
    {
        var (core, _) = await CreateAsync();
        Assert.That(core.Count, Is.EqualTo(0));
    }

    [Test]
    public async Task EnqueueAsync_assigns_increasing_ids_starting_at_one()
    {
        var (core, _) = await CreateAsync();

        var id1 = await EnqueueAsync(core, "a");
        var id2 = await EnqueueAsync(core, "b");

        Assert.That(new[] { id1, id2 }, Is.EqualTo(new[] { 1L, 2L }));
    }

    [Test]
    public async Task EnqueueAsync_writes_entry_row_to_the_store()
    {
        var (core, data) = await CreateAsync();

        await EnqueueAsync(core, "a");

        Assert.That(data.Keys, Has.Some.Matches<string>(k => k.StartsWith("e/", StringComparison.Ordinal)));
    }

    [Test]
    public async Task TryDequeueAsync_returns_and_removes_head_in_fifo_order()
    {
        var (core, _) = await CreateAsync();
        await EnqueueAsync(core, "a");
        await EnqueueAsync(core, "b");

        var head = await core.TryDequeueAsync(CancellationToken.None);

        Assert.That(head, Is.Not.Null);
        Assert.That(head!.Value.Id, Is.EqualTo(1L));
        Assert.That(Encoding.UTF8.GetString(head.Value.Value), Is.EqualTo("a"));
        Assert.That(core.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task TryDequeueAsync_returns_null_when_empty()
    {
        var (core, _) = await CreateAsync();
        Assert.That(await core.TryDequeueAsync(CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task Peek_returns_head_without_removing()
    {
        var (core, _) = await CreateAsync();
        await EnqueueAsync(core, "a");

        var head = core.Peek();

        Assert.That(head, Is.Not.Null);
        Assert.That(head!.Value.Id, Is.EqualTo(1L));
        Assert.That(core.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task Peek_returns_null_when_empty()
    {
        var (core, _) = await CreateAsync();
        Assert.That(core.Peek(), Is.Null);
    }

    [Test]
    public async Task Snapshot_returns_entries_in_ascending_id_order()
    {
        var (core, _) = await CreateAsync();
        await EnqueueAsync(core, "a");
        await EnqueueAsync(core, "b");
        await EnqueueAsync(core, "c");

        var snapshot = core.Snapshot();

        Assert.That(snapshot.Select(e => e.Id), Is.EqualTo(new[] { 1L, 2L, 3L }));
    }

    [Test]
    public async Task RemoveAsync_removes_arbitrary_id()
    {
        var (core, _) = await CreateAsync();
        await EnqueueAsync(core, "a");
        var id2 = await EnqueueAsync(core, "b");
        await EnqueueAsync(core, "c");

        var removed = await core.RemoveAsync(id2, CancellationToken.None);

        Assert.That(removed, Is.True);
        Assert.That(core.Snapshot().Select(e => e.Id), Is.EqualTo(new[] { 1L, 3L }));
    }

    [Test]
    public async Task RemoveAsync_returns_false_for_unknown_id()
    {
        var (core, _) = await CreateAsync();
        Assert.That(await core.RemoveAsync(999, CancellationToken.None), Is.False);
    }

    [Test]
    public async Task TryGet_returns_payload_when_present_and_null_when_absent()
    {
        var (core, _) = await CreateAsync();
        var id = await EnqueueAsync(core, "hello");

        Assert.That(core.TryGet(id), Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(core.TryGet(id)!), Is.EqualTo("hello"));
        Assert.That(core.TryGet(999), Is.Null);
    }

    [Test]
    public async Task EnqueueAsync_evicts_oldest_and_invokes_callback_when_capacity_reached()
    {
        var evicted = new List<long>();
        var (core, _) = await CreateAsync(onEvicted: evicted.Add);

        var id1 = await EnqueueAsync(core, "a", capacity: 2);
        var id2 = await EnqueueAsync(core, "b", capacity: 2);
        var id3 = await EnqueueAsync(core, "c", capacity: 2);

        Assert.Multiple(() =>
        {
            Assert.That(core.Snapshot().Select(e => e.Id), Is.EqualTo(new[] { id2, id3 }));
            Assert.That(evicted, Is.EqualTo(new[] { id1 }));
        });
    }

    [Test]
    public async Task InitializeAsync_rehydrates_cache_and_continues_id_sequence()
    {
        var backing = FakeSystemLattice.Create();
        var (first, _) = await CreateAsync(backing: backing);
        await EnqueueAsync(first, "a");
        await EnqueueAsync(first, "b");

        var (second, _) = await CreateAsync(backing: backing);

        Assert.That(second.Snapshot().Select(e => e.Id), Is.EqualTo(new[] { 1L, 2L }));
        var next = await EnqueueAsync(second, "c");
        Assert.That(next, Is.EqualTo(3L));
    }

    [Test]
    public async Task FlushHeadCursorAsync_persists_head_cursor_row_when_enabled()
    {
        var (core, data) = await CreateAsync(persistHeadCursor: true);
        await EnqueueAsync(core, "a");
        await EnqueueAsync(core, "b");
        await core.TryDequeueAsync(CancellationToken.None);

        await core.FlushHeadCursorAsync(CancellationToken.None);

        Assert.That(data.ContainsKey(LatticeQueueCore.HeadCursorKey), Is.True);
    }

    [Test]
    public async Task Head_cursor_is_not_written_on_every_dequeue()
    {
        var (core, data) = await CreateAsync(persistHeadCursor: true);
        for (var i = 0; i < LatticeQueueCore.HeadCursorFlushInterval; i++)
        {
            await EnqueueAsync(core, "v");
        }

        // A handful of dequeues below the flush interval must not touch the
        // cursor row - it stays off the per-dequeue hot path.
        await core.TryDequeueAsync(CancellationToken.None);
        await core.TryDequeueAsync(CancellationToken.None);

        Assert.That(data.ContainsKey(LatticeQueueCore.HeadCursorKey), Is.False);
    }

    [Test]
    public async Task Head_cursor_is_flushed_automatically_once_the_interval_is_reached()
    {
        var (core, data) = await CreateAsync(persistHeadCursor: true);
        for (var i = 0; i <= LatticeQueueCore.HeadCursorFlushInterval; i++)
        {
            await EnqueueAsync(core, "v");
        }

        for (var i = 0; i < LatticeQueueCore.HeadCursorFlushInterval; i++)
        {
            await core.TryDequeueAsync(CancellationToken.None);
        }

        Assert.That(data.ContainsKey(LatticeQueueCore.HeadCursorKey), Is.True);
    }

    [Test]
    public async Task FlushHeadCursorAsync_is_noop_when_no_writes_are_pending()
    {
        var (core, data) = await CreateAsync(persistHeadCursor: true);
        await EnqueueAsync(core, "a");

        await core.FlushHeadCursorAsync(CancellationToken.None);

        Assert.That(data.ContainsKey(LatticeQueueCore.HeadCursorKey), Is.False);
    }

    [Test]
    public async Task EnqueueAsync_writes_no_cursor_row_when_disabled()
    {
        var (core, data) = await CreateAsync(persistHeadCursor: false);
        await EnqueueAsync(core, "a");

        Assert.Multiple(() =>
        {
            Assert.That(data.ContainsKey(LatticeQueueCore.HeadCursorKey), Is.False);
            Assert.That(data.Keys, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task Cold_start_with_head_cursor_skips_dequeued_ids()
    {
        var backing = FakeSystemLattice.Create();
        var (first, _) = await CreateAsync(persistHeadCursor: true, backing: backing);
        await EnqueueAsync(first, "a");
        await EnqueueAsync(first, "b");
        await first.TryDequeueAsync(CancellationToken.None); // removes id 1
        await first.FlushHeadCursorAsync(CancellationToken.None);

        var (second, _) = await CreateAsync(persistHeadCursor: true, backing: backing);

        Assert.That(second.Snapshot().Select(e => e.Id), Is.EqualTo(new[] { 2L }));
    }

    [Test]
    public async Task Cold_start_with_stale_cursor_does_not_skip_or_double_serve()
    {
        var backing = FakeSystemLattice.Create();
        var (first, data) = await CreateAsync(persistHeadCursor: true, backing: backing);
        for (var i = 0; i < 5; i++)
        {
            await EnqueueAsync(first, $"v{i}");
        }
        await first.TryDequeueAsync(CancellationToken.None); // removes id 1
        await first.TryDequeueAsync(CancellationToken.None); // removes id 2

        // Simulate a cursor that lagged the true head (3) - it was last
        // flushed while the head was still 1. A stale, lower cursor must only
        // cost a re-walk of the already-deleted rows, never skip live ids.
        data[LatticeQueueCore.HeadCursorKey] = BitConverter.GetBytes(1L);

        var (second, _) = await CreateAsync(persistHeadCursor: true, backing: backing);

        Assert.Multiple(() =>
        {
            Assert.That(second.Snapshot().Select(e => e.Id), Is.EqualTo(new[] { 3L, 4L, 5L }));
            Assert.That(second.Count, Is.EqualTo(3));
        });
        var next = await EnqueueAsync(second, "v5");
        Assert.That(next, Is.EqualTo(6L));
    }

    [Test]
    public async Task Cold_start_after_drain_to_empty_does_not_regress_id_or_skip_entries()
    {
        var backing = FakeSystemLattice.Create();
        var (first, _) = await CreateAsync(persistHeadCursor: true, backing: backing);

        // Enqueue and fully drain enough entries to trigger the auto-flush of
        // the head cursor while the cache is empty (cursor records the next
        // id to assign, which sits above every key the entry scan will find).
        var drained = LatticeQueueCore.HeadCursorFlushInterval;
        for (var i = 0; i < drained; i++)
        {
            await EnqueueAsync(first, $"v{i}");
        }
        for (var i = 0; i < drained; i++)
        {
            await first.TryDequeueAsync(CancellationToken.None);
        }

        // Cold start with an empty store but a forward cursor: the id sequence
        // must not regress below the persisted floor, or a new entry would be
        // written below the scan start and be silently skipped next time.
        var (second, _) = await CreateAsync(persistHeadCursor: true, backing: backing);
        var reused = await EnqueueAsync(second, "after-drain");
        Assert.That(reused, Is.GreaterThan((long)drained));

        // The new entry must survive a further cold start.
        var (third, _) = await CreateAsync(persistHeadCursor: true, backing: backing);
        Assert.Multiple(() =>
        {
            Assert.That(third.Count, Is.EqualTo(1));
            Assert.That(third.Peek()!.Value.Id, Is.EqualTo(reused));
        });
    }

    [Test]
    public async Task Draining_a_large_unbounded_queue_preserves_fifo_order()
    {
        var (core, _) = await CreateAsync(persistHeadCursor: true);
        const int n = 200;
        for (var i = 0; i < n; i++)
        {
            await EnqueueAsync(core, i.ToString());
        }

        var dequeued = new List<long>(n);
        while (await core.TryDequeueAsync(CancellationToken.None) is { } head)
        {
            dequeued.Add(head.Id);
        }

        Assert.Multiple(() =>
        {
            Assert.That(dequeued, Is.EqualTo(Enumerable.Range(1, n).Select(i => (long)i)));
            Assert.That(core.Count, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task Head_cursor_row_is_excluded_from_count_and_snapshot_on_cold_start()
    {
        var backing = FakeSystemLattice.Create();
        var (first, data) = await CreateAsync(persistHeadCursor: true, backing: backing);
        await EnqueueAsync(first, "a");
        await EnqueueAsync(first, "b");
        await first.TryDequeueAsync(CancellationToken.None);
        await first.FlushHeadCursorAsync(CancellationToken.None);

        var (second, _) = await CreateAsync(persistHeadCursor: true, backing: backing);

        Assert.Multiple(() =>
        {
            Assert.That(data.ContainsKey(LatticeQueueCore.HeadCursorKey), Is.True);
            Assert.That(second.Count, Is.EqualTo(1));
            Assert.That(second.Snapshot().Select(e => e.Id), Is.EqualTo(new[] { 2L }));
        });
    }

    [Test]
    public void Constructor_throws_when_prefix_collides_with_head_cursor_key()
    {
        var (store, _) = FakeSystemLattice.Create();

        // Prefix "_" yields the scan range ["_", "`") which contains "__head".
        Assert.That(
            () => new LatticeQueueCore(store, "_", persistHeadCursor: true),
            Throws.ArgumentException);
    }

    [Test]
    public void Constructor_allows_cursor_colliding_prefix_when_cursor_disabled()
    {
        var (store, _) = FakeSystemLattice.Create();

        Assert.That(
            () => new LatticeQueueCore(store, "_", persistHeadCursor: false),
            Throws.Nothing);
    }
    [Test]
    public void FormatEntryKey_pads_to_nineteen_digits()
    {
        Assert.That(LatticeQueueCore.FormatEntryKey("e/", 7), Is.EqualTo("e/0000000000000000007"));
    }

    [Test]
    public void PrefixEnd_increments_the_final_character()
    {
        Assert.That(LatticeQueueCore.PrefixEnd("e/"), Is.EqualTo("e0"));
    }
}
