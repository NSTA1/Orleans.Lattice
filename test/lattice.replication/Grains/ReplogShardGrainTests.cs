using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class ReplogShardGrainTests
{
    private static ReplogShardGrain CreateGrain(FakePersistentState<ReplogShardState>? state = null)
    {
        state ??= new FakePersistentState<ReplogShardState>();
        return new ReplogShardGrain(state);
    }

    private static ReplogEntry MakeEntry(string key, byte[]? value = null) => new()
    {
        TreeId = "tree",
        Op = ReplogOp.Set,
        Key = key,
        Value = value ?? new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-a",
    };

    [Test]
    public async Task AppendAsync_assigns_zero_for_first_entry()
    {
        var grain = CreateGrain();

        var seq = await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        Assert.That(seq, Is.EqualTo(0L));
    }

    [Test]
    public async Task AppendAsync_assigns_monotonically_increasing_sequence_numbers()
    {
        var grain = CreateGrain();

        var s0 = await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        var s1 = await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        var s2 = await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        Assert.That(new[] { s0, s1, s2 }, Is.EqualTo(new[] { 0L, 1L, 2L }));
    }

    [Test]
    public async Task AppendAsync_persists_state_on_every_call()
    {
        var state = new FakePersistentState<ReplogShardState>();
        var grain = CreateGrain(state);

        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(2));
    }

    [Test]
    public async Task AppendAsync_writes_entry_to_state_with_assigned_sequence()
    {
        var state = new FakePersistentState<ReplogShardState>();
        var grain = CreateGrain(state);
        var entry = MakeEntry("k", new byte[] { 9, 9 });

        await grain.AppendAsync(entry, CancellationToken.None);

        Assert.That(state.State.Entries, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Entries[0].Sequence, Is.EqualTo(0L));
            Assert.That(state.State.Entries[0].Entry, Is.EqualTo(entry));
        });
    }

    [Test]
    public void AppendAsync_observes_cancellation_before_persisting()
    {
        var state = new FakePersistentState<ReplogShardState>();
        var grain = CreateGrain(state);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("a"), cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
        Assert.Multiple(() =>
        {
            Assert.That(state.WriteCount, Is.EqualTo(0));
            Assert.That(state.State.Entries, Is.Empty);
        });
    }

    [Test]
    public async Task ReadAsync_returns_empty_page_when_log_is_empty()
    {
        var grain = CreateGrain();

        var page = await grain.ReadAsync(0, 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.NextSequence, Is.EqualTo(0L));
        });
    }

    [Test]
    public async Task ReadAsync_returns_entries_from_the_specified_sequence()
    {
        var grain = CreateGrain();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        var page = await grain.ReadAsync(1, 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.Sequence), Is.EqualTo(new[] { 1L, 2L }));
            Assert.That(page.Entries.Select(e => e.Entry.Key), Is.EqualTo(new[] { "b", "c" }));
            Assert.That(page.NextSequence, Is.EqualTo(3L));
        });
    }

    [Test]
    public async Task ReadAsync_caps_returned_entries_at_max_entries()
    {
        var grain = CreateGrain();
        for (var i = 0; i < 5; i++)
        {
            await grain.AppendAsync(MakeEntry($"k{i}"), CancellationToken.None);
        }

        var page = await grain.ReadAsync(0, 2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(2));
            Assert.That(page.NextSequence, Is.EqualTo(2L));
        });
    }

    [Test]
    public async Task ReadAsync_returns_empty_when_from_sequence_is_at_end_of_log()
    {
        var grain = CreateGrain();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        var page = await grain.ReadAsync(1, 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.NextSequence, Is.EqualTo(1L));
        });
    }

    [Test]
    public async Task ReadAsync_returns_empty_when_from_sequence_beyond_end_of_log()
    {
        var grain = CreateGrain();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);

        var page = await grain.ReadAsync(99, 10, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty);
            Assert.That(page.NextSequence, Is.EqualTo(99L));
        });
    }

    [Test]
    public void ReadAsync_throws_on_negative_from_sequence()
    {
        var grain = CreateGrain();

        Assert.That(
            async () => await grain.ReadAsync(-1, 10, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void ReadAsync_throws_on_non_positive_max_entries(int maxEntries)
    {
        var grain = CreateGrain();

        Assert.That(
            async () => await grain.ReadAsync(0, maxEntries, CancellationToken.None),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void ReadAsync_observes_cancellation()
    {
        var grain = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.ReadAsync(0, 10, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetNextSequenceAsync_returns_zero_for_empty_log()
    {
        var grain = CreateGrain();

        var next = await grain.GetNextSequenceAsync(CancellationToken.None);

        Assert.That(next, Is.EqualTo(0L));
    }

    [Test]
    public async Task GetNextSequenceAsync_advances_on_append()
    {
        var grain = CreateGrain();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);

        var next = await grain.GetNextSequenceAsync(CancellationToken.None);

        Assert.That(next, Is.EqualTo(2L));
    }

    [Test]
    public void GetNextSequenceAsync_observes_cancellation()
    {
        var grain = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.GetNextSequenceAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetEntryCountAsync_reflects_appended_entries()
    {
        var grain = CreateGrain();
        await grain.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        await grain.AppendAsync(MakeEntry("c"), CancellationToken.None);

        var count = await grain.GetEntryCountAsync(CancellationToken.None);

        Assert.That(count, Is.EqualTo(3L));
    }

    [Test]
    public void GetEntryCountAsync_observes_cancellation()
    {
        var grain = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.GetEntryCountAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void AppendAsync_propagates_storage_failures()
    {
        var state = new FakePersistentState<ReplogShardState>
        {
            ThrowOnWrite = new InvalidOperationException("boom"),
        };
        var grain = CreateGrain(state);

        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("a"), CancellationToken.None),
            Throws.InvalidOperationException.With.Message.EqualTo("boom"));
    }

    [Test]
    public async Task AppendAsync_rolls_back_in_memory_state_on_storage_failure()
    {
        var state = new FakePersistentState<ReplogShardState>
        {
            ThrowOnWrite = new InvalidOperationException("transient"),
        };
        var grain = CreateGrain(state);

        Assert.That(
            async () => await grain.AppendAsync(MakeEntry("a"), CancellationToken.None),
            Throws.InvalidOperationException);

        // The failed entry must not linger in memory; otherwise reads
        // would return entries that were never persisted, and the next
        // append would skip a sequence number.
        Assert.That(state.State.Entries, Is.Empty);

        var nextSeq = await grain.AppendAsync(MakeEntry("b"), CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(nextSeq, Is.EqualTo(0L));
            Assert.That(state.State.Entries, Has.Count.EqualTo(1));
            Assert.That(state.State.Entries[0].Entry.Key, Is.EqualTo("b"));
        });
    }

    [Test]
    public async Task Sequence_numbers_persist_through_state_round_trip()
    {
        // Simulate an activation re-bind: the new grain instance sees the
        // existing entries and assigns the next sequence number.
        var state = new FakePersistentState<ReplogShardState>();
        var first = CreateGrain(state);
        await first.AppendAsync(MakeEntry("a"), CancellationToken.None);
        await first.AppendAsync(MakeEntry("b"), CancellationToken.None);

        var rebound = CreateGrain(state);
        var seq = await rebound.AppendAsync(MakeEntry("c"), CancellationToken.None);

        Assert.That(seq, Is.EqualTo(2L));
    }
}
