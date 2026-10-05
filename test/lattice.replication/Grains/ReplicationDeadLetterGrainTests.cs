using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests.Grains;

[TestFixture]
public class ReplicationDeadLetterGrainTests
{
    private const string TreeId = "tree";

    private static Serializer<DeadLetterEntry> Serializer { get; } =
        new ServiceCollection().AddSerializer().BuildServiceProvider().GetRequiredService<Serializer<DeadLetterEntry>>();

    private static async Task<(ReplicationDeadLetterGrain grain, SortedDictionary<string, byte[]> data, LatticeReplicationOptions options)> CreateGrainAsync(
        (Orleans.Lattice.BPlusTree.Grains.ISystemLattice store, SortedDictionary<string, byte[]> data)? backing = null,
        int capacity = 1000,
        IReplicationOriginFrontierGrain? frontier = null)
    {
        var (store, data) = backing ?? FakeSystemLattice.Create();
        var context = Substitute.For<IGrainContext>();
        var grainFactory = Substitute.For<IGrainFactory>();
        frontier ??= Substitute.For<IReplicationOriginFrontierGrain>();
        grainFactory.GetGrain<IReplicationOriginFrontierGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(frontier);
        var options = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            DeadLetterQueueCapacity = capacity,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options);
        var grain = new ReplicationDeadLetterGrain(context, grainFactory, monitor, Serializer);
        await grain.InitializeForTestingAsync(TreeId, store, CancellationToken.None);
        return (grain, data, options);
    }

    private static WalRecord MakeEntry(string key = "k") => new()
    {
        TreeId = TreeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1, 2, 3 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-b",
    };

    [Test]
    public async Task EnqueueAsync_assigns_increasing_entry_ids_starting_at_one()
    {
        var (grain, _, _) = await CreateGrainAsync();

        var id1 = await grain.EnqueueAsync(MakeEntry("a"), "boom", 5, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        var id2 = await grain.EnqueueAsync(MakeEntry("b"), "boom", 5, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);

        Assert.That(new[] { id1, id2 }, Is.EqualTo(new[] { 1L, 2L }));
    }

    [Test]
    public async Task EnqueueAsync_writes_through_to_the_system_tree()
    {
        var (grain, data, _) = await CreateGrainAsync();

        await grain.EnqueueAsync(MakeEntry("a"), "boom", 5, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);

        Assert.That(data.Keys, Has.Count.EqualTo(1));
        var key = data.Keys.Single();
        Assert.That(key, Does.StartWith("e/"));
    }

    [Test]
    public async Task No_head_cursor_row_is_ever_written_preserving_on_disk_format()
    {
        var (grain, data, _) = await CreateGrainAsync();

        await grain.EnqueueAsync(MakeEntry("a"), "boom", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        var id2 = await grain.EnqueueAsync(MakeEntry("b"), "boom", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        await grain.DiscardAsync(id2, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(data.ContainsKey(LatticeQueueCore.HeadCursorKey), Is.False);
            Assert.That(data.Keys, Has.All.StartWith("e/"));
        });
    }

    [Test]
    public async Task EnqueueAsync_throws_on_null_failure_reason()
    {
        var (grain, _, _) = await CreateGrainAsync();

        Assert.That(
            async () => await grain.EnqueueAsync(MakeEntry(), null!, 0, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task EnqueueAsync_throws_on_null_reason_tag()
    {
        var (grain, _, _) = await CreateGrainAsync();

        Assert.That(
            async () => await grain.EnqueueAsync(MakeEntry(), "boom", 0, null!, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task EnqueueAsync_throws_on_empty_reason_tag()
    {
        var (grain, _, _) = await CreateGrainAsync();

        Assert.That(
            async () => await grain.EnqueueAsync(MakeEntry(), "boom", 0, string.Empty, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task EnqueueAsync_emits_supplied_reason_tag_on_enqueued_counter()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.dead_letter.enqueued");
        var (grain, _, _) = await CreateGrainAsync();

        await grain.EnqueueAsync(MakeEntry(), "boom", 1, LatticeReplicationMetrics.ReasonSchema, CancellationToken.None);

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        Assert.That(collector.Measurements.Single().Tags,
            Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "reason" && (string?)t.Value == LatticeReplicationMetrics.ReasonSchema));
    }

    [Test]
    public async Task EnqueueAsync_refuses_rather_than_evicts_when_capacity_reached()
    {
        // #4603: every parked entry is the only copy of an acknowledged write,
        // so a full queue refuses the next one instead of evicting the oldest.
        var (grain, data, _) = await CreateGrainAsync(capacity: 2);

        var id1 = await grain.EnqueueAsync(MakeEntry("a"), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        var id2 = await grain.EnqueueAsync(MakeEntry("b"), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);

        Assert.That(
            async () => await grain.EnqueueAsync(MakeEntry("c"), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None),
            Throws.InstanceOf<ReplicationDeadLetterQueueFullException>());

        var entries = await grain.ListAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(entries.Select(e => e.EntryId), Is.EqualTo(new[] { id1, id2 }), "nothing was evicted");
            Assert.That(data.Keys, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public async Task EnqueueAsync_is_idempotent_by_entry_identity()
    {
        var (grain, _, _) = await CreateGrainAsync(capacity: 1);
        var entry = MakeEntry("a");

        var first = await grain.EnqueueAsync(entry, "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        var again = await grain.EnqueueAsync(entry, "y", 2, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);

        var count = await grain.CountAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(again, Is.EqualTo(first), "a re-shipped copy keeps its one slot");
            Assert.That(count, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task EnqueueAsync_emits_dead_letter_enqueued_counter_with_tree_and_unknown_reason()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.dead_letter.enqueued");
        var (grain, _, _) = await CreateGrainAsync();

        await grain.EnqueueAsync(MakeEntry(), "boom", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.Multiple(() =>
        {
            Assert.That(only.Value, Is.EqualTo(1L));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "tree" && (string?)t.Value == TreeId));
            Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "reason" && (string?)t.Value == LatticeReplicationMetrics.ReasonUnknown));
        });
    }

    [Test]
    public async Task EnqueueAsync_emits_refused_counter_and_no_removal_on_capacity_overflow()
    {
        using var removed = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.dead_letter.removed");
        using var refused = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.dead_letter.refused");
        var (grain, _, _) = await CreateGrainAsync(capacity: 1);

        await grain.EnqueueAsync(MakeEntry("a"), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        Assert.ThrowsAsync<ReplicationDeadLetterQueueFullException>(
            async () => await grain.EnqueueAsync(MakeEntry("b"), "x", 1, LatticeReplicationMetrics.ReasonSchema, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(removed.Measurements, Is.Empty, "nothing is evicted");
            Assert.That(refused.Measurements, Has.Count.EqualTo(1));
            Assert.That(refused.Measurements.Single().Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
                t.Key == "reason" && (string?)t.Value == LatticeReplicationMetrics.ReasonSchema));
        });
    }

    [Test]
    public async Task DiscardAsync_records_a_foreign_origin_entry_as_lost_before_removing_it()
    {
        var frontier = Substitute.For<IReplicationOriginFrontierGrain>();
        var (grain, _, _) = await CreateGrainAsync(frontier: frontier);
        var entry = MakeEntry("a");
        var id = await grain.EnqueueAsync(entry, "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);

        Assert.That(await grain.DiscardAsync(id, CancellationToken.None), Is.True);

        await frontier.Received(1).RecordLostAsync(
            Arg.Is<IReadOnlyCollection<HybridLogicalClock>>(l => l.Count == 1 && l.Contains(entry.Timestamp)),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DiscardAsync_keeps_the_entry_when_the_lost_mark_cannot_be_recorded()
    {
        var frontier = Substitute.For<IReplicationOriginFrontierGrain>();
        frontier.RecordLostAsync(default!, default).ReturnsForAnyArgs(Task.FromException(new TimeoutException("frontier down")));
        var (grain, _, _) = await CreateGrainAsync(frontier: frontier);
        var id = await grain.EnqueueAsync(MakeEntry("a"), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);

        Assert.ThrowsAsync<TimeoutException>(async () => await grain.DiscardAsync(id, CancellationToken.None));

        Assert.That(await grain.CountAsync(CancellationToken.None), Is.EqualTo(1), "never deleted without its lost mark");
    }

    [Test]
    public async Task DiscardAsync_does_not_record_a_local_origin_entry_as_lost()
    {
        var frontier = Substitute.For<IReplicationOriginFrontierGrain>();
        var (grain, _, _) = await CreateGrainAsync(frontier: frontier);
        var id = await grain.EnqueueAsync(MakeEntry("a") with { OriginClusterId = "site-a" }, "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);

        await grain.DiscardAsync(id, CancellationToken.None);

        await frontier.DidNotReceiveWithAnyArgs().RecordLostAsync(default!, default);
        await frontier.DidNotReceiveWithAnyArgs().SetHeldAsync(default!, default!, default);
    }

    [Test]
    public async Task A_parked_foreign_write_is_published_as_held_before_the_enqueue_returns_and_released_on_removal()
    {
        var frontier = Substitute.For<IReplicationOriginFrontierGrain>();
        var (grain, _, _) = await CreateGrainAsync(frontier: frontier);
        var entry = MakeEntry("a");

        var id = await grain.EnqueueAsync(entry, "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        var heldWhileParked = await grain.IsHoldingAsync("site-b", entry.Timestamp, CancellationToken.None);
        await grain.RemoveReplayedAsync(id, CancellationToken.None);
        var heldAfterRemoval = await grain.IsHoldingAsync("site-b", entry.Timestamp, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(heldWhileParked, Is.True);
            Assert.That(heldAfterRemoval, Is.False);
        });
        await frontier.Received(1).SetHeldAsync(
            ReplicationOriginFrontierGrain.DeadLetterSource(TreeId),
            Arg.Is<IReadOnlyCollection<HybridLogicalClock>>(h => h.Count == 1 && h.Contains(entry.Timestamp)),
            Arg.Any<CancellationToken>());
        await frontier.Received(1).SetHeldAsync(
            ReplicationOriginFrontierGrain.DeadLetterSource(TreeId),
            Arg.Is<IReadOnlyCollection<HybridLogicalClock>>(h => h.Count == 0),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task An_enqueue_whose_held_publication_fails_throws_so_the_write_is_not_acknowledged()
    {
        var frontier = Substitute.For<IReplicationOriginFrontierGrain>();
        frontier.SetHeldAsync(default!, default!, default).ReturnsForAnyArgs(Task.FromException(new TimeoutException("frontier down")));
        var (grain, _, _) = await CreateGrainAsync(frontier: frontier);

        Assert.ThrowsAsync<TimeoutException>(
            async () => await grain.EnqueueAsync(MakeEntry("a"), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None));
    }

    [Test]
    public async Task ListAsync_returns_entries_in_ascending_id_order()
    {
        var (grain, _, _) = await CreateGrainAsync();
        await grain.EnqueueAsync(MakeEntry("a"), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        await grain.EnqueueAsync(MakeEntry("b"), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        await grain.EnqueueAsync(MakeEntry("c"), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);

        var entries = await grain.ListAsync(CancellationToken.None);

        Assert.That(entries.Select(e => e.EntryId), Is.EqualTo(new[] { 1L, 2L, 3L }));
    }

    [Test]
    public async Task CountAsync_reflects_current_queue_size()
    {
        var (grain, _, _) = await CreateGrainAsync();
        Assert.That(await grain.CountAsync(CancellationToken.None), Is.EqualTo(0));

        await grain.EnqueueAsync(MakeEntry(), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        Assert.That(await grain.CountAsync(CancellationToken.None), Is.EqualTo(1));
    }

    [Test]
    public async Task DiscardAsync_removes_the_entry_and_emits_discarded_reason()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.dead_letter.removed");
        var (grain, data, _) = await CreateGrainAsync();
        var id = await grain.EnqueueAsync(MakeEntry(), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);

        var removed = await grain.DiscardAsync(id, CancellationToken.None);
        var count = await grain.CountAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(removed, Is.True);
            Assert.That(count, Is.EqualTo(0));
            Assert.That(data, Is.Empty);
        });

        var discardMeasurements = collector.Measurements
            .Where(m => m.Tags.Any(t => t.Key == "reason" && (string?)t.Value == LatticeReplicationMetrics.ReasonDiscarded))
            .ToList();
        Assert.That(discardMeasurements, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task DiscardAsync_returns_false_for_unknown_id()
    {
        var (grain, _, _) = await CreateGrainAsync();

        Assert.That(await grain.DiscardAsync(999, CancellationToken.None), Is.False);
    }

    [Test]
    public async Task RemoveReplayedAsync_emits_replayed_reason_on_removed_counter()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            "orleans.lattice.replication.dead_letter.removed");
        var (grain, _, _) = await CreateGrainAsync();
        var id = await grain.EnqueueAsync(MakeEntry(), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);

        var removed = await grain.RemoveReplayedAsync(id, CancellationToken.None);

        Assert.That(removed, Is.True);
        var replayedMeasurements = collector.Measurements
            .Where(m => m.Tags.Any(t => t.Key == "reason" && (string?)t.Value == LatticeReplicationMetrics.ReasonReplayed))
            .ToList();
        Assert.That(replayedMeasurements, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task TryGetAsync_returns_the_parked_entry()
    {
        var (grain, _, _) = await CreateGrainAsync();
        var id = await grain.EnqueueAsync(MakeEntry("hello"), "boom", 5, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);

        var parked = await grain.TryGetAsync(id, CancellationToken.None);

        Assert.That(parked, Is.Not.Null);
        Assert.That(parked!.Value.EntryId, Is.EqualTo(id));
        Assert.That(parked.Value.Entry.Key, Is.EqualTo("hello"));
        Assert.That(parked.Value.FailureReason, Is.EqualTo("boom"));
    }

    [Test]
    public async Task TryGetAsync_returns_null_for_unknown_id()
    {
        var (grain, _, _) = await CreateGrainAsync();

        Assert.That(await grain.TryGetAsync(999, CancellationToken.None), Is.Null);
    }

    [Test]
    public async Task InitializeForTestingAsync_rehydrates_cache_from_existing_store_contents()
    {
        var backing = FakeSystemLattice.Create();
        // Pre-seed via a first grain instance, then load a second.
        var (first, _, _) = await CreateGrainAsync(backing);
        await first.EnqueueAsync(MakeEntry("a"), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        await first.EnqueueAsync(MakeEntry("b"), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);

        var (second, _, _) = await CreateGrainAsync(backing);

        var entries = await second.ListAsync(CancellationToken.None);
        Assert.That(entries.Select(e => e.Entry.Key), Is.EqualTo(new[] { "a", "b" }));

        // Next id continues from the highest seen.
        var next = await second.EnqueueAsync(MakeEntry("c"), "x", 1, LatticeReplicationMetrics.ReasonUnknown, CancellationToken.None);
        Assert.That(next, Is.EqualTo(3L));
    }

    [Test]
    public void EntryKey_pads_to_nineteen_digits()
    {
        Assert.That(ReplicationDeadLetterGrain.EntryKey(7), Is.EqualTo("e/0000000000000000007"));
    }

    [Test]
    public void BackingTreeId_lives_under_the_replog_system_prefix()
    {
        var id = ReplicationDeadLetterGrain.BackingTreeId("my-tree");
        Assert.That(id, Is.EqualTo("_lattice_replog_dlq_my-tree"));
    }
}

