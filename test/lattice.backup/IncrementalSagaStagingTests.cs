using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="IncrementalSagaStaging{TEntry}"/>: how an incremental
/// backup resolves the atomic writes in its delta window against the capture's
/// decision snapshot (issue #4589).
/// </summary>
[TestFixture]
public sealed class IncrementalSagaStagingTests
{
    private static readonly Guid Tx = Guid.Parse("11111111-1111-1111-1111-111111111111");

    [Test]
    public void An_ordinary_write_is_not_staged()
    {
        var staging = new IncrementalSagaStaging<string>();
        var ordinary = new LatticeMutation { Kind = MutationKind.Set, Key = "k", TransactionId = Guid.NewGuid() };

        Assert.That(staging.TryStage(ordinary, 0, 0, "k"), Is.False);
    }

    [Test]
    public void A_prepared_write_with_no_transaction_id_is_swallowed_and_never_emitted()
    {
        var staging = new IncrementalSagaStaging<string>();

        Assert.Multiple(() =>
        {
            Assert.That(staging.TryStage(Prepare(Guid.Empty, 0, 1, 0), 0, 0, "k"), Is.True);
            Assert.That(staging.TakeUnresolved(), Is.Empty);
            Assert.That(Drain(staging), Is.Empty);
        });
    }

    [Test]
    public void A_committed_batch_is_emitted_once_and_whole_when_its_prepares_cover_it()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TryStage(Prepare(Tx, 0, 2, 1), 0, 1, "a");
        Resolve(staging, TxStatus.Committed);

        Assert.That(Drain(staging), Is.Empty, "half the batch is never emitted");

        staging.TryStage(Prepare(Tx, 1, 2, 2), 1, 5, "b");
        Assert.Multiple(() =>
        {
            Assert.That(Drain(staging), Is.EquivalentTo(new[] { "a", "b" }));
            Assert.That(Drain(staging), Is.Empty, "a batch is emitted once");
        });
    }

    [Test]
    public void A_retried_prepare_replaces_its_index_rather_than_completing_the_batch()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TryStage(Prepare(Tx, 0, 2, 1), 0, 1, "a");
        staging.TryStage(Prepare(Tx, 0, 2, 2), 0, 2, "a-retried");
        Resolve(staging, TxStatus.Committed);
        staging.Finish();

        Assert.Multiple(() =>
        {
            Assert.That(Drain(staging), Is.Empty);
            Assert.That(staging.RequiresFullFallback, Is.True, "one index of two is not the whole batch");
        });
    }

    [Test]
    public void A_committed_batch_the_window_does_not_cover_requires_a_full_backup()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TryStage(Prepare(Tx, 1, 2, 1), 0, 4, "b");
        staging.TryStage(Terminal(Tx, MutationKind.TxCommit, 0, 1, 2), 0, 5, null);
        Resolve(staging, TxStatus.Committed);
        staging.Finish();

        Assert.That(staging.RequiresFullFallback, Is.True);
    }

    [Test]
    public void A_commit_terminal_alone_requires_a_full_backup()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TryStage(Terminal(Tx, MutationKind.TxCommit, 0, 1, 2), 0, 5, null);
        Resolve(staging, TxStatus.Committed);
        staging.Finish();

        Assert.That(staging.RequiresFullFallback, Is.True, "its prepares all precede the window");
    }

    [Test]
    public void A_commit_terminal_the_decision_snapshot_does_not_hold_as_committed_fails_safe()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TryStage(Prepare(Tx, 0, 1, 1), 0, 1, "a");
        staging.TryStage(Terminal(Tx, MutationKind.TxCommit, 0, 1, 2), 0, 2, null);
        Resolve(staging, TxStatus.InFlight);
        staging.Finish();

        Assert.That(staging.RequiresFullFallback, Is.True);
    }

    [Test]
    public void An_aborted_batch_is_dropped_and_holds_nothing()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TryStage(Prepare(Tx, 0, 1, 1), 0, 1, "a");
        staging.TryStage(Terminal(Tx, MutationKind.TxAbort, 0, 1, 2), 0, 2, null);

        Assert.Multiple(() =>
        {
            Assert.That(staging.TakeUnresolved(), Is.Empty, "an abort terminal needs no lookup");
            Assert.That(Drain(staging), Is.Empty);
            Assert.That(staging.HeldOffsets, Is.Empty);
            Assert.That(staging.BlockedFloor, Is.Null);
        });
    }

    [TestCase(1)]
    [TestCase(2)]
    public void A_batch_the_snapshot_holds_as_settled_but_not_committed_is_dropped(int settled)
    {
        var status = settled == 1 ? TxStatus.Aborted : TxStatus.Indeterminate;
        var staging = new IncrementalSagaStaging<string>();
        staging.TryStage(Prepare(Tx, 0, 1, 1), 0, 1, "a");
        Resolve(staging, status);
        staging.Finish();

        Assert.Multiple(() =>
        {
            Assert.That(Drain(staging), Is.Empty);
            Assert.That(staging.HeldOffsets, Is.Empty);
            Assert.That(staging.RequiresFullFallback, Is.False);
        });
    }

    [Test]
    public void An_undecided_batch_is_dropped_and_holds_the_frontier_at_its_earliest_entry_per_partition()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TryStage(Prepare(Tx, 0, 2, 7), 0, 9, "a");
        staging.TryStage(Prepare(Tx, 1, 2, 3), 1, 4, "b");
        Resolve(staging, TxStatus.InFlight);
        staging.TryStage(Prepare(Tx, 0, 2, 9), 0, 12, "a");
        staging.Finish();

        Assert.Multiple(() =>
        {
            Assert.That(Drain(staging), Is.Empty);
            Assert.That(staging.HeldOffsets, Is.EquivalentTo(new Dictionary<int, long> { [0] = 9, [1] = 4 }));
            Assert.That(staging.BlockedFloor, Is.EqualTo(Hlc(3)));
            Assert.That(staging.RequiresFullFallback, Is.False);
        });
    }

    [Test]
    public void A_transaction_with_no_decision_in_the_snapshot_is_undecided()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TryStage(Prepare(Tx, 0, 1, 1), 0, 1, "a");
        var pending = staging.TakeUnresolved();
        staging.ApplyDecisions(pending, new Dictionary<Guid, TxStatus>());

        Assert.Multiple(() =>
        {
            Assert.That(Drain(staging), Is.Empty);
            Assert.That(staging.HeldOffsets.ContainsKey(0), Is.True);
        });
    }

    [Test]
    public void A_committed_batch_is_emitted_but_holds_the_frontier_until_every_shard_terminal_is_read()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TryStage(Prepare(Tx, 0, 2, 1), 0, 1, "a");
        staging.TryStage(Prepare(Tx, 1, 2, 2), 1, 1, "b");
        staging.TryStage(Terminal(Tx, MutationKind.TxCommit, 0, 2, 3), 2, 1, null);
        Resolve(staging, TxStatus.Committed);

        Assert.That(Drain(staging), Is.EquivalentTo(new[] { "a", "b" }));
        Assert.That(staging.HeldOffsets, Is.Not.Empty, "one of two shard terminals is still to come");

        staging.TryStage(Terminal(Tx, MutationKind.TxCommit, 1, 2, 4), 2, 2, null);
        staging.Finish();

        Assert.Multiple(() =>
        {
            Assert.That(staging.HeldOffsets, Is.Empty, "every shard terminal has been read");
            Assert.That(staging.RequiresFullFallback, Is.False);
        });
    }

    [Test]
    public void Each_transaction_is_looked_up_once()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TryStage(Prepare(Tx, 0, 2, 1), 0, 1, "a");

        Assert.That(staging.TakeUnresolved(), Is.EqualTo(new[] { Tx }));

        staging.ApplyDecisions(new[] { Tx }, new Dictionary<Guid, TxStatus> { [Tx] = TxStatus.Committed });
        staging.TryStage(Prepare(Tx, 1, 2, 2), 0, 2, "b");

        Assert.That(staging.TakeUnresolved(), Is.Empty);
    }

    [Test]
    public void A_base_undecided_saga_committed_with_no_record_in_the_window_requires_a_full_backup()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TrackBaseUndecided(new[] { Tx });
        Resolve(staging, TxStatus.Committed);
        staging.Finish();

        Assert.Multiple(() =>
        {
            Assert.That(staging.RequiresFullFallback, Is.True, "its prepares all precede the window");
            Assert.That(staging.CarriedUndecided, Is.Empty);
        });
    }

    [Test]
    public void A_base_undecided_saga_committed_and_whole_in_the_window_is_emitted()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TrackBaseUndecided(new[] { Tx });
        staging.TryStage(Prepare(Tx, 0, 1, 1), 0, 1, "a");
        Resolve(staging, TxStatus.Committed);
        staging.TryStage(Terminal(Tx, MutationKind.TxCommit, 0, 1, 2), 0, 2, null);
        staging.Finish();

        Assert.Multiple(() =>
        {
            Assert.That(Drain(staging), Is.EqualTo(new[] { "a" }));
            Assert.That(staging.RequiresFullFallback, Is.False);
        });
    }

    [Test]
    public void A_base_undecided_saga_still_undecided_is_handed_on_without_pinning_the_wal()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TrackBaseUndecided(new[] { Tx, Guid.Empty });
        Resolve(staging, TxStatus.InFlight);
        staging.Finish();

        Assert.Multiple(() =>
        {
            Assert.That(staging.CarriedUndecided, Is.EqualTo(new[] { Tx }), "the empty id is ignored");
            Assert.That(staging.RequiresFullFallback, Is.False);
            Assert.That(staging.BlockedFloor, Is.Null, "a saga with no record in the window pins nothing");
            Assert.That(staging.HeldOffsets, Is.Empty);
        });
    }

    [TestCase(1)]
    [TestCase(2)]
    public void A_base_undecided_saga_since_settled_without_commit_is_dropped(int settled)
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TrackBaseUndecided(new[] { Tx });
        Resolve(staging, settled == 1 ? TxStatus.Aborted : TxStatus.Indeterminate);
        staging.Finish();

        Assert.Multiple(() =>
        {
            Assert.That(staging.CarriedUndecided, Is.Empty);
            Assert.That(staging.RequiresFullFallback, Is.False);
        });
    }

    [Test]
    public void A_base_captured_before_the_set_was_recorded_tracks_nothing()
    {
        var staging = new IncrementalSagaStaging<string>();
        staging.TrackBaseUndecided(null);

        Assert.That(staging.TakeUnresolved(), Is.Empty);
    }

    [Test]
    public void ApplyDecisions_validates_its_arguments()
    {
        var staging = new IncrementalSagaStaging<string>();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => staging.ApplyDecisions(null!, new Dictionary<Guid, TxStatus>()));
            Assert.Throws<ArgumentNullException>(() => staging.ApplyDecisions(Array.Empty<Guid>(), null!));
            Assert.Throws<ArgumentNullException>(() => staging.DrainCommitted(null!));
        });
    }

    private static void Resolve(IncrementalSagaStaging<string> staging, TxStatus status)
    {
        var pending = staging.TakeUnresolved();
        staging.ApplyDecisions(pending, pending.ToDictionary(t => t, _ => status));
    }

    private static List<string> Drain(IncrementalSagaStaging<string> staging)
    {
        var into = new List<string>();
        staging.DrainCommitted(into);
        return into;
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static LatticeMutation Prepare(Guid txId, int index, int size, long ticks) => new()
    {
        Kind = MutationKind.Set,
        Key = $"key-{index}",
        TransactionId = txId,
        IsPrepared = true,
        AtomicBatchIndex = index,
        AtomicBatchSize = size,
        Timestamp = Hlc(ticks),
    };

    private static LatticeMutation Terminal(Guid txId, MutationKind kind, int shard, int shardCount, long ticks) => new()
    {
        Kind = kind,
        Key = shard.ToString(System.Globalization.CultureInfo.InvariantCulture),
        TransactionId = txId,
        ShardIndex = shard,
        AtomicShardCount = shardCount,
        Timestamp = Hlc(ticks),
    };
}
