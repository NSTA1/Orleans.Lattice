using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// The durable poison list behind a dead-lettered prepare (issue #4494): it is
/// persisted with the cursors, retires only once the origin registry no longer
/// holds the decided saga's row and the cursor has passed every tail sampled
/// after that, fails closed when full, and follows the source-log rebind
/// policy.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    /// <summary>A registry whose stored row for each saga the test controls; absent means no row.</summary>
    private static (ITxRegistryGrain Registry, Dictionary<Guid, TxStatus> Rows) ControlledRegistry()
    {
        var rows = new Dictionary<Guid, TxStatus>();
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetRecordedStatusAsync(Arg.Any<Guid>())
            .Returns(call => Task.FromResult(rows.TryGetValue(call.Arg<Guid>(), out var s) ? s : TxStatus.InFlight));
        return (registry, rows);
    }

    private static async Task ProbeAndPumpAsync(ReplicationShipperGrain grain, int ticks)
    {
        for (var i = 0; i < ticks; i++)
        {
            grain.ExpirePoisonRetirementProbeForTesting();
            grain.ResetBackoffForTesting();
            await grain.PumpForTestingAsync(CancellationToken.None);
        }
    }

    [Test]
    public async Task Poisoned_saga_retires_only_after_its_registry_row_is_gone_and_the_cursor_passes_the_sampled_tails()
    {
        var (dlq, _) = RecordingDeadLetters();
        var (registry, rows) = ControlledRegistry();
        var (grain, state, feeds, _) = CreateOrderingShipper(
            OrderingOptions(partitions: 2, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(2),
            deadLetters: dlq,
            txRegistry: registry);
        grain.SetPoisonRetirementGraceForTesting(TimeSpan.Zero);
        var txid = Guid.NewGuid();
        rows[txid] = TxStatus.Committed;
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[1].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 2));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 2, ticks: 3));
        feeds[1].Append(CommitEntry(txid, shardIndex: 1, shardCount: 2, ticks: 4));

        await ProbeAndPumpAsync(grain, 4);
        Assert.That(state.State.PoisonedSagas.Keys, Is.EquivalentTo(new[] { txid }),
            "every stamped terminal is parked, but the registry still holds the row, so a late terminal can still follow");

        rows.Remove(txid);
        feeds[0].Append(MakeEntry("after", ticks: 5));
        await ProbeAndPumpAsync(grain, 3);

        Assert.That(state.State.PoisonedSagas, Is.Empty,
            "the row is gone and the cursor has passed every tail sampled after that, so no record of the saga can follow");
    }

    [Test]
    public async Task Poisoned_saga_does_not_sample_tails_until_the_row_has_been_absent_for_the_grace()
    {
        // A split sweep can read the decision just before the purge and append
        // its late terminal after it; the grace keeps that terminal below the
        // sampled tails.
        var (dlq, _) = RecordingDeadLetters();
        var (registry, rows) = ControlledRegistry();
        var (grain, state, feeds, _) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(1),
            deadLetters: dlq,
            txRegistry: registry);
        var txid = Guid.NewGuid();
        rows[txid] = TxStatus.Committed;
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 1, ticks: 1));
        await ProbeAndPumpAsync(grain, 2);

        rows.Remove(txid);
        feeds[0].Append(MakeEntry("after", ticks: 2));
        await ProbeAndPumpAsync(grain, 3);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.PoisonedSagas.Keys, Is.EquivalentTo(new[] { txid }),
                "inside the grace the saga stays poisoned");
            Assert.That(state.State.PoisonedSagas[txid].RetireAfterTails, Is.Null, "no tails are sampled inside the grace");
            Assert.That(state.State.PoisonedSagas[txid].AbsentSinceUtcTicks, Is.Not.Zero);
        });
    }

    [Test]
    public async Task Poisoned_saga_whose_decision_was_never_observed_does_not_retire()
    {
        // A registry with no row is ambiguous until a decision has been seen:
        // the saga may simply not be decided yet.
        var (dlq, _) = RecordingDeadLetters();
        var (registry, _) = ControlledRegistry();
        var (grain, state, feeds, _) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(1),
            deadLetters: dlq,
            txRegistry: registry);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[0].Append(MakeEntry("after", ticks: 2));

        await ProbeAndPumpAsync(grain, 4);

        Assert.That(state.State.PoisonedSagas.Keys, Is.EquivalentTo(new[] { txid }));
    }

    [Test]
    public async Task Poisoned_saga_survives_a_reactivation_and_its_terminal_is_still_parked()
    {
        var options = OrderingOptions(partitions: 1, batchSize: 1);
        var walEncoder = new StubWalRecordEncoder();
        var (dlq, parked) = RecordingDeadLetters();
        var (grain, state, feeds, _) = CreateOrderingShipper(
            options, walEncoder: walEncoder, modeResolver: ResolverFailingOnCalls(1), deadLetters: dlq);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 1, ticks: 1));
        await PumpTicksAsync(grain, 2);
        Assert.That(state.State.PoisonedSagas.Keys, Is.EquivalentTo(new[] { txid }), "precondition: the saga is poisoned");

        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 1, ticks: 2));
        var (restarted, _, _, after) = CreateOrderingShipper(
            options, seedState: state.State, feeds: feeds, walEncoder: walEncoder, deadLetters: dlq);
        await PumpTicksAsync(restarted, 2);

        AssertSagaNeverCommittedOnThePeer(after, txid);
        Assert.That(parked.Count(p => p.Record.Op == MutationKind.TxCommit && p.Reason == LatticeReplicationMetrics.ReasonPoisonedSaga),
            Is.EqualTo(1), "the new activation parks the terminal from the persisted poison list");
    }

    [Test]
    public async Task Full_poison_list_fails_closed_and_does_not_advance_past_the_failing_batch()
    {
        // Evicting a poison entry would let that saga's terminal through torn, so
        // a full list refuses to park the batch and the cursor stays put.
        var seed = new ReplicationShipperState();
        for (var i = 0; i < ReplicationShipperGrain.PoisonedSagaCapacity; i++)
        {
            var id = Guid.NewGuid();
            seed.PoisonedSagas[id] = new PoisonedSaga { TransactionId = id };
        }

        var (dlq, parked) = RecordingDeadLetters();
        var (grain, state, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            seedState: seed,
            modeResolver: ResolverFailingOnCalls(1, 2, 3),
            deadLetters: dlq);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 1, ticks: 1));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 1, ticks: 2));

        await PumpTicksAsync(grain, 3);

        Assert.Multiple(() =>
        {
            Assert.That(parked, Is.Empty, "nothing is parked while the poison list cannot take the saga");
            Assert.That(stream.Applied.SelectMany(b => b), Is.Empty, "nothing ships past the failing batch");
            Assert.That(state.State.PartitionCursors.GetValueOrDefault(0), Is.Zero, "the cursor does not advance");
            Assert.That(state.State.PoisonedSagas.ContainsKey(txid), Is.False);
        });
    }

    private static async Task<(ReplicationShipperGrain Grain, FakePersistentState<ReplicationShipperState> State, AppliedStream Stream, StubReplogShardGrain[] Rebound, ILatticeRegistry Registry, IReplicationDeadLetterGrain Dlq, Guid TxId)>
        PoisonASagaBeforeARebindAsync()
    {
        var walEncoder = new StubWalRecordEncoder();
        var rebound = new[] { new StubReplogShardGrain(walEncoder) };
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(Arg.Any<string>()).Returns(Tree);
        var (dlq, _) = RecordingDeadLetters();
        var (grain, state, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            walEncoder: walEncoder, reboundFeeds: rebound, registry: registry,
            modeResolver: ResolverFailingOnCalls(1), deadLetters: dlq);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 1, ticks: 1));
        await PumpTicksAsync(grain, 2);
        Assert.That(state.State.PoisonedSagas.Keys, Is.EquivalentTo(new[] { txid }), "precondition: the saga is poisoned");

        registry.ResolveAsync(Arg.Any<string>()).Returns(ReboundPhysical);
        return (grain, state, stream, rebound, registry, dlq, txid);
    }

    [Test]
    public async Task Rebind_outside_a_saga_pause_keeps_the_poison_for_the_mirrored_terminal()
    {
        var (grain, state, stream, rebound, _, _, txid) = await PoisonASagaBeforeARebindAsync();
        rebound[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 1, ticks: 2));

        await grain.NotifySourceIdentityChangedAsync(ReboundPhysical, CancellationToken.None);
        await PumpTicksAsync(grain, 2);

        AssertSagaNeverCommittedOnThePeer(stream, txid);
        Assert.That(state.State.PoisonedSagas.Keys, Is.EquivalentTo(new[] { txid }),
            "the poison survives a rebind outside a saga pause, so the terminal mirrored into the new log is parked");
    }

    [Test]
    public async Task Rebind_during_a_saga_pause_drops_the_poison_list()
    {
        // A coordinated restore resets both clusters to the cut, so no poisoned
        // saga survives it.
        var (grain, state, _, _, _, _, _) = await PoisonASagaBeforeARebindAsync();

        await grain.PauseShippingAsync("restore-saga", CancellationToken.None);
        await grain.NotifySourceIdentityChangedAsync(ReboundPhysical, CancellationToken.None);
        await grain.ResumeShippingAsync("restore-saga", CancellationToken.None);

        Assert.That(state.State.PoisonedSagas, Is.Empty);
    }
}
