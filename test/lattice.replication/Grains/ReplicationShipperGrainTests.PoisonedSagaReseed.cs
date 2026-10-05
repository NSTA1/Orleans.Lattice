using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// A shipper-side saga poison asks the peer to re-seed (issue #4620). The
/// poison (#4570) keeps the peer from committing a saga whose prepare was
/// dead-lettered, but the saga is then never shipped to the peer again, so
/// without a re-seed the peer serves it as never written for good while this
/// cluster has it decided (the cross-cluster model's
/// <c>RCommittedEventuallyVisible</c>). Poisoning now marks the peer re-seed
/// required through the forced-gap path (#4577): every push carries the
/// export epoch, the peer re-bootstraps, and the echo clears the marker.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private static async Task<(ReplicationShipperGrain Grain, FakePersistentState<ReplicationShipperState> State, AppliedStream Stream, Guid TxId, StubReplogShardGrain[] Feeds)>
        PoisonASagaForReseedAsync(long exportEpoch)
    {
        var (dlq, _) = RecordingDeadLetters();
        var (grain, state, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(2),
            deadLetters: dlq,
            exportEpoch: exportEpoch);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[0].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 2));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 1, ticks: 3));
        feeds[0].Append(MakeEntry("after", ticks: 4));
        await PumpTicksAsync(grain, 4);
        return (grain, state, stream, txid, feeds);
    }

    [Test]
    public async Task Dead_lettered_prepare_marks_the_peer_reseed_required_and_every_push_requests_the_re_seed()
    {
        var (_, state, stream, txid, _) = await PoisonASagaForReseedAsync(exportEpoch: 7);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.PoisonedSagas.Keys, Is.EquivalentTo(new[] { txid }), "precondition: the saga is poisoned");
            Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(7L),
                "poisoning a saga must ask the peer to re-seed, or the peer never sees the saga");
            Assert.That(stream.ReseedRequests.Skip(1), Is.Not.Empty.And.All.EqualTo(7L),
                "every push after the poison carries the re-seed request");
            AssertSagaNeverCommittedOnThePeer(stream, txid);
        });
    }

    [Test]
    public async Task Peer_re_seed_echo_clears_the_marker_and_the_rewind_keeps_the_poisoned_saga_off_the_peer()
    {
        var (grain, state, stream, txid, feeds) = await PoisonASagaForReseedAsync(exportEpoch: 7);
        Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(7L), "precondition: the peer must be re-seeded");

        // The peer re-bootstraps from an export after the marker and echoes it
        // on the next ack.
        stream.EchoedBootstrapEpoch = () => 8;
        feeds[0].Append(MakeEntry("after-reseed", ticks: 10));
        grain.ExpirePoisonRetirementProbeForTesting();
        await PumpTicksAsync(grain, 3);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ReseedRequiredEpoch, Is.Null, "the echo of a later export clears the marker");
            Assert.That(state.State.PoisonedSagas.Keys, Does.Contain(txid),
                "the saga stays poisoned, so the rewind parks its records again instead of re-seeding again");
            AssertSagaNeverCommittedOnThePeer(stream, txid);
            Assert.That(stream.Applied.SelectMany(b => b).Count(r => r.Key == "after"), Is.GreaterThanOrEqualTo(1),
                "plain writes keep shipping");
        });
    }

    [Test]
    public async Task A_poison_whose_park_the_full_queue_refuses_still_owes_the_re_seed()
    {
        // The full queue refuses every park (#4603), so the batch is not
        // advanced past and a later attempt sees the saga already poisoned.
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        dlq.EnqueueAsync(
                Arg.Any<WalRecord>(), Arg.Any<string>(), Arg.Any<int>(),
                Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<long>(new ReplicationDeadLetterQueueFullException(Tree, 1)));
        var (grain, state, feeds, _) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(2),
            deadLetters: dlq,
            exportEpoch: 5);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[0].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 2));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 1, ticks: 3));
        await PumpTicksAsync(grain, 4);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.PoisonedSagas.Keys, Does.Contain(txid), "precondition: the saga is poisoned");
            Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(5L),
                "the re-seed is owed with the poison, even though the queue refused the park");
        });
    }
}
