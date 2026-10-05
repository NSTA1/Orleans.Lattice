using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// A dead-lettered prepare poisons its saga at the shipper (issue #4494). A batch
/// the shipper cannot encode is parked on the dead-letter queue and the cursor
/// advances past it, which loses that prepare for the peer for good (a replay
/// applies it on this cluster, not on the peer). If any terminal of the saga
/// still shipped, the peer would commit the saga without that write - a torn
/// batch. Each test drives the real shipper over a real merge of its WAL
/// partitions, makes exactly one batch fail to encode, and checks the stream
/// the peer applied: no terminal of the saga, and the saga's later records parked
/// instead. This is the detector for the cross-cluster model's RAllOrNothing
/// under a lossy edge.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private const string PoisonedSagaReason = "poisoned_saga";

    /// <summary>A merge-mode resolver whose listed calls fail the batch's encode.</summary>
    private static ILatticeMergeModeResolver ResolverFailingOnCalls(params int[] failingCalls)
    {
        var calls = 0;
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Arg.Any<string>()).Returns<LatticeMergeMode?>(_ =>
        {
            calls++;
            if (failingCalls.Contains(calls))
            {
                throw new InvalidOperationException($"schema-shaped encode failure on call {calls}");
            }

            return LatticeMergeMode.LwwRegister;
        });
        return resolver;
    }

    private static (IReplicationDeadLetterGrain Dlq, List<(WalRecord Record, string Reason)> Parked) RecordingDeadLetters()
    {
        var parked = new List<(WalRecord Record, string Reason)>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        dlq.EnqueueAsync(
                Arg.Any<WalRecord>(), Arg.Any<string>(), Arg.Any<int>(),
                Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                parked.Add((call.ArgAt<WalRecord>(0), call.ArgAt<string>(3)));
                return Task.FromResult((long)parked.Count);
            });
        return (dlq, parked);
    }

    private static async Task PumpTicksAsync(ReplicationShipperGrain grain, int ticks)
    {
        for (var i = 0; i < ticks; i++)
        {
            grain.ResetBackoffForTesting();
            await grain.PumpForTestingAsync(CancellationToken.None);
        }
    }

    private static void AssertSagaNeverCommittedOnThePeer(AppliedStream stream, Guid txid)
    {
        var terminals = stream.Applied.SelectMany(b => b)
            .Where(r => r.TransactionId == txid && r.Op is MutationKind.TxCommit or MutationKind.TxAbort)
            .ToList();
        Assert.That(terminals, Is.Empty,
            "a terminal of a saga whose prepare was dead-lettered reached the peer, which commits the saga without that write (torn batch)");
    }

    [TestCase(1, TestName = "Terminal_consumed_after_its_saga_prepare_was_dead_lettered_is_parked_not_shipped")]
    [TestCase(2, TestName = "Pipelined_terminal_consumed_after_its_saga_prepare_was_dead_lettered_is_parked_not_shipped")]
    public async Task Terminal_consumed_after_its_saga_prepare_was_dead_lettered_is_parked_not_shipped(int window)
    {
        // Two partitions, so terminals are held. keyB's prepare is in the second
        // batch, which fails to encode and is dead-lettered. Both terminals are
        // read afterwards.
        var (dlq, parked) = RecordingDeadLetters();
        var (grain, _, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 2, batchSize: 1, window: window),
            modeResolver: ResolverFailingOnCalls(2),
            deadLetters: dlq);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[1].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 2));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 2, ticks: 3));
        feeds[1].Append(CommitEntry(txid, shardIndex: 1, shardCount: 2, ticks: 4));

        await PumpTicksAsync(grain, 4);

        Assert.Multiple(() =>
        {
            Assert.That(parked.Where(p => p.Record.IsPrepared).Select(p => (p.Record.Key, p.Reason)),
                Is.EqualTo(new[] { ("keyB", LatticeReplicationMetrics.ReasonSchema) }),
                "precondition: exactly keyB's prepare was dead-lettered by the encode failure");
            AssertSagaNeverCommittedOnThePeer(stream, txid);
            Assert.That(parked.Where(p => p.Record.Op == MutationKind.TxCommit).Select(p => (p.Record.ShardIndex, p.Reason)),
                Is.EquivalentTo(new[] { (0, PoisonedSagaReason), (1, PoisonedSagaReason) }),
                "every terminal of the poisoned saga is parked on the dead-letter queue instead");
            Assert.That(grain.HeldTerminalCountForTesting, Is.Zero);
        });
    }

    [Test]
    public async Task Terminal_held_before_its_saga_prepare_was_dead_lettered_is_parked_not_released()
    {
        // Partition 1 opens with an entry whose leaf clock runs ahead, so the
        // merge holds shard 0's terminal before it reaches keyB's prepare. keyB's
        // prepare then fails to encode (third batch) while that terminal is held.
        var (dlq, parked) = RecordingDeadLetters();
        var (grain, _, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 2, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(3),
            deadLetters: dlq);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 2, ticks: 3));
        feeds[1].Append(MakeEntry("ahead", ticks: 100));
        feeds[1].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 2));
        feeds[1].Append(CommitEntry(txid, shardIndex: 1, shardCount: 2, ticks: 4));

        await PumpTicksAsync(grain, 5);

        Assert.Multiple(() =>
        {
            Assert.That(parked.Where(p => p.Record.IsPrepared).Select(p => (p.Record.Key, p.Reason)),
                Is.EqualTo(new[] { ("keyB", LatticeReplicationMetrics.ReasonSchema) }),
                "precondition: exactly keyB's prepare was dead-lettered by the encode failure");
            AssertSagaNeverCommittedOnThePeer(stream, txid);
            Assert.That(parked.Where(p => p.Record.Op == MutationKind.TxCommit).Select(p => (p.Record.ShardIndex, p.Reason)),
                Is.EquivalentTo(new[] { (0, PoisonedSagaReason), (1, PoisonedSagaReason) }),
                "the held terminal and the later one are both parked");
            Assert.That(stream.Applied.SelectMany(b => b).Select(r => r.Key), Does.Contain("ahead"),
                "unrelated traffic keeps shipping");
            Assert.That(grain.HeldTerminalCountForTesting, Is.Zero);
        });
    }

    [Test]
    public async Task Single_partition_serial_shipper_parks_the_terminal_of_a_saga_whose_prepare_was_dead_lettered()
    {
        // One partition and a window of one take no holds: the terminal is
        // consumed by the merge after the batch carrying keyB's prepare failed.
        var (dlq, parked) = RecordingDeadLetters();
        var (grain, _, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(2),
            deadLetters: dlq);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[0].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 2));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 1, ticks: 3));
        feeds[0].Append(MakeEntry("after", ticks: 4));

        await PumpTicksAsync(grain, 4);

        Assert.Multiple(() =>
        {
            Assert.That(parked.Where(p => p.Record.IsPrepared).Select(p => (p.Record.Key, p.Reason)),
                Is.EqualTo(new[] { ("keyB", LatticeReplicationMetrics.ReasonSchema) }),
                "precondition: exactly keyB's prepare was dead-lettered by the encode failure");
            AssertSagaNeverCommittedOnThePeer(stream, txid);
            Assert.That(parked.Count(p => p.Record.Op == MutationKind.TxCommit && p.Reason == PoisonedSagaReason), Is.EqualTo(1));
            Assert.That(stream.Applied.SelectMany(b => b).Select(r => r.Key), Does.Contain("after"),
                "the stream continues past the parked terminal");
        });
    }

    [Test]
    public async Task Later_prepare_of_a_saga_whose_prepare_was_dead_lettered_is_parked_not_shipped()
    {
        // keyA's prepare is the first batch and fails to encode. keyB's prepare,
        // read afterwards, buys the peer nothing - without a terminal the saga
        // never becomes visible there - so it is parked rather than staged.
        var (dlq, parked) = RecordingDeadLetters();
        var (grain, _, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(1),
            deadLetters: dlq);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[0].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 2));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 1, ticks: 3));

        await PumpTicksAsync(grain, 4);

        Assert.Multiple(() =>
        {
            AssertSagaNeverCommittedOnThePeer(stream, txid);
            Assert.That(stream.Applied.SelectMany(b => b).Where(r => r.TransactionId == txid), Is.Empty,
                "no record of the poisoned saga reaches the peer");
            Assert.That(parked.Select(p => (p.Record.Key, p.Reason)), Does.Contain(("keyB", PoisonedSagaReason)));
        });
    }

    [Test]
    public async Task Late_sweep_terminal_after_the_stamped_terminals_were_parked_is_parked_not_shipped()
    {
        // Both stamped terminals of the poisoned saga are parked. A split's
        // retroactive sweep then appends an unstamped terminal for the same
        // decided saga (#4499). A tally of stamped terminals is no bound on the
        // saga's terminals, so the saga must still be poisoned and the late
        // terminal parked: shipped, it would mark the saga on the peer and drain
        // the bucket of the prepare that did ship.
        var (dlq, parked) = RecordingDeadLetters();
        var registry = Substitute.For<ITxRegistryGrain>();
        registry.GetRecordedStatusAsync(Arg.Any<Guid>()).Returns(Task.FromResult(TxStatus.Committed));
        var (grain, _, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 2, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(2),
            deadLetters: dlq,
            txRegistry: registry);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[1].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 2));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 2, ticks: 3));
        feeds[1].Append(CommitEntry(txid, shardIndex: 1, shardCount: 2, ticks: 4));
        await PumpTicksAsync(grain, 4);
        Assert.That(parked.Count(p => p.Record.Op == MutationKind.TxCommit), Is.EqualTo(2),
            "precondition: both stamped terminals are parked");

        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 0, ticks: 9));
        feeds[0].Append(MakeEntry("after", ticks: 10));
        await PumpTicksAsync(grain, 3);

        Assert.Multiple(() =>
        {
            AssertSagaNeverCommittedOnThePeer(stream, txid);
            Assert.That(parked.Count(p => p.Record.Op == MutationKind.TxCommit && p.Record.AtomicShardCount == 0), Is.EqualTo(1),
                "the late sweep terminal is parked");
            Assert.That(stream.Applied.SelectMany(b => b).Select(r => r.Key), Does.Contain("after"));
        });
    }

    [Test]
    public async Task Unpoisoned_saga_beside_a_poisoned_one_still_commits_on_the_peer()
    {
        // Only the saga whose prepare was dead-lettered is withheld for good; the
        // healthy saga ships once the peer has re-seeded.
        var (dlq, _) = RecordingDeadLetters();
        var (grain, _, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(1),
            deadLetters: dlq);
        var poisoned = Guid.NewGuid();
        var healthy = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", poisoned, index: 0, batchSize: 1, ticks: 1));
        feeds[0].Append(PreparedEntry("keyH", healthy, index: 0, batchSize: 1, ticks: 2));
        feeds[0].Append(CommitEntry(poisoned, shardIndex: 0, shardCount: 1, ticks: 3));
        feeds[0].Append(CommitEntry(healthy, shardIndex: 0, shardCount: 1, ticks: 4));

        await PumpTicksAsync(grain, 4);

        // The poison asks the peer to re-seed (#4620); saga records are withheld
        // until the peer echoes a bootstrap after the marker, then re-ship.
        stream.EchoedBootstrapEpoch = () => long.MaxValue;
        feeds[0].Append(MakeEntry("after-reseed", ticks: 5));
        await PumpTicksAsync(grain, 4);

        AssertSagaNeverCommittedOnThePeer(stream, poisoned);
        AssertEveryTerminalFollowsItsPrepares(stream, new Dictionary<Guid, int> { [healthy] = 1 });
    }
}
