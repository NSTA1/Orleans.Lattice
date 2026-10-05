using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// A batch the shipper cannot encode (issue #4614). It used to be parked on this
/// cluster's dead-letter queue with the cursor moved past it, which lost its
/// writes for the peer for good: a replay applies a parked entry here, never on
/// the peer. Now the failure takes the peer off the log and quarantines the
/// batch; the peer re-seeds from an export after the marker, which carries the
/// batch's writes, and the rewind consumes the quarantined positions without
/// shipping them. Each test drives the real shipper over a real merge of its WAL
/// partitions, makes chosen batches fail to encode, and checks the stream the
/// peer applied.
/// </summary>
public partial class ReplicationShipperGrainTests
{
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

    private static (IReplicationDeadLetterGrain Dlq, List<WalRecord> Parked) RecordingDeadLetters()
    {
        var parked = new List<WalRecord>();
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        dlq.EnqueueAsync(
                Arg.Any<WalRecord>(), Arg.Any<string>(), Arg.Any<int>(),
                Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                parked.Add(call.ArgAt<WalRecord>(0));
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

    private static IEnumerable<WalRecord> AppliedRecords(AppliedStream stream, int fromBatch = 0) =>
        stream.Applied.Skip(fromBatch).SelectMany(b => b);

    private static void AssertNoTerminalOf(AppliedStream stream, Guid txid, string because) =>
        Assert.That(
            AppliedRecords(stream).Where(r => r.TransactionId == txid && r.Op is MutationKind.TxCommit or MutationKind.TxAbort),
            Is.Empty,
            because);

    [TestCase(1, TestName = "Encode_failure_takes_the_peer_off_the_log_and_quarantines_the_batch_instead_of_dead_lettering_it")]
    [TestCase(2, TestName = "Pipelined_encode_failure_takes_the_peer_off_the_log_and_quarantines_the_batch_instead_of_dead_lettering_it")]
    public async Task Encode_failure_takes_the_peer_off_the_log_and_quarantines_the_batch(int window)
    {
        var (dlq, parked) = RecordingDeadLetters();
        var (grain, state, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1, window: window),
            modeResolver: ResolverFailingOnCalls(1),
            deadLetters: dlq,
            exportEpoch: 7);
        feeds[0].Append(MakeEntry("unencodable", ticks: 1));
        feeds[0].Append(MakeEntry("after", ticks: 2));

        await PumpTicksAsync(grain, 3);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(7L),
                "the peer must be re-seeded, or it never receives the batch's writes");
            Assert.That(stream.ReseedRequests, Is.Not.Empty.And.All.EqualTo(7L),
                "every push after the failure asks the peer to re-seed");
            Assert.That(state.State.EncodeQuarantineFrom, Is.EquivalentTo(new Dictionary<int, long> { [0] = 0 }));
            Assert.That(state.State.EncodeQuarantineThrough, Is.EquivalentTo(new Dictionary<int, long> { [0] = 0 }));
            Assert.That(state.State.PartitionCursors.GetValueOrDefault(0), Is.EqualTo(2),
                "the cursor moves past the quarantined batch, so the stream does not stall");
            Assert.That(parked, Is.Empty, "nothing is parked on this cluster's dead-letter queue, where a replay is a no-op");
            Assert.That(AppliedRecords(stream).Select(r => r.Key), Is.EqualTo(new[] { "after" }),
                "the unencodable batch is never shipped and plain writes keep shipping");
        });
    }

    [TestCase(1, TestName = "Terminal_of_a_saga_that_lost_a_prepare_to_an_encode_failure_does_not_reach_the_peer_before_the_re_seed")]
    [TestCase(2, TestName = "Pipelined_terminal_of_a_saga_that_lost_a_prepare_to_an_encode_failure_does_not_reach_the_peer_before_the_re_seed")]
    public async Task Terminal_of_a_saga_that_lost_a_prepare_does_not_reach_the_peer_before_the_re_seed(int window)
    {
        // Two partitions, so terminals are held. keyB's prepare is in the second
        // batch, which fails to encode; both terminals are read afterwards.
        var (dlq, parked) = RecordingDeadLetters();
        var (grain, state, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 2, batchSize: 1, window: window),
            modeResolver: ResolverFailingOnCalls(2),
            deadLetters: dlq,
            exportEpoch: 3);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[1].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 2));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 2, ticks: 3));
        feeds[1].Append(CommitEntry(txid, shardIndex: 1, shardCount: 2, ticks: 4));
        feeds[0].Append(MakeEntry("after", ticks: 5));

        await PumpTicksAsync(grain, 4);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(3L), "precondition: the peer was taken off the log");
            AssertNoTerminalOf(stream, txid, "a terminal shipped before the re-seed would commit the saga without keyB (torn batch)");
            Assert.That(AppliedRecords(stream).Select(r => r.Key), Has.No.Member("keyB"));
            Assert.That(AppliedRecords(stream).Select(r => r.Key), Does.Contain("after"), "plain writes keep shipping");
            Assert.That(parked, Is.Empty);
            Assert.That(grain.HeldTerminalCountForTesting, Is.Zero);
        });
    }

    [Test]
    public async Task Terminal_held_before_its_saga_lost_a_prepare_to_an_encode_failure_is_not_released()
    {
        // Partition 1 opens with an entry whose leaf clock runs ahead, so the
        // merge holds shard 0's terminal before it reaches keyB's prepare, whose
        // batch (the third) then fails to encode.
        var (grain, state, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 2, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(3),
            exportEpoch: 4);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 2, ticks: 3));
        feeds[1].Append(MakeEntry("ahead", ticks: 100));
        feeds[1].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 2));
        feeds[1].Append(CommitEntry(txid, shardIndex: 1, shardCount: 2, ticks: 4));

        await PumpTicksAsync(grain, 5);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(4L), "precondition: the peer was taken off the log");
            AssertNoTerminalOf(stream, txid, "the held terminal must not be released without keyB");
            Assert.That(AppliedRecords(stream).Select(r => r.Key), Does.Contain("ahead"), "unrelated traffic keeps shipping");
            Assert.That(grain.HeldTerminalCountForTesting, Is.Zero, "the holds are dropped; the rewind re-reads them");
        });
    }

    [Test]
    public async Task Later_prepare_of_a_saga_that_lost_a_prepare_is_withheld_until_the_re_seed()
    {
        var (grain, _, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(1),
            exportEpoch: 2);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[0].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 2));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 1, ticks: 3));

        await PumpTicksAsync(grain, 4);

        Assert.That(AppliedRecords(stream).Where(r => r.TransactionId == txid), Is.Empty,
            "no record of the saga reaches the peer while it awaits the re-seed");
    }

    [Test]
    public async Task After_the_re_seed_the_rewind_skips_the_quarantined_batch_and_the_saga_commits()
    {
        // The detector for the model's EventualConvergence: the export the peer
        // re-seeded from carries keyB's prepare, so the re-shipped terminal
        // commits the saga whole, and the unencodable batch is never shipped.
        var (grain, state, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(2),
            exportEpoch: 7);
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[0].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 2));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 1, ticks: 3));
        feeds[0].Append(MakeEntry("before-reseed", ticks: 4));
        await PumpTicksAsync(grain, 4);
        AssertNoTerminalOf(stream, txid, "precondition: the terminal waits for the re-seed");
        Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(7L), "precondition: the peer was taken off the log");

        var reseeded = stream.Applied.Count;
        stream.EchoedBootstrapEpoch = () => 8;
        feeds[0].Append(MakeEntry("after-reseed", ticks: 5));
        await PumpTicksAsync(grain, 4);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ReseedRequiredEpoch, Is.Null, "the echo of a later export clears the marker");
            Assert.That(AppliedRecords(stream).Select(r => r.Key), Has.No.Member("keyB"),
                "the quarantined prepare is never shipped; the export carried it");
            Assert.That(AppliedRecords(stream, reseeded).Count(r => r.TransactionId == txid && r.Op == MutationKind.TxCommit),
                Is.EqualTo(1), "after the re-seed the terminal ships and commits the saga the export staged");
            Assert.That(AppliedRecords(stream).Select(r => r.Key), Does.Contain("after-reseed"));
            Assert.That(state.State.EncodeQuarantineThrough, Is.Empty,
                "the quarantine retires once no re-seed is outstanding and the cursor has passed it");
        });
    }

    [Test]
    public async Task Healthy_saga_beside_one_that_lost_a_prepare_commits_on_the_peer_after_the_re_seed()
    {
        var (grain, _, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(1),
            exportEpoch: 5);
        var broken = Guid.NewGuid();
        var healthy = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", broken, index: 0, batchSize: 1, ticks: 1));
        feeds[0].Append(PreparedEntry("keyH", healthy, index: 0, batchSize: 1, ticks: 2));
        feeds[0].Append(CommitEntry(broken, shardIndex: 0, shardCount: 1, ticks: 3));
        feeds[0].Append(CommitEntry(healthy, shardIndex: 0, shardCount: 1, ticks: 4));
        await PumpTicksAsync(grain, 4);

        stream.EchoedBootstrapEpoch = () => 6;
        feeds[0].Append(MakeEntry("after-reseed", ticks: 5));
        await PumpTicksAsync(grain, 4);

        Assert.Multiple(() =>
        {
            Assert.That(AppliedRecords(stream).Select(r => r.Key), Has.No.Member("keyA"), "the quarantined prepare never ships");
            AssertEveryTerminalFollowsItsPrepares(stream, new Dictionary<Guid, int> { [healthy] = 1, [broken] = 0 });
            Assert.That(AppliedRecords(stream).Count(r => r.TransactionId == healthy && r.Op == MutationKind.TxCommit),
                Is.EqualTo(1), "the healthy saga commits on the peer");
        });
    }

    [Test]
    public async Task A_second_encode_failure_raises_the_marker_so_an_export_opened_past_the_old_epoch_does_not_clear_it()
    {
        // The first failure marks epoch 7. The peer then opens an export at 8,
        // and a second batch fails while it runs: that export predates the second
        // batch's records, so its echo must not clear the marker. Only an export
        // after the second failure (9) does.
        long epoch = 7;
        var (grain, state, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            modeResolver: ResolverFailingOnCalls(1, 2),
            exportEpochOf: () => epoch);
        feeds[0].Append(MakeEntry("first-unencodable", ticks: 1));
        await PumpTicksAsync(grain, 2);
        Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(7L), "precondition: the first failure marks epoch 7");
        var since = state.State.ReseedRequiredSinceUtcTicks;

        epoch = 8;
        feeds[0].Append(MakeEntry("second-unencodable", ticks: 2));
        await PumpTicksAsync(grain, 2);
        Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(8L), "every failure raises the marker to the current export epoch");
        Assert.That(state.State.ReseedRequiredSinceUtcTicks, Is.EqualTo(since), "the re-seed age is kept from the first failure");

        stream.EchoedBootstrapEpoch = () => 8;
        feeds[0].Append(MakeEntry("probe-1", ticks: 3));
        await PumpTicksAsync(grain, 2);
        Assert.Multiple(() =>
        {
            Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(8L),
                "an export that opened before the second failure must not clear the marker");
            Assert.That(state.State.EncodeQuarantineThrough, Is.EquivalentTo(new Dictionary<int, long> { [0] = 1 }),
                "the quarantine spans both failed batches");
        });

        epoch = 9;
        stream.EchoedBootstrapEpoch = () => 9;
        feeds[0].Append(MakeEntry("probe-2", ticks: 4));
        await PumpTicksAsync(grain, 3);
        Assert.Multiple(() =>
        {
            Assert.That(state.State.ReseedRequiredEpoch, Is.Null, "an export after the latest failure clears it");
            Assert.That(AppliedRecords(stream).Select(r => r.Key),
                Has.No.Member("first-unencodable").And.No.Member("second-unencodable"),
                "the rewind skips both quarantined batches");
        });
    }

    [Test]
    public async Task A_reactivation_before_the_cursor_moved_still_skips_the_quarantined_batch()
    {
        // The marker and the quarantine are written before the cursor moves. A
        // crash in between leaves the cursor before the batch; the next
        // activation must consume it without shipping it.
        var seed = new ReplicationShipperState
        {
            ReseedRequiredEpoch = 3,
            EncodeQuarantineFrom = new Dictionary<int, long> { [0] = 0 },
            EncodeQuarantineThrough = new Dictionary<int, long> { [0] = 0 },
        };
        var (grain, state, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            seedState: seed,
            exportEpoch: 3);
        feeds[0].Append(MakeEntry("unencodable", ticks: 1));
        feeds[0].Append(MakeEntry("after", ticks: 2));

        await PumpTicksAsync(grain, 3);

        Assert.Multiple(() =>
        {
            Assert.That(AppliedRecords(stream).Select(r => r.Key), Is.EqualTo(new[] { "after" }));
            Assert.That(state.State.PartitionCursors.GetValueOrDefault(0), Is.EqualTo(2));
            Assert.That(state.State.EncodeQuarantineThrough, Is.Not.Empty, "it stays until the re-seed has completed");
        });
    }

    [Test]
    public async Task A_rebind_to_a_new_source_log_clears_the_quarantine()
    {
        var walEncoder = new StubWalRecordEncoder();
        var rebound = new[] { new StubReplogShardGrain(walEncoder) };
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(Arg.Any<string>()).Returns(Tree);
        var (grain, state, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            walEncoder: walEncoder, reboundFeeds: rebound, registry: registry,
            modeResolver: ResolverFailingOnCalls(1),
            exportEpoch: 2);
        feeds[0].Append(MakeEntry("unencodable", ticks: 1));
        await PumpTicksAsync(grain, 2);
        Assert.That(state.State.EncodeQuarantineThrough, Is.Not.Empty, "precondition: the batch is quarantined");

        registry.ResolveAsync(Arg.Any<string>()).Returns(ReboundPhysical);
        rebound[0].Append(MakeEntry("new-log", ticks: 5));
        await grain.NotifySourceIdentityChangedAsync(ReboundPhysical, CancellationToken.None);
        await PumpTicksAsync(grain, 2);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.EncodeQuarantineFrom, Is.Empty, "the quarantined sequences belong to the retired log");
            Assert.That(state.State.EncodeQuarantineThrough, Is.Empty);
            Assert.That(AppliedRecords(stream).Select(r => r.Key), Does.Contain("new-log"),
                "sequence 0 of the new log is not mistaken for the quarantined batch");
        });
    }

    [Test]
    public async Task A_legacy_poison_list_takes_the_peer_off_the_log_and_is_forgotten()
    {
        var seed = new ReplicationShipperState();
        var poisoned = Guid.NewGuid();
        seed.PoisonedSagas[poisoned] = new PoisonedSaga { TransactionId = poisoned };
        var (grain, state, feeds, stream) = CreateOrderingShipper(
            OrderingOptions(partitions: 1, batchSize: 1),
            seedState: seed,
            exportEpoch: 11);
        feeds[0].Append(CommitEntry(poisoned, shardIndex: 0, shardCount: 1, ticks: 1));
        feeds[0].Append(MakeEntry("after", ticks: 2));

        await PumpTicksAsync(grain, 2);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.PoisonedSagas, Is.Empty);
            Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(11L), "the re-seed delivers each legacy-poisoned saga whole");
            AssertNoTerminalOf(stream, poisoned, "the poisoned saga's terminal waits for the re-seed");
            Assert.That(AppliedRecords(stream).Select(r => r.Key), Does.Contain("after"));
        });
    }
}
