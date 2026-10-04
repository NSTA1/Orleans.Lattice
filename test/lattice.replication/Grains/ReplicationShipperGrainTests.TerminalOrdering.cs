using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Saga terminal ordering at the shipper boundary (issue #4480): a replicated
/// TxCommit / TxAbort must not reach the peer before the peer has applied every
/// prepare of its saga. Each test drives the real shipper over a multi-partition
/// WAL and checks the stream the peer applied - the batches the transport
/// accepted, in the order it accepted them - against that rule
/// (<see cref="AssertEveryTerminalFollowsItsPrepares"/>). This is the detector
/// for the cross-cluster model's DeliverTerminal guard.
/// <para>
/// The three routes by which the HLC merge let a terminal overtake a prepare
/// are each pinned: a partition whose leaf clock runs ahead holding a
/// higher-HLC entry in front of the prepare; a partition read empty earlier in
/// the tick that a prepare lands in before the terminal is read; and a
/// pipelined batch carrying the prepare that fails while the later batch
/// carrying the terminal applies.
/// </para>
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private static WalRecord PreparedEntry(string key, Guid txid, int index, int batchSize, long ticks) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = new HybridLogicalClock { WallClockTicks = ticks },
        OriginClusterId = LocalCluster,
        TransactionId = txid,
        IsPrepared = true,
        AtomicBatchSize = batchSize,
        AtomicBatchIndex = index,
    };

    private static WalRecord CommitEntry(Guid txid, int shardIndex, int shardCount, long ticks) =>
        MakeTerminalEntry(MutationKind.TxCommit, shardIndex, ticks: ticks, transactionId: txid) with
        {
            AtomicShardCount = shardCount,
        };

    /// <summary>
    /// The batches the peer applied, in apply order. A batch whose send threw or
    /// was not accepted never applied.
    /// </summary>
    private sealed class AppliedStream
    {
        public List<List<WalRecord>> Applied { get; } = new();

        public int Sends { get; set; }

        public Func<int, bool> Fail { get; set; } = _ => false;
    }

    private static AppliedStream RecordAppliedStream(IReplicationTransport transport, StubWalRecordEncoder encoder)
    {
        var stream = new AppliedStream();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var send = ++stream.Sends;
                if (stream.Fail(send))
                {
                    return Task.FromException<ReplicationAck>(new IOException($"send {send} failed"));
                }

                var batch = call.Arg<ReplicationBatch>();
                var segments = batch.EncodedEnvelope!.Value.EncodedEntries.Span;
                var records = new List<WalRecord>(segments.Length);
                for (var i = 0; i < segments.Length; i++)
                {
                    records.Add(encoder.Decode(segments[i].AsSpan()));
                }

                stream.Applied.Add(records);
                return Task.FromResult(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
            });
        return stream;
    }

    private static (
        ReplicationShipperGrain Grain,
        FakePersistentState<ReplicationShipperState> State,
        StubReplogShardGrain[] Feeds,
        AppliedStream Stream) CreateOrderingShipper(
            LatticeReplicationOptions options,
            ReplicationShipperState? seedState = null,
            StubReplogShardGrain[]? feeds = null,
            StubWalRecordEncoder? walEncoder = null)
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        walEncoder ??= new StubWalRecordEncoder();
        feeds ??= Enumerable.Range(0, options.ReplogPartitions)
            .Select(_ => new StubReplogShardGrain(walEncoder))
            .ToArray();
        var transport = Substitute.For<IReplicationTransport>();
        var stream = RecordAppliedStream(transport, walEncoder);
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        if (seedState is not null)
        {
            fakeState.State = seedState;
        }

        var grain = new ReplicationShipperGrain(
            ctx, Substitute.For<IReminderRegistry>(),
            NullLogger<ReplicationShipperGrain>.Instance,
            Monitor(options), transport, new TestEncoder(), walEncoder, Substitute.For<IWalCursorRegistry>(),
            BuildGrainFactory(null, feeds, Tree), fakeState,
            new ReplicationPeerStats(),
            Substitute.For<ILatticeMergeModeResolver>(),
            new WireVersionNegotiationState(), new NoOpReplicationDigestProbeTransport());
        grain.InitializeForTesting(Tree, Peer);
        return (grain, fakeState, feeds, stream);
    }

    private static LatticeReplicationOptions OrderingOptions(int partitions, int batchSize, int window = 1, int pageSize = 64) => new()
    {
        ClusterId = LocalCluster,
        ShipCursorWriteInterval = 1,
        ReplogPartitions = partitions,
        ShipBatchSize = batchSize,
        ShipPartitionPageSize = pageSize,
        ShipMaxInFlight = window,
        WireVersionNegotiationEnabled = false,
        PreShipCoalescingEnabled = false,
    };

    /// <summary>
    /// The peer must have applied every prepare of a saga, in an earlier batch,
    /// before it applies any terminal of that saga.
    /// </summary>
    private static void AssertEveryTerminalFollowsItsPrepares(
        AppliedStream stream,
        IReadOnlyDictionary<Guid, int> prepareCountByTransaction)
    {
        var appliedPrepares = new Dictionary<Guid, HashSet<int>>();
        var terminals = 0;
        for (var b = 0; b < stream.Applied.Count; b++)
        {
            foreach (var record in stream.Applied[b])
            {
                if (record.Op is MutationKind.TxCommit or MutationKind.TxAbort)
                {
                    terminals++;
                    appliedPrepares.TryGetValue(record.TransactionId, out var seen);
                    Assert.That(seen?.Count ?? 0, Is.EqualTo(prepareCountByTransaction[record.TransactionId]),
                        $"batch {b} applies the {record.Op} of saga {record.TransactionId} before the peer applied every one of its prepares");
                }
            }

            foreach (var record in stream.Applied[b])
            {
                if (record.IsPrepared)
                {
                    if (!appliedPrepares.TryGetValue(record.TransactionId, out var seen))
                    {
                        appliedPrepares[record.TransactionId] = seen = new HashSet<int>();
                    }

                    seen.Add(record.AtomicBatchIndex);
                }
            }
        }

        Assert.That(terminals, Is.GreaterThan(0), "the test is vacuous: no terminal was applied");
    }

    [TestCase(2, TestName = "Terminal_does_not_overtake_a_prepare_queued_behind_a_higher_hlc_entry")]
    [TestCase(0, TestName = "Unstamped_split_sweep_terminal_does_not_overtake_a_prepare_queued_behind_a_higher_hlc_entry")]
    public async Task Terminal_does_not_overtake_a_prepare_queued_behind_a_higher_hlc_entry(int shardCount)
    {
        // Leaf clocks are independent: partition 0's first entry was stamped by
        // a leaf whose clock runs ahead, so it sorts after the saga's terminals
        // although it was appended before the prepare behind it. The merge by
        // HLC therefore reaches both terminals before keyB's prepare. A shard
        // count of 0 is the split coordinator's retroactive-sweep terminal: the
        // hold never reads the count, it waits for every prepare of the saga.
        var (grain, _, feeds, stream) = CreateOrderingShipper(OrderingOptions(partitions: 2, batchSize: 16));
        var txid = Guid.NewGuid();
        feeds[0].Append(MakeEntry("unrelated", ticks: 100));
        feeds[1].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[0].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 2));
        feeds[1].Append(CommitEntry(txid, shardIndex: 0, shardCount: shardCount, ticks: 3));
        feeds[1].Append(CommitEntry(txid, shardIndex: 1, shardCount: shardCount, ticks: 4));

        await grain.PumpForTestingAsync(CancellationToken.None);

        AssertEveryTerminalFollowsItsPrepares(stream, new Dictionary<Guid, int> { [txid] = 2 });
        Assert.That(stream.Applied.SelectMany(b => b).Count(r => r.Op == MutationKind.TxCommit), Is.EqualTo(2),
            "both terminals ship within the tick once their prepares are acknowledged");
        Assert.That(grain.HeldTerminalCountForTesting, Is.Zero);
    }

    [Test]
    public async Task Terminal_does_not_overtake_a_prepare_landing_in_a_partition_read_empty_earlier_in_the_tick()
    {
        // Partition 0 is read empty when the tick primes and is not read again
        // that tick. Before partition 1's refill, the saga prepares keyB (in
        // partition 0), decides, and appends its terminal to partition 1, so
        // the refill returns the terminal while the prepare stays unread.
        var (grain, _, feeds, stream) = CreateOrderingShipper(OrderingOptions(partitions: 2, batchSize: 1, pageSize: 1));
        var txid = Guid.NewGuid();
        feeds[1].Append(MakeEntry("warm", ticks: 1));
        var injected = false;
        feeds[1].OnReadShipping = _ =>
        {
            if (!injected && feeds[1].ReadCalls == 2)
            {
                injected = true;
                feeds[0].Append(PreparedEntry("keyB", txid, index: 0, batchSize: 1, ticks: 5));
                feeds[1].Append(CommitEntry(txid, shardIndex: 1, shardCount: 1, ticks: 6));
            }

            return Task.CompletedTask;
        };

        await grain.PumpForTestingAsync(CancellationToken.None);
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(injected, Is.True, "the scenario never injected the saga");
        AssertEveryTerminalFollowsItsPrepares(stream, new Dictionary<Guid, int> { [txid] = 1 });
    }

    [Test]
    public async Task Terminal_is_not_applied_ahead_of_a_failed_pipelined_batch_that_carried_its_prepare()
    {
        // Window of two: the batch carrying the prepare and the batch carrying
        // the terminal are in flight together. The first fails, the second
        // applies - the terminal reached the peer and the prepare did not.
        var (grain, _, feeds, stream) = CreateOrderingShipper(OrderingOptions(partitions: 1, batchSize: 1, window: 2));
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 1, ticks: 1));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 1, ticks: 2));
        stream.Fail = send => send == 1;

        await grain.PumpForTestingAsync(CancellationToken.None);
        grain.ResetBackoffForTesting();
        await grain.PumpForTestingAsync(CancellationToken.None);

        AssertEveryTerminalFollowsItsPrepares(stream, new Dictionary<Guid, int> { [txid] = 1 });
    }

    [Test]
    public async Task Terminal_with_no_prepare_in_the_log_ships_within_the_tick_through_the_tail_barrier()
    {
        // A saga that aborted before any prepare: nothing to tally, so the hold
        // releases on the tail barrier once every partition is acknowledged.
        var (grain, _, feeds, stream) = CreateOrderingShipper(OrderingOptions(partitions: 2, batchSize: 16));
        var txid = Guid.NewGuid();
        feeds[0].Append(MakeEntry("k0", ticks: 1));
        feeds[1].Append(MakeTerminalEntry(MutationKind.TxAbort, shardIndex: 1, ticks: 2, transactionId: txid));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(stream.Applied.SelectMany(b => b).Count(r => r.Op == MutationKind.TxAbort), Is.EqualTo(1));
            Assert.That(grain.HeldTerminalCountForTesting, Is.Zero);
        });
    }

    [Test]
    public async Task Held_terminal_caps_its_durable_cursor_and_a_new_activation_ships_it_after_its_prepare()
    {
        // keyA's prepare is acknowledged, then the terminal is consumed while
        // keyB's prepare is still unread (the refill route). The durable
        // cursor must stop at the held terminal, and the reported HLC cursor
        // below it, so a new activation re-reads it. That activation never
        // re-reads keyA's acknowledged prepare, so its tally cannot complete
        // and the release must come from the tail barrier - still after keyB.
        var options = OrderingOptions(partitions: 2, batchSize: 1, pageSize: 1);
        var walEncoder = new StubWalRecordEncoder();
        var (grain, state, feeds, before) = CreateOrderingShipper(options, walEncoder: walEncoder);
        var txid = Guid.NewGuid();
        feeds[1].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 2, ticks: 1));
        feeds[1].OnReadShipping = _ =>
        {
            if (feeds[1].ReadCalls == 2 && feeds[0].Entries.Count == 0)
            {
                feeds[0].Append(PreparedEntry("keyB", txid, index: 1, batchSize: 2, ticks: 5));
                feeds[1].Append(CommitEntry(txid, shardIndex: 1, shardCount: 1, ticks: 6));
                feeds[1].Append(MakeEntry("after", ticks: 7));
            }

            return Task.CompletedTask;
        };

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grain.HeldTerminalCountForTesting, Is.EqualTo(1), "the terminal is held while keyB's prepare is unread");
            Assert.That(state.State.PartitionCursors[1], Is.EqualTo(1L),
                "the durable cursor stops at the held terminal (sequence 1), not past the entry shipped after it");
            Assert.That(state.State.Cursor.WallClockTicks, Is.LessThan(6L),
                "the reported HLC cursor stays below the held terminal");
        });

        var (restarted, _, _, after) = CreateOrderingShipper(
            options, seedState: state.State, feeds: feeds, walEncoder: walEncoder);
        await restarted.PumpForTestingAsync(CancellationToken.None);

        var combined = new AppliedStream();
        combined.Applied.AddRange(before.Applied);
        combined.Applied.AddRange(after.Applied);
        AssertEveryTerminalFollowsItsPrepares(combined, new Dictionary<Guid, int> { [txid] = 2 });
        Assert.That(after.Applied.SelectMany(b => b).Count(r => r.Op == MutationKind.TxCommit), Is.EqualTo(1));
    }

    [Test]
    public async Task Rebind_to_a_new_source_log_drops_a_hold_whose_prepare_never_shipped()
    {
        // The terminal is held because keyB's prepare is unread when the
        // source log is replaced. The retired log is no longer read, so the
        // prepare never ships: releasing the terminal would commit the saga on
        // the peer without keyB.
        var options = OrderingOptions(partitions: 2, batchSize: 1, pageSize: 1);
        var walEncoder = new StubWalRecordEncoder();
        var retired = new[] { new StubReplogShardGrain(walEncoder), new StubReplogShardGrain(walEncoder) };
        var (grain, _, feeds, stream) = CreateOrderingShipper(options, feeds: retired, walEncoder: walEncoder);
        var txid = Guid.NewGuid();
        feeds[1].Append(MakeEntry("warm", ticks: 1));
        feeds[1].OnReadShipping = _ =>
        {
            if (feeds[1].ReadCalls == 2 && feeds[0].Entries.Count == 0)
            {
                feeds[0].Append(PreparedEntry("keyB", txid, index: 0, batchSize: 1, ticks: 5));
                feeds[1].Append(CommitEntry(txid, shardIndex: 1, shardCount: 1, ticks: 6));
            }

            return Task.CompletedTask;
        };

        await grain.PumpForTestingAsync(CancellationToken.None);
        Assert.That(grain.HeldTerminalCountForTesting, Is.EqualTo(1), "precondition: the terminal is held");

        await grain.NotifySourceIdentityChangedAsync("phys-new", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(grain.HeldTerminalCountForTesting, Is.Zero, "the hold is dropped with the retired log");
            Assert.That(stream.Applied.SelectMany(b => b).Any(r => r.Op == MutationKind.TxCommit), Is.False,
                "the terminal never reached the peer ahead of its prepare");
        });
    }

    [Test]
    public async Task Single_partition_serial_shipper_takes_no_holds()
    {
        // One partition and a window of one already deliver in append order and
        // apply each batch before the next ships, so the terminal rides in its
        // prepare's batch exactly as before.
        var (grain, _, feeds, stream) = CreateOrderingShipper(OrderingOptions(partitions: 1, batchSize: 16));
        var txid = Guid.NewGuid();
        feeds[0].Append(PreparedEntry("keyA", txid, index: 0, batchSize: 1, ticks: 1));
        feeds[0].Append(CommitEntry(txid, shardIndex: 0, shardCount: 1, ticks: 2));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(stream.Applied, Has.Count.EqualTo(1));
        Assert.That(stream.Applied[0].Select(r => r.Op), Is.EqualTo(new[] { MutationKind.Set, MutationKind.TxCommit }));
    }
}
