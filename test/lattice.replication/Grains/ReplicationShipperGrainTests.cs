using System.Buffers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit coverage of the per-(tree, peer) outbound shipper grain.
/// Tests bypass <c>StartCoordinatorAsync</c> by constructing the
/// grain with substituted Orleans dependencies and driving the pump
/// loop via <see cref="IReplicationShipperGrain.OnDoorbellAsync"/>
/// (which forwards to the same
/// <see cref="ReplicationShipperGrain.ProcessNextPhaseAsync"/> hook
/// the steady-state phase timer would invoke).
/// </summary>
[TestFixture]
public partial class ReplicationShipperGrainTests
{
    private const string Tree = "shipper-tree";
    private const string Peer = "site-b";
    private const string LocalCluster = "site-a";

    private sealed class TestEncoder : IReplicationBatchEncoder
    {
        public string ContentType => "application/x-test";
        public int CurrentWireVersion => 1;
        public bool ThrowOnEncode { get; set; }
        public Exception EncodeException { get; set; } = new ArgumentException("malformed");
        public int Encodes { get; private set; }

        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            if (ThrowOnEncode)
            {
                throw EncodeException;
            }
            Encodes++;
            writer.Write(new byte[] { 1, 2, 3 });
        }

        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) =>
            throw new NotSupportedException();
    }

    private static ReplogEntry MakeEntry(
        string key,
        string origin = LocalCluster,
        long ticks = 1,
        int counter = 0)
        => new()
        {
            TreeId = Tree,
            Op = ReplogOp.Set,
            Key = key,
            Value = new byte[] { 1 },
            Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = counter },
            OriginClusterId = origin,
        };

    /// <summary>
    /// Builds an options monitor that always returns <paramref name="options"/>.
    /// When <paramref name="options"/> is null, defaults to a fresh
    /// instance with <see cref="LatticeReplicationOptions.ShipCursorWriteInterval"/>=1
    /// so legacy single-batch tests observe the cursor flush on every
    /// ack. The deferred-persist semantics are covered by dedicated
    /// tests in the partial file.
    /// </summary>
    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(
        LatticeReplicationOptions? options = null)
    {
        var resolved = options ?? new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(resolved);
        monitor.Get(Arg.Any<string>()).Returns(resolved);
        return monitor;
    }

    /// <summary>
    /// In-memory <see cref="IReplogShardGrain"/> stand-in. Tests
    /// populate it via <see cref="Append(ReplogEntry)"/> (or the
    /// equivalent legacy <see cref="Entries"/> list); the stub assigns
    /// monotonically-increasing sequence numbers starting at <c>0</c>.
    /// <para>
    /// <see cref="ThrowOnRead"/> simulates a transient WAL read
    /// failure on the next <see cref="ReadAsync"/> call.
    /// <see cref="ReadCalls"/> records how many reads have happened
    /// — used by partition-resume tests to assert the shipper does not
    /// rescan from sequence 0 each tick.
    /// </para>
    /// </summary>
    private sealed class StubReplogShardGrain : IReplogShardGrain
    {
        public List<ReplogEntry> Entries { get; } = new();
        public Exception? ThrowOnRead { get; set; }
        public int ReadCalls { get; private set; }
        public List<long> ReadFromSequences { get; } = new();

        public void Append(ReplogEntry entry) => Entries.Add(entry);

        public Task<long> AppendAsync(ReplogEntry entry, CancellationToken cancellationToken)
        {
            Entries.Add(entry);
            return Task.FromResult((long)(Entries.Count - 1));
        }

        public Task<ReplogShardPage> ReadAsync(long fromSequence, int maxEntries, CancellationToken cancellationToken)
        {
            ReadCalls++;
            ReadFromSequences.Add(fromSequence);
            if (ThrowOnRead is not null)
            {
                var ex = ThrowOnRead;
                throw ex;
            }
            cancellationToken.ThrowIfCancellationRequested();
            if (fromSequence >= Entries.Count)
            {
                return Task.FromResult(ReplogShardPage.Empty(fromSequence));
            }
            var endExclusive = (int)Math.Min(Entries.Count, fromSequence + maxEntries);
            var capacity = endExclusive - (int)fromSequence;
            var entries = new ReplogShardEntry[capacity];
            for (var i = 0; i < capacity; i++)
            {
                var seq = fromSequence + i;
                entries[i] = new ReplogShardEntry
                {
                    Sequence = seq,
                    Entry = Entries[(int)seq],
                };
            }
            return Task.FromResult(new ReplogShardPage
            {
                Entries = entries,
                NextSequence = endExclusive,
            });
        }

        public Task<long> GetNextSequenceAsync(CancellationToken cancellationToken) =>
            Task.FromResult((long)Entries.Count);

        public Task<long> GetEntryCountAsync(CancellationToken cancellationToken) =>
            Task.FromResult((long)Entries.Count);
    }

    /// <summary>
    /// Wires the per-partition stubs into a substitute <see cref="IGrainFactory"/>
    /// so the shipper resolves <see cref="IReplogShardGrain"/> by
    /// <c>{tree}/{partition}</c> and gets back the right stub for that
    /// partition. Single-partition tests populate
    /// <c>partitionedFeeds[0]</c> only.
    /// </summary>
    private static IGrainFactory BuildGrainFactory(
        IGrainFactory? caller,
        StubReplogShardGrain[] partitionedFeeds,
        string treeName)
    {
        var factory = caller ?? Substitute.For<IGrainFactory>();
        for (var p = 0; p < partitionedFeeds.Length; p++)
        {
            factory.GetGrain<IReplogShardGrain>($"{treeName}/{p}").Returns(partitionedFeeds[p]);
        }
        return factory;
    }

    private static (
        ReplicationShipperGrain Grain,
        FakePersistentState<ReplicationShipperState> State,
        StubReplogShardGrain Feed,
        IReplicationTransport Transport,
        TestEncoder Encoder,
        ILatticeReplicationCursorRegistry Registry,
        LatticeReplicationOptions Options) Create(
            LatticeReplicationOptions? options = null,
            ReplicationShipperState? seedState = null,
            string treeName = Tree,
            string peerClusterId = Peer,
            IGrainFactory? grainFactory = null)
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{treeName}/{peerClusterId}"));
        var reminders = Substitute.For<IReminderRegistry>();
        var monitor = Monitor(options);
        var feed = new StubReplogShardGrain();
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
        var encoder = new TestEncoder();
        var registry = Substitute.For<ILatticeReplicationCursorRegistry>();
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        if (seedState is not null)
        {
            fakeState.State = seedState;
        }
        var factory = BuildGrainFactory(grainFactory, new[] { feed }, treeName);
        var grain = new ReplicationShipperGrain(
            ctx, reminders, NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, encoder, registry, factory, fakeState,
            new ReplicationPeerStats());
        grain.InitializeForTesting(treeName, peerClusterId);
        return (grain, fakeState, feed, transport, encoder, registry, monitor.CurrentValue);
    }

    // --- Constructor null guards ---

    private static ReplicationShipperGrain ConstructWith(
        IGrainContext? ctx = null,
        IReminderRegistry? reminders = null,
        IOptionsMonitor<LatticeReplicationOptions>? monitor = null,
        IReplicationTransport? transport = null,
        IReplicationBatchEncoder? encoder = null,
        ILatticeReplicationCursorRegistry? registry = null,
        IGrainFactory? grainFactory = null,
        IPersistentState<ReplicationShipperState>? state = null,
        ReplicationPeerStats? peerStats = null)
        => new(
            ctx ?? Substitute.For<IGrainContext>(),
            reminders ?? Substitute.For<IReminderRegistry>(),
            NullLogger<ReplicationShipperGrain>.Instance,
            monitor ?? Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
            transport ?? Substitute.For<IReplicationTransport>(),
            encoder ?? Substitute.For<IReplicationBatchEncoder>(),
            registry ?? Substitute.For<ILatticeReplicationCursorRegistry>(),
            grainFactory ?? Substitute.For<IGrainFactory>(),
            state ?? new FakePersistentState<ReplicationShipperState>(),
            peerStats ?? new ReplicationPeerStats());

    [Test]
    public void Constructor_throws_when_options_monitor_is_null() =>
        Assert.That(() => ConstructWith(monitor: null!),
            Throws.Nothing);  // sanity — substitute fallback works
    // Note: ConstructWith accepts a null monitor by handing in a substitute fallback.
    // Dedicated null-arg tests below pass null! directly bypassing the fallback.

    [Test]
    public void Constructor_throws_when_options_monitor_is_null_explicit()
    {
        Assert.That(
            () => new ReplicationShipperGrain(
                Substitute.For<IGrainContext>(),
                Substitute.For<IReminderRegistry>(),
                NullLogger<ReplicationShipperGrain>.Instance,
                null!,
                Substitute.For<IReplicationTransport>(),
                Substitute.For<IReplicationBatchEncoder>(),
                Substitute.For<ILatticeReplicationCursorRegistry>(),
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationShipperState>(),
                new ReplicationPeerStats()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_transport_is_null()
    {
        Assert.That(
            () => new ReplicationShipperGrain(
                Substitute.For<IGrainContext>(),
                Substitute.For<IReminderRegistry>(),
                NullLogger<ReplicationShipperGrain>.Instance,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                null!,
                Substitute.For<IReplicationBatchEncoder>(),
                Substitute.For<ILatticeReplicationCursorRegistry>(),
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationShipperState>(),
                new ReplicationPeerStats()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_encoder_is_null()
    {
        Assert.That(
            () => new ReplicationShipperGrain(
                Substitute.For<IGrainContext>(),
                Substitute.For<IReminderRegistry>(),
                NullLogger<ReplicationShipperGrain>.Instance,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                Substitute.For<IReplicationTransport>(),
                null!,
                Substitute.For<ILatticeReplicationCursorRegistry>(),
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationShipperState>(),
                new ReplicationPeerStats()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_cursor_registry_is_null()
    {
        Assert.That(
            () => new ReplicationShipperGrain(
                Substitute.For<IGrainContext>(),
                Substitute.For<IReminderRegistry>(),
                NullLogger<ReplicationShipperGrain>.Instance,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                Substitute.For<IReplicationTransport>(),
                Substitute.For<IReplicationBatchEncoder>(),
                null!,
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationShipperState>(),
                new ReplicationPeerStats()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_grain_factory_is_null()
    {
        Assert.That(
            () => new ReplicationShipperGrain(
                Substitute.For<IGrainContext>(),
                Substitute.For<IReminderRegistry>(),
                NullLogger<ReplicationShipperGrain>.Instance,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                Substitute.For<IReplicationTransport>(),
                Substitute.For<IReplicationBatchEncoder>(),
                Substitute.For<ILatticeReplicationCursorRegistry>(),
                null!,
                new FakePersistentState<ReplicationShipperState>(),
                new ReplicationPeerStats()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    // --- InitializeForTesting parameter validation ---

    [TestCase("")]
    [TestCase(null)]
    public void InitializeForTesting_throws_for_invalid_tree_name(string? tree)
    {
        var grain = ConstructWith();
        Assert.That(
            () => grain.InitializeForTesting(tree!, Peer),
            Throws.InstanceOf<ArgumentException>());
    }

    [TestCase("")]
    [TestCase(null)]
    public void InitializeForTesting_throws_for_invalid_peer(string? peer)
    {
        var grain = ConstructWith();
        Assert.That(
            () => grain.InitializeForTesting(Tree, peer!),
            Throws.InstanceOf<ArgumentException>());
    }

    // --- OnDoorbellAsync ---

    [Test]
    public void OnDoorbellAsync_observes_pre_cancelled_token()
    {
        var (grain, _, _, _, _, _, _) = Create();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await grain.OnDoorbellAsync(cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task OnDoorbellAsync_no_op_when_change_feed_empty()
    {
        var (grain, state, _, transport, _, _, _) = Create();

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.WriteCount, Is.EqualTo(0));
        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    // --- Cycle-break: never ship a peer its own writes back ---

    [Test]
    public async Task PumpOnceAsync_skips_entries_originating_from_peer()
    {
        var (grain, state, feed, transport, _, _, _) = Create();
        feed.Append(MakeEntry("k1", origin: Peer, ticks: 10));

        await grain.OnDoorbellAsync(CancellationToken.None);

        // Peer-origin entries are filtered before the encode/send;
        // the batch is empty so nothing is sent.
        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero));
    }

    // --- Filter: KeyFilter ---

    [Test]
    public async Task PumpOnceAsync_drops_entries_failing_key_filter()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            KeyFilter = key => key.StartsWith("repl/"),
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeEntry("other/x", ticks: 5));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PumpOnceAsync_keeps_entries_passing_key_filter()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            KeyFilter = key => key.StartsWith("repl/"),
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        var hlc = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc });
        feed.Append(MakeEntry("repl/x", ticks: 5));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.Received(1).SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    // --- Filter: KeyPrefixes ---

    [Test]
    public async Task PumpOnceAsync_drops_entries_outside_key_prefixes()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            KeyPrefixes = new[] { "repl/", "ops/" },
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeEntry("other/x", ticks: 5));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PumpOnceAsync_keeps_entries_matching_at_least_one_prefix()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            KeyPrefixes = new[] { "repl/", "ops/" },
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        var hlc = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = hlc });
        feed.Append(MakeEntry("ops/x", ticks: 5));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.Received(1).SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    // --- Cursor advance / persistence ---

    [Test]
    public async Task PumpOnceAsync_advances_cursor_to_ack_high_water_mark()
    {
        var (grain, state, feed, transport, _, registry, _) = Create();
        var ackHlc = new HybridLogicalClock { WallClockTicks = 10, Counter = 0 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = ackHlc });
        feed.Append(MakeEntry("k", ticks: 10));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.State.Cursor, Is.EqualTo(ackHlc));
        Assert.That(state.WriteCount, Is.EqualTo(1));
        await registry.Received(1).ReportCursorAsync(
            Tree, Peer, ackHlc, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PumpOnceAsync_resets_consecutive_failures_on_success()
    {
        var (grain, state, feed, transport, _, _, _) = Create(
            seedState: new ReplicationShipperState
            {
                Cursor = HybridLogicalClock.Zero,
                ConsecutiveFailures = 3,
            });
        var ackHlc = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = ackHlc });
        feed.Append(MakeEntry("k", ticks: 5));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.State.ConsecutiveFailures, Is.EqualTo(0));
    }

    [Test]
    public async Task PumpOnceAsync_advances_cursor_to_source_hlc_when_ack_lower()
    {
        // Receiver returned a frontier below the batch's last entry —
        // the shipper still advances to the last-shipped HLC so it
        // does not re-ship the same batch on the next tick.
        var (grain, state, feed, transport, _, _, _) = Create();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
        feed.Append(MakeEntry("k", ticks: 7));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.State.Cursor, Is.EqualTo(new HybridLogicalClock { WallClockTicks = 7, Counter = 0 }));
    }

    [Test]
    public async Task PumpOnceAsync_does_not_advance_cursor_on_negative_ack()
    {
        var (grain, state, feed, transport, _, _, _) = Create();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = false, HighestAppliedHlc = HybridLogicalClock.Zero });
        feed.Append(MakeEntry("k", ticks: 7));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero));
        Assert.That(state.State.ConsecutiveFailures, Is.GreaterThan(0));
    }

    // --- Backoff on transport failure ---

    [Test]
    public async Task PumpOnceAsync_increments_consecutive_failures_on_transport_throw()
    {
        var (grain, state, feed, transport, _, _, _) = Create();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns<ReplicationAck>(_ => throw new InvalidOperationException("transport-down"));
        feed.Append(MakeEntry("k", ticks: 7));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.State.ConsecutiveFailures, Is.EqualTo(1));
        Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task PumpOnceAsync_skips_send_while_backoff_window_in_effect()
    {
        var (grain, _, feed, transport, _, _, _) = Create();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns<ReplicationAck>(_ => throw new InvalidOperationException("down"));
        feed.Append(MakeEntry("k", ticks: 7));

        // First call applies backoff.
        await grain.OnDoorbellAsync(CancellationToken.None);
        // Second call within the backoff window must not invoke the transport again.
        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.Received(1).SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    // --- Schema-shaped encode failure ---

    [Test]
    public async Task PumpOnceAsync_advances_cursor_past_batch_on_argument_exception_during_encode()
    {
        var (grain, state, feed, transport, encoder, _, _) = Create();
        encoder.ThrowOnEncode = true;
        encoder.EncodeException = new ArgumentException("malformed");
        feed.Append(MakeEntry("k", ticks: 7));

        await grain.OnDoorbellAsync(CancellationToken.None);

        // Schema-shaped failure: the cursor advances past the bad
        // batch so the stream makes progress.
        Assert.That(state.State.Cursor, Is.EqualTo(new HybridLogicalClock { WallClockTicks = 7, Counter = 0 }));
        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PumpOnceAsync_advances_cursor_past_batch_on_invalid_operation_during_encode()
    {
        var (grain, state, feed, _, encoder, _, _) = Create();
        encoder.ThrowOnEncode = true;
        encoder.EncodeException = new InvalidOperationException("schema-broken");
        feed.Append(MakeEntry("k", ticks: 9));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.State.Cursor, Is.EqualTo(new HybridLogicalClock { WallClockTicks = 9, Counter = 0 }));
    }

    // --- Drain failure ---

    [Test]
    public async Task PumpOnceAsync_applies_backoff_on_partition_read_throw()
    {
        var (grain, state, feed, transport, _, _, _) = Create();
        feed.ThrowOnRead = new InvalidOperationException("feed-down");

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.State.ConsecutiveFailures, Is.EqualTo(1));
        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    // --- Registry report failure does not unwind cursor advance ---

    [Test]
    public async Task PumpOnceAsync_persists_cursor_even_when_registry_report_throws()
    {
        var (grain, state, feed, transport, _, registry, _) = Create();
        var ackHlc = new HybridLogicalClock { WallClockTicks = 5, Counter = 0 };
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = ackHlc });
        registry.ReportCursorAsync(
            Arg.Any<string>(), Arg.Any<string>(),
            Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .Returns<Task>(_ => Task.FromException(new InvalidOperationException("registry-down")));
        feed.Append(MakeEntry("k", ticks: 5));

        // The cursor advance is the durable side-effect; the registry
        // report is best-effort and a failure must be logged but not
        // propagated.
        Assert.That(
            async () => await grain.OnDoorbellAsync(CancellationToken.None),
            Throws.Nothing);

        Assert.That(state.State.Cursor, Is.EqualTo(ackHlc));
    }

    // --- Batch sizing ---

    [Test]
    public async Task PumpOnceAsync_caps_batch_at_ship_batch_size()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 2,
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        for (var i = 0; i < 10; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i + 1));
        }
        var captured = new List<ReplicationBatch>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captured.Add(call.Arg<ReplicationBatch>());
                return new ReplicationAck
                {
                    Accepted = true,
                    HighestAppliedHlc = new HybridLogicalClock { WallClockTicks = 2, Counter = 0 },
                };
            });

        await grain.OnDoorbellAsync(CancellationToken.None);

        // Single pump tick ships at most ShipBatchSize entries; the
        // remaining entries wait for the next pump.
        Assert.That(captured, Has.Count.EqualTo(1));
    }

    // --- Routing metadata correctness on the batch ---

    [Test]
    public async Task PumpOnceAsync_attaches_routing_metadata_to_batch()
    {
        var (grain, _, feed, transport, _, _, _) = Create();
        ReplicationBatch? captured = null;
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captured = call.Arg<ReplicationBatch>();
                return new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero };
            });
        feed.Append(MakeEntry("k", ticks: 3));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(captured, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(captured!.Value.TargetClusterId, Is.EqualTo(Peer));
            Assert.That(captured.Value.TreeName, Is.EqualTo(Tree));
            Assert.That(captured.Value.OriginClusterId, Is.EqualTo(LocalCluster));
            Assert.That(captured.Value.Payload.IsEmpty, Is.False);
        });
    }

    // --- DLQ routing on schema-shaped encode failure (R-067 §365) ---

    [Test]
    public async Task PumpOnceAsync_routes_every_entry_to_dlq_on_argument_exception_during_encode()
    {
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationDeadLetterGrain>(Tree).Returns(dlq);
        var (grain, state, feed, _, encoder, _, _) = Create(grainFactory: factory);
        encoder.ThrowOnEncode = true;
        encoder.EncodeException = new ArgumentException("malformed");
        feed.Append(MakeEntry("k1", ticks: 5));
        feed.Append(MakeEntry("k2", ticks: 6));
        feed.Append(MakeEntry("k3", ticks: 7));

        await grain.OnDoorbellAsync(CancellationToken.None);

        // Each entry in the failed batch is parked individually, tagged ReasonSchema.
        await dlq.Received(3).EnqueueAsync(
            Arg.Any<ReplogEntry>(),
            "malformed",
            0,
            LatticeReplicationMetrics.ReasonSchema,
            Arg.Any<CancellationToken>());
        // Cursor still advances past the batch so the stream makes progress.
        Assert.That(state.State.Cursor, Is.EqualTo(new HybridLogicalClock { WallClockTicks = 7, Counter = 0 }));
    }

    [Test]
    public async Task PumpOnceAsync_routes_to_dlq_on_invalid_operation_during_encode()
    {
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationDeadLetterGrain>(Tree).Returns(dlq);
        var (grain, state, feed, _, encoder, _, _) = Create(grainFactory: factory);
        encoder.ThrowOnEncode = true;
        encoder.EncodeException = new InvalidOperationException("schema-broken");
        feed.Append(MakeEntry("k", ticks: 9));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await dlq.Received(1).EnqueueAsync(
            Arg.Any<ReplogEntry>(),
            "schema-broken",
            0,
            LatticeReplicationMetrics.ReasonSchema,
            Arg.Any<CancellationToken>());
        Assert.That(state.State.Cursor, Is.EqualTo(new HybridLogicalClock { WallClockTicks = 9, Counter = 0 }));
    }

    [Test]
    public async Task PumpOnceAsync_advances_cursor_even_when_dlq_enqueue_throws()
    {
        // A deterministically-failing DLQ must not pin the ship loop.
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        dlq.EnqueueAsync(
            Arg.Any<ReplogEntry>(),
            Arg.Any<string>(),
            Arg.Any<int>(),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("dlq-down"));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationDeadLetterGrain>(Tree).Returns(dlq);
        var (grain, state, feed, _, encoder, _, _) = Create(grainFactory: factory);
        encoder.ThrowOnEncode = true;
        feed.Append(MakeEntry("k", ticks: 11));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.State.Cursor, Is.EqualTo(new HybridLogicalClock { WallClockTicks = 11, Counter = 0 }));
    }

    // --- Activation-scoped buffer reuse (R-067 perf-pass) ---

    private sealed class CapturingEncoder : IReplicationBatchEncoder
    {
        public string ContentType => "application/x-test";
        public int CurrentWireVersion => 1;
        public List<IReadOnlyList<ReplogEntry>> CapturedEntryLists { get; } = new();
        public List<IBufferWriter<byte>> CapturedWriters { get; } = new();
        public List<int> WrittenCountBeforeEncode { get; } = new();
        public int BytesPerEncode { get; set; } = 4;

        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)
        {
            CapturedEntryLists.Add(envelope.Entries);
            CapturedWriters.Add(writer);
            // ArrayBufferWriter exposes WrittenCount; capture before writing
            // so the test can assert ResetWrittenCount() rewound to 0.
            if (writer is ArrayBufferWriter<byte> abw)
            {
                WrittenCountBeforeEncode.Add(abw.WrittenCount);
            }
            writer.Write(new byte[BytesPerEncode]);
        }

        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) =>
            throw new NotSupportedException();
    }

    private static (
        ReplicationShipperGrain Grain,
        FakePersistentState<ReplicationShipperState> State,
        StubReplogShardGrain Feed,
        IReplicationTransport Transport,
        CapturingEncoder Encoder) CreateWithCapturingEncoder(
            LatticeReplicationOptions? options = null)
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var monitor = Monitor(options);
        var feed = new StubReplogShardGrain();
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
        var encoder = new CapturingEncoder();
        var registry = Substitute.For<ILatticeReplicationCursorRegistry>();
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        var factory = BuildGrainFactory(null, new[] { feed }, Tree);
        var grain = new ReplicationShipperGrain(
            ctx,
            Substitute.For<IReminderRegistry>(),
            NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, encoder, registry,
            factory,
            fakeState,
            new ReplicationPeerStats());
        grain.InitializeForTesting(Tree, Peer);
        return (grain, fakeState, feed, transport, encoder);
    }

    [Test]
    public async Task PumpOnceAsync_reuses_drain_buffer_across_pump_ticks()
    {
        var (grain, _, feed, _, encoder) = CreateWithCapturingEncoder();
        feed.Append(MakeEntry("k1", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);
        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);

        // Both ticks encoded once; the entries-list reference must be
        // the same activation-scoped _drainBuffer instance both times.
        Assert.That(encoder.CapturedEntryLists, Has.Count.EqualTo(2));
        Assert.That(
            encoder.CapturedEntryLists[1],
            Is.SameAs(encoder.CapturedEntryLists[0]),
            "The drain buffer should be reused across pump ticks (no per-tick allocation).");
    }

    [Test]
    public async Task PumpOnceAsync_reuses_write_buffer_and_resets_written_count_across_pump_ticks()
    {
        var (grain, _, feed, _, encoder) = CreateWithCapturingEncoder();
        feed.Append(MakeEntry("k1", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);
        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(encoder.CapturedWriters, Has.Count.EqualTo(2));
            Assert.That(
                encoder.CapturedWriters[1],
                Is.SameAs(encoder.CapturedWriters[0]),
                "The framing buffer should be reused across pump ticks.");
            // ResetWrittenCount() rewinds the write index to 0 between
            // ticks so each Encode starts at offset 0 even though the
            // underlying array survives.
            Assert.That(encoder.WrittenCountBeforeEncode, Is.EqualTo(new[] { 0, 0 }));
        });
    }

    [Test]
    public async Task PumpOnceAsync_recreates_write_buffer_after_capacity_hits_large_threshold()
    {
        // Force a large encode (>= 4 MB threshold) on tick 1 so the
        // shipper recreates the buffer on tick 2 rather than reusing.
        var (grain, _, feed, _, encoder) = CreateWithCapturingEncoder();
        encoder.BytesPerEncode = 4 * 1024 * 1024 + 1; // pushes Capacity past LargeWriteBufferThreshold
        feed.Append(MakeEntry("k1", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);
        encoder.BytesPerEncode = 4;
        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(encoder.CapturedWriters, Has.Count.EqualTo(2));
        Assert.That(
            encoder.CapturedWriters[1],
            Is.Not.SameAs(encoder.CapturedWriters[0]),
            "A buffer that grew at-or-past the soft cap must be recreated on the next pump tick rather than pinning the large array forever.");
    }

    // --- Peer stats recording ---

    /// <summary>
    /// Variant of <see cref="Create"/> that exposes the
    /// <see cref="ReplicationPeerStats"/> instance handed to the grain
    /// so tests can assert against <see cref="ReplicationPeerStats.Snapshot"/>
    /// after driving the pump tick. Mirrors <see cref="Create"/> but
    /// returns a smaller tuple containing only the dependencies the
    /// peer-stats tests actually need.
    /// </summary>
    private static (
        ReplicationShipperGrain Grain,
        StubReplogShardGrain Feed,
        IReplicationTransport Transport,
        ReplicationPeerStats Stats) CreateWithStats(
            LatticeReplicationOptions? options = null)
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var reminders = Substitute.For<IReminderRegistry>();
        var monitor = Monitor(options);
        var feed = new StubReplogShardGrain();
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
        var encoder = new TestEncoder();
        var registry = Substitute.For<ILatticeReplicationCursorRegistry>();
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        var factory = BuildGrainFactory(null, new[] { feed }, Tree);
        var stats = new ReplicationPeerStats();
        var grain = new ReplicationShipperGrain(
            ctx, reminders, NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, encoder, registry, factory, fakeState, stats);
        grain.InitializeForTesting(Tree, Peer);
        return (grain, feed, transport, stats);
    }

    [Test]
    public async Task PumpOnceAsync_records_peer_success_after_round_trip()
    {
        var (grain, feed, _, stats) = CreateWithStats();
        feed.Append(MakeEntry("k1", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        var snapshot = stats.Snapshot();
        Assert.That(snapshot, Has.Count.EqualTo(1));
        var snap = snapshot.Single();
        Assert.Multiple(() =>
        {
            Assert.That(snap.Tree, Is.EqualTo(Tree));
            Assert.That(snap.Peer, Is.EqualTo(Peer));
            Assert.That(snap.ConsecutiveErrors, Is.EqualTo(0));
            // LastContactSeconds is NaN until the first successful contact;
            // after a successful round-trip it must be a real, non-negative
            // value.
            Assert.That(snap.LastContactSeconds, Is.Not.NaN);
            Assert.That(snap.LastContactSeconds, Is.GreaterThanOrEqualTo(0.0));
        });
    }

    [Test]
    public async Task PumpOnceAsync_records_peer_error_on_transport_throw()
    {
        var (grain, feed, transport, stats) = CreateWithStats();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns<ReplicationAck>(_ => throw new InvalidOperationException("transport-down"));
        feed.Append(MakeEntry("k", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        var snap = stats.Snapshot().Single();
        Assert.That(snap.ConsecutiveErrors, Is.EqualTo(1));
        // A transport throw never reaches RecordSuccess, so the
        // last-contact timestamp must remain unset (NaN sentinel).
        Assert.That(snap.LastContactSeconds, Is.NaN);
    }

    [Test]
    public async Task PumpOnceAsync_records_peer_error_on_ack_rejected()
    {
        var (grain, feed, transport, stats) = CreateWithStats();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = false, HighestAppliedHlc = HybridLogicalClock.Zero });
        feed.Append(MakeEntry("k", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        var snap = stats.Snapshot().Single();
        Assert.That(snap.ConsecutiveErrors, Is.EqualTo(1));
        Assert.That(snap.LastContactSeconds, Is.NaN);
    }

    [Test]
    public async Task PumpOnceAsync_does_not_record_peer_error_on_drain_throw()
    {
        // Drain failures (local WAL read errors) must NOT bump the
        // per-peer consecutive_errors gauge — the peer is fine; the
        // local source is down. The shipper still increments its own
        // ConsecutiveFailures backoff counter, but the peer-stats
        // surface stays clean.
        var (grain, feed, _, stats) = CreateWithStats();
        feed.ThrowOnRead = new InvalidOperationException("feed-down");

        await grain.OnDoorbellAsync(CancellationToken.None);

        // No RecordError, no RecordSuccess: the peer entry is never
        // touched, so Snapshot() returns an empty collection.
        Assert.That(stats.Snapshot(), Is.Empty);
    }

    [Test]
    public async Task PumpOnceAsync_records_zero_backlog_when_batch_below_cap()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 64,
        };
        var (grain, feed, _, stats) = CreateWithStats(opts);
        // Single entry — the drain returns 1 < ShipBatchSize so
        // hitBatchCap is false and the recorded backlog is zero.
        feed.Append(MakeEntry("k", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        var snap = stats.Snapshot().Single();
        Assert.Multiple(() =>
        {
            Assert.That(snap.EntriesBehind, Is.EqualTo(0L));
            Assert.That(snap.BytesBehind, Is.EqualTo(0L));
        });
    }

    [Test]
    public async Task PumpOnceAsync_records_backlog_lower_bound_when_batch_capped()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 2,
        };
        var (grain, feed, _, stats) = CreateWithStats(opts);
        // Three entries forces the drain to fill the buffer to the
        // ShipBatchSize cap — _drainBuffer.Count >= maxPerBatch is
        // the lower-bound signal that the WAL has at least one full
        // batch's worth of entries past the cursor.
        feed.Append(MakeEntry("k1", ticks: 1));
        feed.Append(MakeEntry("k2", ticks: 2));
        feed.Append(MakeEntry("k3", ticks: 3));

        await grain.OnDoorbellAsync(CancellationToken.None);

        var snap = stats.Snapshot().Single();
        Assert.Multiple(() =>
        {
            // entries_behind reports the just-shipped count (>=
            // ShipBatchSize) as a floor.
            Assert.That(snap.EntriesBehind, Is.GreaterThanOrEqualTo(opts.ShipBatchSize));
            // bytes_behind is the encoded payload size — TestEncoder
            // writes 3 bytes per encode so bytes_behind is at least 1.
            Assert.That(snap.BytesBehind, Is.GreaterThan(0L));
        });
    }
}
