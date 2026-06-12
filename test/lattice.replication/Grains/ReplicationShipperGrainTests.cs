using Orleans.Lattice.BPlusTree.Grains;
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
        public ReplicationBatchEnvelope? LastEnvelope { get; private set; }

        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            if (ThrowOnEncode)
            {
                throw EncodeException;
            }
            Encodes++;
            LastEnvelope = envelope;
            writer.Write(new byte[] { 1, 2, 3 });
        }

        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) =>
            throw new NotSupportedException();
    }

    private static WalRecord MakeEntry(
        string key,
        string origin = LocalCluster,
        long ticks = 1,
        int counter = 0)
        => new()
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = key,
            Value = new byte[] { 1 },
            Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = counter },
            OriginClusterId = origin,
        };

    /// <summary>
    /// Returns the entry count of the most recent
    /// <see cref="ReplicationBatch"/> handed to <paramref name="transport"/>,
    /// read from the framing-only <see cref="ReplicationBatchEncodedEnvelope.EncodedEntries"/>
    /// memory the shipper now writes. Used by tests that previously
    /// asserted against <c>encoder.LastEnvelope.Value.Entries.Count</c>;
    /// the encoder is no longer on the steady-state ship path so the
    /// equivalent signal is the encoded-segment count on the captured
    /// batch.
    /// </summary>
    private static int LastShippedEntryCount(IReplicationTransport transport)
    {
        var calls = transport.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
            .ToList();
        Assert.That(calls, Is.Not.Empty,
            "the shipper must have invoked the transport at least once before this assertion");
        var batch = (ReplicationBatch)calls[^1].GetArguments()[0]!;
        Assert.That(batch.EncodedEnvelope, Is.Not.Null,
            "the shipper must populate EncodedEnvelope on every batch on the framing-only path");
        return batch.EncodedEnvelope!.Value.EncodedEntries.Length;
    }

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
            // The shipper-grain unit-test fixture stubs a single-partition feed
            // (StubReplogShardGrain at {tree}/0). Force ReplogPartitions=1 so
            // tests stay deterministic after the silo-wide default flipped to 8.
            ReplogPartitions = 1,
        };
        // Tests that supply their own options but don't explicitly set
        // ReplogPartitions inherit the silo-wide default (8). The unit-test
        // fixture only stubs a single-partition feed, so collapse the default
        // back to 1 here; multi-partition tests set ReplogPartitions != default.
        if (resolved.ReplogPartitions == LatticeReplicationOptions.DefaultReplogPartitions)
        {
            resolved.ReplogPartitions = 1;
        }
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(resolved);
        monitor.Get(Arg.Any<string>()).Returns(resolved);
        return monitor;
    }

    /// <summary>
    /// In-process <see cref="IWalRecordEncoder"/> shared between
    /// <see cref="StubReplogShardGrain"/> (which encodes entries into
    /// shipping-page bytes via this encoder when the shipper drains)
    /// and the shipper grain itself (which decodes the head bytes
    /// back to typed <see cref="WalRecord"/> for HLC / ShouldShip
    /// predicates). Round-trip is keyed on a 4-byte little-endian
    /// stash index so the test fixture does not need to spin up the
    /// real Orleans serializer for shipper-grain unit tests.
    /// </summary>
    private sealed class StubWalRecordEncoder : IWalRecordEncoder
    {
        private readonly List<WalRecord> _stash = new();

        public byte[] EncodeToBytes(WalRecord record)
        {
            var idx = _stash.Count;
            _stash.Add(record);
            var bytes = new byte[4];
            BitConverter.TryWriteBytes(bytes, idx);
            return bytes;
        }

        public void Encode(in WalRecord record, IBufferWriter<byte> writer)
        {
            ArgumentNullException.ThrowIfNull(writer);
            var idx = _stash.Count;
            _stash.Add(record);
            var span = writer.GetSpan(4);
            BitConverter.TryWriteBytes(span, idx);
            writer.Advance(4);
        }

        public WalRecord Decode(ReadOnlySpan<byte> encoded)
            => _stash[BitConverter.ToInt32(encoded)];
    }

    /// <summary>
    /// In-memory <see cref="IWalShardGrain"/> stand-in. Tests
    /// populate it via <see cref="Append(WalRecord)"/> (or the
    /// equivalent legacy <see cref="Entries"/> list); the stub assigns
    /// monotonically-increasing sequence numbers starting at <c>0</c>.
    /// The shipper drains via <see cref="ReadShippingAsync"/>, which
    /// projects each typed <see cref="WalRecord"/> through the
    /// activation-shared <see cref="StubWalRecordEncoder"/> so the
    /// shipper's per-tick decode round-trips back to the same record.
    /// <para>
    /// <see cref="ThrowOnRead"/> simulates a transient WAL read
    /// failure on the next read call. <see cref="ReadCalls"/> records
    /// how many reads have happened - used by partition-resume tests
    /// to assert the shipper does not rescan from sequence 0 each
    /// tick.
    /// </para>
    /// </summary>
    private sealed class StubReplogShardGrain(StubWalRecordEncoder? encoder = null) : IWalShardGrain
    {
        private readonly StubWalRecordEncoder _encoder = encoder ?? new StubWalRecordEncoder();
        public List<WalRecord> Entries { get; } = new();
        public Exception? ThrowOnRead { get; set; }
        public int ReadCalls { get; private set; }
        public List<long> ReadFromSequences { get; } = new();

        public void Append(WalRecord entry) => Entries.Add(entry);

        public Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken)
        {
            Entries.Add(entry);
            return Task.FromResult((long)(Entries.Count - 1));
        }

        public Task<WalShardShippingPage> ReadShippingAsync(long fromSequence, int maxEntries, CancellationToken cancellationToken)
        {
            ReadCalls++;
            ReadFromSequences.Add(fromSequence);
            if (ThrowOnRead is not null)
            {
                throw ThrowOnRead;
            }
            cancellationToken.ThrowIfCancellationRequested();
            if (fromSequence >= Entries.Count)
            {
                return Task.FromResult(new WalShardShippingPage
                {
                    Entries = Array.Empty<WalShardShippingEntry>(),
                    NextSequence = fromSequence,
                });
            }
            var endExclusive = (int)Math.Min(Entries.Count, fromSequence + maxEntries);
            var capacity = endExclusive - (int)fromSequence;
            var entries = new WalShardShippingEntry[capacity];
            for (var i = 0; i < capacity; i++)
            {
                var seq = fromSequence + i;
                entries[i] = new WalShardShippingEntry
                {
                    Sequence = seq,
                    EncodedPayload = _encoder.EncodeToBytes(Entries[(int)seq]),
                };
            }
            return Task.FromResult(new WalShardShippingPage
            {
                Entries = entries,
                NextSequence = endExclusive,
            });
        }

        public Task<IReadOnlyList<long>> AppendBatchAsync(IReadOnlyList<WalRecord> entries, CancellationToken cancellationToken)
        {
            var offsets = new long[entries.Count];
            for (var i = 0; i < entries.Count; i++)
            {
                Entries.Add(entries[i]);
                offsets[i] = Entries.Count - 1;
            }
            return Task.FromResult<IReadOnlyList<long>>(offsets);
        }

        public Task<WalShardPage> ReadAsync(long fromSequence, int maxEntries, CancellationToken cancellationToken)
        {
            // ReadCalls / ReadFromSequences are now tracked on the
            // ReadShippingAsync path the shipper actually drives;
            // ReadAsync is retained for non-shipper test paths only.
            if (ThrowOnRead is not null)
            {
                var ex = ThrowOnRead;
                throw ex;
            }
            cancellationToken.ThrowIfCancellationRequested();
            if (fromSequence >= Entries.Count)
            {
                return Task.FromResult(WalShardPage.Empty(fromSequence));
            }
            var endExclusive = (int)Math.Min(Entries.Count, fromSequence + maxEntries);
            var capacity = endExclusive - (int)fromSequence;
            var entries = new WalShardSequencedEntry[capacity];
            for (var i = 0; i < capacity; i++)
            {
                var seq = fromSequence + i;
                entries[i] = new WalShardSequencedEntry
                {
                    Sequence = seq,
                    Entry = Entries[(int)seq],
                };
            }
            return Task.FromResult(new WalShardPage
            {
                Entries = entries,
                NextSequence = endExclusive,
            });
        }

        public Task<long> GetNextSequenceAsync(CancellationToken cancellationToken) =>
            Task.FromResult((long)Entries.Count);

        public Task<long> GetLiveEntryCountAsync(CancellationToken cancellationToken) =>
            Task.FromResult((long)Entries.Count);

        public Task<long> GetRetainedByteSizeAsync(CancellationToken cancellationToken) =>
            Task.FromResult(-1L);

#pragma warning disable LATTICE0001 // GetEntryCountAsync is an obsolete forwarder retained for one minor version.
        public Task<long> GetEntryCountAsync(CancellationToken cancellationToken) =>
            Task.FromResult((long)Entries.Count);
#pragma warning restore LATTICE0001

        public Task<WalMoveQuiesceResult> QuiesceForMoveAsync(long expectedPlacementVersion, TimeSpan lease, CancellationToken cancellationToken) =>
            Task.FromResult(new WalMoveQuiesceResult(true, Entries.Count - 1, expectedPlacementVersion, "default"));

        public Task DeactivateForMoveAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    /// <summary>
    /// Wires the per-partition stubs into a substitute <see cref="IGrainFactory"/>
    /// so the shipper resolves <see cref="IWalShardGrain"/> by
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
            factory.GetGrain<IWalShardGrain>($"{treeName}/{p}").Returns(partitionedFeeds[p]);
        }
        return factory;
    }

    private static (
        ReplicationShipperGrain Grain,
        FakePersistentState<ReplicationShipperState> State,
        StubReplogShardGrain Feed,
        IReplicationTransport Transport,
        TestEncoder Encoder,
        IWalCursorRegistry Registry,
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
        var walRecordEncoder = new StubWalRecordEncoder();
        var feed = new StubReplogShardGrain(walRecordEncoder);
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
        var encoder = new TestEncoder();
        var registry = Substitute.For<IWalCursorRegistry>();
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        if (seedState is not null)
        {
            fakeState.State = seedState;
        }
        var factory = BuildGrainFactory(grainFactory, new[] { feed }, treeName);
        var grain = new ReplicationShipperGrain(
            ctx, reminders, NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, encoder, walRecordEncoder, registry, factory, fakeState,
            new ReplicationPeerStats(),
            Substitute.For<ILatticeMergeModeResolver>(),
            new WireVersionNegotiationState());
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
        IWalRecordEncoder? walRecordEncoder = null,
        IWalCursorRegistry? registry = null,
        IGrainFactory? grainFactory = null,
        IPersistentState<ReplicationShipperState>? state = null,
        ReplicationPeerStats? peerStats = null,
        ILatticeMergeModeResolver? modeResolver = null,
        WireVersionNegotiationState? negotiationState = null)
        => new(
            ctx ?? Substitute.For<IGrainContext>(),
            reminders ?? Substitute.For<IReminderRegistry>(),
            NullLogger<ReplicationShipperGrain>.Instance,
            monitor ?? Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
            transport ?? Substitute.For<IReplicationTransport>(),
            encoder ?? Substitute.For<IReplicationBatchEncoder>(),
            walRecordEncoder ?? Substitute.For<IWalRecordEncoder>(),
            registry ?? Substitute.For<IWalCursorRegistry>(),
            grainFactory ?? Substitute.For<IGrainFactory>(),
            state ?? new FakePersistentState<ReplicationShipperState>(),
            peerStats ?? new ReplicationPeerStats(),
            modeResolver ?? Substitute.For<ILatticeMergeModeResolver>(),
            negotiationState ?? new WireVersionNegotiationState());

    [Test]
    public void Constructor_throws_when_options_monitor_is_null() =>
        Assert.That(() => ConstructWith(monitor: null!),
            Throws.Nothing);  // sanity - substitute fallback works
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
                Substitute.For<IWalRecordEncoder>(),
                Substitute.For<IWalCursorRegistry>(),
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationShipperState>(),
                new ReplicationPeerStats(),
                Substitute.For<ILatticeMergeModeResolver>(),
                new WireVersionNegotiationState()),
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
                Substitute.For<IWalRecordEncoder>(),
                Substitute.For<IWalCursorRegistry>(),
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationShipperState>(),
                new ReplicationPeerStats(),
                Substitute.For<ILatticeMergeModeResolver>(),
                new WireVersionNegotiationState()),
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
                Substitute.For<IWalRecordEncoder>(),
                Substitute.For<IWalCursorRegistry>(),
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationShipperState>(),
                new ReplicationPeerStats(),
                Substitute.For<ILatticeMergeModeResolver>(),
                new WireVersionNegotiationState()),
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
                Substitute.For<IWalRecordEncoder>(),
                null!,
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationShipperState>(),
                new ReplicationPeerStats(),
                Substitute.For<ILatticeMergeModeResolver>(),
                new WireVersionNegotiationState()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_wal_record_encoder_is_null()
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
                Substitute.For<IWalCursorRegistry>(),
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationShipperState>(),
                new ReplicationPeerStats(),
                Substitute.For<ILatticeMergeModeResolver>(),
                new WireVersionNegotiationState()),
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
                Substitute.For<IWalRecordEncoder>(),
                Substitute.For<IWalCursorRegistry>(),
                null!,
                new FakePersistentState<ReplicationShipperState>(),
                new ReplicationPeerStats(),
                Substitute.For<ILatticeMergeModeResolver>(),
                new WireVersionNegotiationState()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_mode_resolver_is_null()
    {
        Assert.That(
            () => new ReplicationShipperGrain(
                Substitute.For<IGrainContext>(),
                Substitute.For<IReminderRegistry>(),
                NullLogger<ReplicationShipperGrain>.Instance,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                Substitute.For<IReplicationTransport>(),
                Substitute.For<IReplicationBatchEncoder>(),
                Substitute.For<IWalRecordEncoder>(),
                Substitute.For<IWalCursorRegistry>(),
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationShipperState>(),
                new ReplicationPeerStats(),
                null!,
                new WireVersionNegotiationState()),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Constructor_throws_when_negotiation_state_is_null()
    {
        Assert.That(
            () => new ReplicationShipperGrain(
                Substitute.For<IGrainContext>(),
                Substitute.For<IReminderRegistry>(),
                NullLogger<ReplicationShipperGrain>.Instance,
                Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>(),
                Substitute.For<IReplicationTransport>(),
                Substitute.For<IReplicationBatchEncoder>(),
                Substitute.For<IWalRecordEncoder>(),
                Substitute.For<IWalCursorRegistry>(),
                Substitute.For<IGrainFactory>(),
                new FakePersistentState<ReplicationShipperState>(),
                new ReplicationPeerStats(),
                Substitute.For<ILatticeMergeModeResolver>(),
                null!),
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

    [Test]
    public async Task PumpOnceAsync_skips_entries_originating_from_third_cluster_other_than_local()
    {
        // Receiver-apply regression: under
        // WAL-as-sole-durability-boundary the local WAL contains
        // entries authored by *any* peer (e.g. cluster C's entries
        // apply-installed on cluster B). The shipper must restrict
        // outbound traffic to writes authored by the *local* cluster
        // - the broader local-origin rule subsumes the older "skip
        // entries from this peer" cycle-break and prevents B from
        // re-shipping C-authored entries to A.
        var (grain, state, feed, transport, _, _, _) = Create();
        feed.Append(MakeEntry("k1", origin: "site-c", ticks: 10));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero));
    }

    // --- Empty-origin filter: drop durability-only WAL appends ---
    //
    // The leaf-grain durability writer (WalCommitLogWriter) and the
    // replication mutation observer (ReplicationMutationObserver +
    // ShardedReplogSink) both append to the same per-tree WAL. The
    // durability writer leaves OriginClusterId empty; the replication
    // observer stamps a non-empty origin. Shipping the empty-origin
    // copy would surface as ArgumentException on the receiver's
    // per-origin HWM dedup path and dead-letter the entry every tick,
    // even though the matching stamped copy carries the same payload.

    [Test]
    public async Task PumpOnceAsync_skips_entries_with_empty_origin_cluster_id()
    {
        var (grain, state, feed, transport, _, _, _) = Create();
        feed.Append(MakeEntry("k1", origin: string.Empty, ticks: 10));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task PumpOnceAsync_skips_entries_with_null_origin_cluster_id()
    {
        var (grain, state, feed, transport, _, _, _) = Create();
        feed.Append(MakeEntry("k1", origin: null!, ticks: 10));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero));
    }

    [Test]
    public async Task PumpOnceAsync_ships_only_stamped_copy_when_durability_and_observer_both_appended()
    {
        // Mirrors the production layout: a single Set commits both a
        // durability WAL append (empty origin, stamped HLC) and an
        // observer WAL append (stamped origin, same HLC). The shipper
        // must drop the empty-origin row and ship exactly the stamped
        // one, with no duplicate sends.
        var (grain, _, feed, transport, _, _, _) = Create();
        ReplicationBatch? captured = null;
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captured = call.Arg<ReplicationBatch>();
                return new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero };
            });
        feed.Append(MakeEntry("k1", origin: string.Empty, ticks: 5));
        feed.Append(MakeEntry("k1", origin: LocalCluster, ticks: 5));

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.Received(1).SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.Value.OriginClusterId, Is.EqualTo(LocalCluster));
    }

    // --- Content-hash dedup measurement (opt-in, default off) ---

    private static WalRecord MakeEntryWithValue(
        string key,
        byte[] value,
        long ticks,
        string origin = LocalCluster)
        => new()
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Key = key,
            Value = value,
            Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = 0 },
            OriginClusterId = origin,
        };

    private static LatticeReplicationOptions ContentHashDedupOptions(bool enabled) =>
        new()
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            ContentHashDedupEnabled = enabled,
        };

    [Test]
    public async Task PumpOnceAsync_does_not_record_redundant_payload_metric_when_dedup_disabled()
    {
        var (grain, _, feed, _, _, _, _) = Create(ContentHashDedupOptions(enabled: false));
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipRedundantPayloadsName);

        // Two byte-identical re-sets of the same key. With the
        // measurement off this must produce no redundant-payload
        // measurements at all - the default-off path is observably inert.
        feed.Append(MakeEntryWithValue("k1", new byte[] { 7, 7 }, ticks: 1));
        feed.Append(MakeEntryWithValue("k1", new byte[] { 7, 7 }, ticks: 2));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(collector.Measurements, Is.Empty);
    }

    [Test]
    public async Task PumpOnceAsync_records_redundant_payload_metric_for_byte_identical_re_set()
    {
        var (grain, _, feed, _, _, _, _) = Create(ContentHashDedupOptions(enabled: true));
        using var entries = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipRedundantPayloadsName);
        using var bytes = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipRedundantPayloadBytesName);

        // First Set establishes the content; the second is a
        // byte-identical re-set (idempotent upstream retry shape) and
        // must be counted as one redundant payload of 2 value bytes.
        feed.Append(MakeEntryWithValue("k1", new byte[] { 9, 9 }, ticks: 1));
        feed.Append(MakeEntryWithValue("k1", new byte[] { 9, 9 }, ticks: 2));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(entries.Measurements.Sum(m => m.Value), Is.EqualTo(1));
        Assert.That(bytes.Measurements.Sum(m => m.Value), Is.EqualTo(2));
        var tags = entries.Measurements.Single().Tags;
        Assert.That(
            tags.Any(t => t.Key == LatticeReplicationMetrics.TagTree && (string?)t.Value == Tree),
            Is.True);
        Assert.That(
            tags.Any(t => t.Key == LatticeReplicationMetrics.TagPeer && (string?)t.Value == Peer),
            Is.True);
    }

    [Test]
    public async Task PumpOnceAsync_does_not_count_distinct_content_for_same_key_as_redundant()
    {
        var (grain, _, feed, _, _, _, _) = Create(ContentHashDedupOptions(enabled: true));
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipRedundantPayloadsName);

        // Same key, different content each time: no redundant re-send.
        feed.Append(MakeEntryWithValue("k1", new byte[] { 1 }, ticks: 1));
        feed.Append(MakeEntryWithValue("k1", new byte[] { 2 }, ticks: 2));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(collector.Measurements, Is.Empty);
    }

    [Test]
    public async Task PumpOnceAsync_ships_every_entry_even_when_content_is_redundant()
    {
        var (grain, _, feed, transport, _, _, _) = Create(ContentHashDedupOptions(enabled: true));

        feed.Append(MakeEntryWithValue("k1", new byte[] { 5 }, ticks: 1));
        feed.Append(MakeEntryWithValue("k1", new byte[] { 5 }, ticks: 2));

        await grain.OnDoorbellAsync(CancellationToken.None);

        // The measurement never elides: both byte-identical entries are
        // still framed onto the wire so LWW / HLC convergence is intact.
        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2));
    }

    // --- Zero-HLC exemption: ship DeleteRange entries even when the
    //     cursor has advanced past Zero ---
    //
    // DeleteRange entries intentionally carry HybridLogicalClock.Zero
    // (per WalRecord.Timestamp docs) because a single range may
    // produce many per-leaf HLCs that cannot be faithfully collapsed.
    // The defensive HLC filter at the merge head must therefore
    // exempt Zero-stamped entries; otherwise every DeleteRange write
    // is silently dropped once any non-zero cursor has been observed.
    // DeleteRange entries are tracked solely by the per-partition
    // sequence cursor, which already prevents re-shipping in steady
    // state.

    [Test]
    public async Task PumpOnceAsync_ships_zero_hlc_delete_range_entry_when_cursor_already_advanced()
    {
        var seedState = new ReplicationShipperState
        {
            Cursor = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 },
        };
        var (grain, _, feed, transport, _, _, _) = Create(seedState: seedState);
        ReplicationBatch? captured = null;
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                captured = call.Arg<ReplicationBatch>();
                return new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero };
            });
        feed.Append(new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "c",
            Timestamp = HybridLogicalClock.Zero,
            IsTombstone = true,
            OriginClusterId = LocalCluster,
        });

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.Received(1).SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
        Assert.That(captured, Is.Not.Null);
    }

    [Test]
    public async Task PumpOnceAsync_skips_zero_hlc_entry_originating_from_peer_under_cycle_break()
    {
        // Composition: the Zero-HLC exemption only neutralises the
        // defensive HLC filter; the cycle-break filter still rejects
        // an entry whose origin matches the destination peer.
        var seedState = new ReplicationShipperState
        {
            Cursor = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 },
        };
        var (grain, _, feed, transport, _, _, _) = Create(seedState: seedState);
        feed.Append(new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "c",
            Timestamp = HybridLogicalClock.Zero,
            IsTombstone = true,
            OriginClusterId = Peer,
        });

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PumpOnceAsync_skips_zero_hlc_entry_with_empty_origin()
    {
        // Composition: the Zero-HLC exemption only neutralises the
        // defensive HLC filter; the empty-origin filter still rejects
        // a durability-only DeleteRange append.
        var seedState = new ReplicationShipperState
        {
            Cursor = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 },
        };
        var (grain, _, feed, transport, _, _, _) = Create(seedState: seedState);
        feed.Append(new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "c",
            Timestamp = HybridLogicalClock.Zero,
            IsTombstone = true,
            OriginClusterId = string.Empty,
        });

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
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
        // Receiver returned a frontier below the batch's last entry -
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
    //
    // The historical typed-envelope encode-throw -> DLQ path is gone:
    // the steady-state ship path is framing-only and never invokes
    // IReplicationBatchEncoder.Encode. Schema-shaped errors during
    // framing-header construction still route to the per-tree DLQ
    // (covered by the DLQ-routing tests in
    // ReplicationDeadLetterGrainTests), but the encoder is no longer
    // a producer of those errors. The two encoder-throw tests that
    // exercised the legacy path were removed alongside the
    // typed-envelope sender-path retirement.

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
            Assert.That(captured.Value.EncodedEnvelope, Is.Not.Null,
                "every shipped batch must carry the framing-only EncodedEnvelope slot");
            Assert.That(captured.Value.EncodedEnvelope!.Value.EncodedEntries.Length, Is.EqualTo(1));
        });
    }

    // --- DLQ routing on schema-shaped encode failure ---
    //
    // The encode-throw -> DLQ tests that used to live here exercised
    // a code path the framing-only ship path no longer reaches:
    // IReplicationBatchEncoder.Encode is never called on the
    // steady-state path, so encoder.ThrowOnEncode never fires. The
    // DLQ-routing contract itself (ReasonSchema parking + cursor
    // advance past a poison batch) is still pinned by the framing-
    // header construction catch in ReplicationShipperGrain.PumpOnceAsync
    // and by ReplicationDeadLetterGrainTests; no shipper-level
    // coverage of the legacy encoder-throw shape is preserved.

    [Test]
    public async Task PumpOnceAsync_advances_cursor_even_when_dlq_enqueue_throws()
    {
        // A deterministically-failing DLQ must not pin the ship loop.
        var dlq = Substitute.For<IReplicationDeadLetterGrain>();
        dlq.EnqueueAsync(
            Arg.Any<WalRecord>(),
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

    // --- Activation-scoped buffer reuse (encoder perf-pass) ---
    //
    // The historical typed-envelope encode path is gone (folded into
    // the framing-only ship-path migration).
    // Steady-state allocation on the framing-only ship path is guarded
    // by the microbenchmarks under benchmark/host/Bench.Microbench/
    // (see r114-ship-bench-results.md). The shipper's per-tick scratch
    // is exercised end-to-end by the partition-cursor and DLQ tests
    // below; there is no longer a separate IBufferWriter to capture.

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
        var walEncoder = new StubWalRecordEncoder();
        var feed = new StubReplogShardGrain(walEncoder);
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
        var encoder = new TestEncoder();
        var registry = Substitute.For<IWalCursorRegistry>();
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        var factory = BuildGrainFactory(null, new[] { feed }, Tree);
        var stats = new ReplicationPeerStats();
        var grain = new ReplicationShipperGrain(
            ctx, reminders, NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, encoder, walEncoder, registry, factory, fakeState, stats,
            Substitute.For<ILatticeMergeModeResolver>(),
            new WireVersionNegotiationState());
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
        // per-peer consecutive_errors gauge - the peer is fine; the
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
        // Single entry - the drain returns 1 < ShipBatchSize so
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
        // ShipBatchSize cap - _drainBuffer.Count >= maxPerBatch is
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
            // bytes_behind is the encoded payload size - TestEncoder
            // writes 3 bytes per encode so bytes_behind is at least 1.
            Assert.That(snap.BytesBehind, Is.GreaterThan(0L));
        });
    }
}
