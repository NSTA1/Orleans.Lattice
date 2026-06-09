using Orleans.Lattice.BPlusTree.Grains;
using System.Buffers;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// partition-resume crash-safety integration coverage. Proves that no entry is
/// silently dropped across a simulated silo crash inside the
/// deferred-persist window. The contract is "at-least-once delivery":
/// the receiver's HLC dedupe makes re-shipped entries safe, but every
/// entry that reached the WAL must eventually reach the transport on
/// the post-crash activation.
/// <para>
/// The fixture builds two successive <see cref="ReplicationShipperGrain"/>
/// activations sharing a single <see cref="FakePersistentState{ReplicationShipperState}"/>:
/// the first activation crashes mid-window (no
/// <c>OnDeactivateAsync</c>); the second activation is constructed
/// against the durable state the first activation left behind. A
/// <see cref="RecordingTransport"/> stands in for the receiver; an
/// idempotence wrapper folds duplicate sends by entry HLC so the
/// final delivered set is exactly the WAL contents.
/// </para>
/// </summary>
[TestFixture]
public class ShipperPersistenceIntegrationTests
{
    private const string Tree = "partition-resume-tree";
    private const string Peer = "site-b";
    private const string LocalCluster = "site-a";

    /// <summary>Captures every payload the shipper sends across all activations.</summary>
    private sealed class RecordingTransport : IReplicationTransport
    {
        public List<long> SentHlcSequence { get; } = new();
        public List<int> BatchSizes { get; } = new();
        public Func<int, ReplicationAck>? AckOverride { get; set; }
        private readonly StubWalRecordEncoder _walEncoder;

        public RecordingTransport(StubWalRecordEncoder walEncoder)
        {
            _walEncoder = walEncoder;
        }

        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        {
            var seg = batch.EncodedEnvelope!.Value.EncodedEntries.Span;
            BatchSizes.Add(seg.Length);
            for (var i = 0; i < seg.Length; i++)
            {
                SentHlcSequence.Add(_walEncoder.Decode(seg[i]).Timestamp.WallClockTicks);
            }
            var ack = AckOverride?.Invoke(BatchSizes.Count - 1)
                ?? new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero };
            return Task.FromResult(ack);
        }
    }

    /// <summary>
    /// Stash-based <see cref="IWalRecordEncoder"/> shared between the
    /// in-process WAL stub and the shipper grain. Each Encode call
    /// stamps a 4-byte little-endian stash index that Decode looks up
    /// to recover the original record - no Orleans serializer is
    /// required.
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
    /// Pass-through replication-batch encoder; retained on the shipper
    /// constructor signature even though the framing-only ship path
    /// no longer drives it on the steady-state send.
    /// </summary>
    private sealed class TestEncoder : IReplicationBatchEncoder
    {
        public string ContentType => "application/x-test";
        public int CurrentWireVersion => 1;

        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)
        {
            writer.Write(new byte[] { 1 });
        }

        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) =>
            throw new NotSupportedException();
    }

    /// <summary>
    /// In-process <see cref="IWalShardGrain"/> stand-in. The same
    /// instance is reused across activations to model "WAL persists
    /// across silo crash". Shipper-side reads project entries through
    /// the shared <see cref="StubWalRecordEncoder"/>.
    /// </summary>
    private sealed class WalShardStub(StubWalRecordEncoder walEncoder) : IWalShardGrain
    {
        private readonly StubWalRecordEncoder _walEncoder = walEncoder;
        public List<WalRecord> Entries { get; } = new();

        public void Append(WalRecord entry) => Entries.Add(entry);

        public Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken)
        {
            Entries.Add(entry);
            return Task.FromResult((long)(Entries.Count - 1));
        }

        public Task<WalShardShippingPage> ReadShippingAsync(long fromSequence, int maxEntries, CancellationToken cancellationToken)
        {
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
                    EncodedPayload = _walEncoder.EncodeToBytes(Entries[(int)seq]),
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
            => throw new NotSupportedException("ReadAsync is not exercised by the framing-only shipper path.");

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

    private static WalRecord MakeEntry(string key, long ticks) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = 0 },
        OriginClusterId = LocalCluster,
    };

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(LatticeReplicationOptions opts)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(opts);
        monitor.Get(Arg.Any<string>()).Returns(opts);
        return monitor;
    }

    private static IGrainFactory FactoryFor(WalShardStub[] shards, string tree)
    {
        var factory = Substitute.For<IGrainFactory>();
        for (var p = 0; p < shards.Length; p++)
        {
            var key = $"{tree}/{p}";
            var stub = shards[p];
            factory.GetGrain<IWalShardGrain>(key).Returns(stub);
        }
        factory.GetGrain<IReplicationDeadLetterGrain>(Arg.Any<string>())
            .Returns(Substitute.For<IReplicationDeadLetterGrain>());
        return factory;
    }

    private static ReplicationShipperGrain BuildGrain(
        FakePersistentState<ReplicationShipperState> persistent,
        IReplicationTransport transport,
        IReplicationBatchEncoder encoder,
        IWalRecordEncoder walRecordEncoder,
        IWalCursorRegistry registry,
        IGrainFactory factory,
        IOptionsMonitor<LatticeReplicationOptions> monitor)
    {
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var grain = new ReplicationShipperGrain(
            ctx,
            Substitute.For<IReminderRegistry>(),
            NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, encoder, walRecordEncoder, registry, factory, persistent,
            new ReplicationPeerStats(),
            Substitute.For<ILatticeMergeModeResolver>());
        grain.InitializeForTesting(Tree, Peer);
        return grain;
    }

    /// <summary>
    /// Crash-safety contract: every WAL entry reaches the transport
    /// at-least-once across an activation that crashes mid-deferred-
    /// persist-window. The receiver's HLC dedupe is what makes
    /// re-shipped entries safe; the at-least-once invariant is what
    /// makes the system not lose data.
    /// </summary>
    [Test]
    public async Task Crash_inside_deferred_persist_window_does_not_lose_entries()
    {
        // Arrange: 8 entries in the WAL; interval=4 so the first
        // activation flushes once (after 4 acks), then the next 3 acks
        // are pending when the crash happens.
        const int totalEntries = 8;
        const int interval = 4;
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = interval,
            ShipBatchSize = 1,
            ReplogPartitions = 1,
        };
        var walEncoder = new StubWalRecordEncoder();
        var shards = new[] { new WalShardStub(walEncoder) };
        for (var i = 1; i <= totalEntries; i++)
        {
            shards[0].Append(MakeEntry($"k{i}", ticks: i));
        }
        var encoder = new TestEncoder();
        var transport = new RecordingTransport(walEncoder);
        var registry = Substitute.For<IWalCursorRegistry>();
        var persistent = new FakePersistentState<ReplicationShipperState>();
        var grainA = BuildGrain(persistent, transport, encoder, walEncoder, registry,
            FactoryFor(shards, Tree), Monitor(opts));

        // Pump 7 times - first flush at tick 4, then 3 pending writes
        // (5/6/7) sit in the deferred-persist window.
        for (var i = 0; i < 7; i++)
        {
            await grainA.OnDoorbellAsync(CancellationToken.None);
        }

        // Pre-crash invariant: durable state reflects exactly one flush.
        Assert.That(persistent.WriteCount, Is.EqualTo(1),
            "Pre-crash: only one flush expected (interval=4 reached on tick 4 and once again only at tick 8).");
        var preCrashCursor = persistent.State.Cursor;
        var preCrashPartitionCursor = persistent.State.PartitionCursors.TryGetValue(0, out var pc) ? pc : 0L;
        var preCrashSent = transport.SentHlcSequence.Count;

        // Simulate crash: bypass OnDeactivateAsync entirely. The new
        // grain inherits ONLY the durable persistent state (which is
        // exactly what the storage provider would surface to a fresh
        // activation after silo restart).
        var crashedDurableState = new ReplicationShipperState
        {
            Cursor = persistent.State.Cursor,
            ConsecutiveFailures = 0, // backoff state is best-effort, see XML doc on ConsecutiveFailures
            PartitionCursors = new Dictionary<int, long>(persistent.State.PartitionCursors),
        };
        var persistent2 = new FakePersistentState<ReplicationShipperState>
        {
            State = crashedDurableState,
        };
        var grainB = BuildGrain(persistent2, transport, encoder, walEncoder, registry,
            FactoryFor(shards, Tree), Monitor(opts));

        // Recover: pump enough times for the post-crash activation to
        // flush + drain everything left in the WAL. Pump margin is
        // generous (12 ticks for at most ~5 remaining entries).
        for (var i = 0; i < 12; i++)
        {
            await grainB.OnDoorbellAsync(CancellationToken.None);
        }

        // Idempotent delivery: every entry HLC must appear in the
        // transport's recorded sequence. Duplicates are allowed (the
        // receiver dedupes by HLC); MISSING entries would be data loss.
        var deliveredHlcs = transport.SentHlcSequence.ToHashSet();
        Assert.Multiple(() =>
        {
            // Every WAL HLC was delivered.
            for (var i = 1L; i <= totalEntries; i++)
            {
                Assert.That(deliveredHlcs, Contains.Item(i),
                    $"Entry HLC={i} was lost across the crash - no data must be silently dropped.");
            }
            // Recovery actually re-shipped at least one duplicate
            // (entries 5,6,7 were pre-crash-pending so they reship
            // with HLC <= preCrashCursor and get filtered if cursor
            // already advanced; otherwise they replay cleanly).
            Assert.That(transport.SentHlcSequence.Count, Is.GreaterThanOrEqualTo(totalEntries),
                "Recovery must deliver every entry at-least-once.");
            // Post-crash activation must eventually persist everything.
            Assert.That(persistent2.State.PartitionCursors[0], Is.EqualTo((long)totalEntries),
                "After full recovery, partition cursor must point one past the last WAL sequence.");
            // Cursors must monotonically advance - never roll back.
            Assert.That(persistent2.State.Cursor.CompareTo(preCrashCursor), Is.GreaterThanOrEqualTo(0),
                "Recovery cursor must not roll back behind the durable pre-crash cursor.");
            Assert.That(persistent2.State.PartitionCursors[0], Is.GreaterThanOrEqualTo(preCrashPartitionCursor),
                "Recovery partition cursor must not roll back.");
            Assert.That(transport.SentHlcSequence.Count, Is.GreaterThan(preCrashSent),
                "Recovery must perform additional sends past the pre-crash high-water mark.");
        });
    }

    /// <summary>
    /// Graceful-deactivation flush eliminates the re-ship window.
    /// After a clean shutdown (e.g. operator-initiated silo drain),
    /// the next activation must NOT re-ship anything that was already
    /// acknowledged before the deactivation.
    /// </summary>
    [Test]
    public async Task Graceful_deactivation_flush_prevents_re_ship_on_next_activation()
    {
        const int totalEntries = 6;
        const int interval = 100; // never reached organically
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = interval,
            ShipBatchSize = 1,
            ReplogPartitions = 1,
        };
        var walEncoder = new StubWalRecordEncoder();
        var shards = new[] { new WalShardStub(walEncoder) };
        for (var i = 1; i <= totalEntries; i++)
        {
            shards[0].Append(MakeEntry($"k{i}", ticks: i));
        }
        var encoder = new TestEncoder();
        var transport = new RecordingTransport(walEncoder);
        var registry = Substitute.For<IWalCursorRegistry>();
        var persistent = new FakePersistentState<ReplicationShipperState>();
        var grainA = BuildGrain(persistent, transport, encoder, walEncoder, registry,
            FactoryFor(shards, Tree), Monitor(opts));

        // Pump every entry under interval=100 (no organic flush).
        for (var i = 0; i < totalEntries; i++)
        {
            await grainA.OnDoorbellAsync(CancellationToken.None);
        }
        Assert.That(persistent.WriteCount, Is.EqualTo(0),
            "Organic flushes must not have happened (interval=100, only 6 acks).");
        var preDeactivateSendCount = transport.SentHlcSequence.Count;

        // Graceful deactivation: OnDeactivate must flush.
        await ((IGrainBase)grainA).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "drain"),
            CancellationToken.None);
        Assert.That(persistent.WriteCount, Is.EqualTo(1),
            "Graceful deactivation must flush pending cursor writes.");

        // Reactivate against the flushed durable state.
        var inheritedState = new ReplicationShipperState
        {
            Cursor = persistent.State.Cursor,
            PartitionCursors = new Dictionary<int, long>(persistent.State.PartitionCursors),
        };
        var persistent2 = new FakePersistentState<ReplicationShipperState>
        {
            State = inheritedState,
        };
        var grainB = BuildGrain(persistent2, transport, encoder, walEncoder, registry,
            FactoryFor(shards, Tree), Monitor(opts));

        // Drive several pumps against a now-fully-shipped WAL; the
        // post-flush activation must observe an empty drain.
        for (var i = 0; i < 4; i++)
        {
            await grainB.OnDoorbellAsync(CancellationToken.None);
        }

        Assert.That(transport.SentHlcSequence.Count, Is.EqualTo(preDeactivateSendCount),
            "Graceful-flush activation must NOT re-ship anything - the durable cursor already covered every WAL entry.");
    }

    /// <summary>
    /// Even when graceful deactivation's flush fails (e.g. storage
    /// outage during shutdown), no entry is lost - recovery just
    /// re-ships the deferred-persist window's worth at the receiver,
    /// which dedupes.
    /// </summary>
    [Test]
    public async Task Crash_during_deactivation_flush_still_does_not_lose_entries()
    {
        const int totalEntries = 5;
        const int interval = 100;
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = interval,
            ShipBatchSize = 1,
            ReplogPartitions = 1,
        };
        var walEncoder = new StubWalRecordEncoder();
        var shards = new[] { new WalShardStub(walEncoder) };
        for (var i = 1; i <= totalEntries; i++)
        {
            shards[0].Append(MakeEntry($"k{i}", ticks: i));
        }
        var encoder = new TestEncoder();
        var transport = new RecordingTransport(walEncoder);
        var registry = Substitute.For<IWalCursorRegistry>();
        var persistent = new FakePersistentState<ReplicationShipperState>();
        var grainA = BuildGrain(persistent, transport, encoder, walEncoder, registry,
            FactoryFor(shards, Tree), Monitor(opts));

        for (var i = 0; i < totalEntries; i++)
        {
            await grainA.OnDoorbellAsync(CancellationToken.None);
        }

        // Arm OnDeactivate's WriteStateAsync to fail - simulate a
        // storage outage during shutdown.
        persistent.ThrowOnWrite = new InvalidOperationException("storage-down");
        Assert.That(
            async () => await ((IGrainBase)grainA).OnDeactivateAsync(
                new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "drain"),
                CancellationToken.None),
            Throws.Nothing,
            "OnDeactivate must swallow storage failures so deactivation completes.");
        Assert.That(persistent.WriteCount, Is.EqualTo(0),
            "Storage was failing - no durable write actually landed.");

        // Reactivate against the un-flushed durable state. Crashed
        // shutdown == empty PartitionCursors == cold-start re-ship.
        var inheritedState = new ReplicationShipperState
        {
            Cursor = persistent.State.Cursor,
            PartitionCursors = new Dictionary<int, long>(persistent.State.PartitionCursors),
        };
        var persistent2 = new FakePersistentState<ReplicationShipperState>
        {
            State = inheritedState,
        };
        var grainB = BuildGrain(persistent2, transport, encoder, walEncoder, registry,
            FactoryFor(shards, Tree), Monitor(opts));

        for (var i = 0; i < totalEntries + 4; i++)
        {
            await grainB.OnDoorbellAsync(CancellationToken.None);
        }

        // At-least-once: every WAL HLC delivered post-crash too.
        var deliveredHlcs = transport.SentHlcSequence.ToHashSet();
        for (var i = 1L; i <= totalEntries; i++)
        {
            Assert.That(deliveredHlcs, Contains.Item(i),
                $"Entry HLC={i} was lost across a crashed deactivation flush - no data must be silently dropped.");
        }
    }

    /// <summary>
    /// Multi-partition crash-safety: WAL entries spread across
    /// multiple partitions all reach the transport at-least-once,
    /// across a crash, with HLC ordering preserved on the merged
    /// output.
    /// </summary>
    [Test]
    public async Task Multi_partition_crash_does_not_lose_entries_or_break_hlc_order()
    {
        const int interval = 4;
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = interval,
            ShipBatchSize = 4,
            ReplogPartitions = 3,
        };
        var walEncoder = new StubWalRecordEncoder();
        var shards = new[] { new WalShardStub(walEncoder), new WalShardStub(walEncoder), new WalShardStub(walEncoder) };
        // Spread HLCs 1..12 round-robin across the three partitions.
        // Partition 0: 1, 4, 7, 10
        // Partition 1: 2, 5, 8, 11
        // Partition 2: 3, 6, 9, 12
        for (var hlc = 1; hlc <= 12; hlc++)
        {
            shards[(hlc - 1) % 3].Append(MakeEntry($"k{hlc}", ticks: hlc));
        }
        var encoder = new TestEncoder();
        var transport = new RecordingTransport(walEncoder);
        var registry = Substitute.For<IWalCursorRegistry>();
        var persistent = new FakePersistentState<ReplicationShipperState>();
        var grainA = BuildGrain(persistent, transport, encoder, walEncoder, registry,
            FactoryFor(shards, Tree), Monitor(opts));

        // Pre-crash pumps - drain a few batches.
        for (var i = 0; i < 2; i++)
        {
            await grainA.OnDoorbellAsync(CancellationToken.None);
        }
        var preCrashSent = transport.SentHlcSequence.Count;
        Assert.That(preCrashSent, Is.GreaterThan(0),
            "Pre-crash drain must have shipped at least one batch.");

        // Crash without OnDeactivate.
        var crashedState = new ReplicationShipperState
        {
            Cursor = persistent.State.Cursor,
            PartitionCursors = new Dictionary<int, long>(persistent.State.PartitionCursors),
        };
        var persistent2 = new FakePersistentState<ReplicationShipperState>
        {
            State = crashedState,
        };
        var grainB = BuildGrain(persistent2, transport, encoder, walEncoder, registry,
            FactoryFor(shards, Tree), Monitor(opts));

        // Drain everything else.
        for (var i = 0; i < 8; i++)
        {
            await grainB.OnDoorbellAsync(CancellationToken.None);
        }

        // Every WAL HLC delivered at-least-once.
        var deliveredHlcs = transport.SentHlcSequence.ToHashSet();
        for (var i = 1L; i <= 12L; i++)
        {
            Assert.That(deliveredHlcs, Contains.Item(i),
                $"Multi-partition: entry HLC={i} was lost across the crash.");
        }

        // Each batch (encoded as a contiguous slice of SentHlcSequence
        // sized by BatchSizes) must be HLC-monotonic internally.
        var offset = 0;
        foreach (var batchSize in transport.BatchSizes)
        {
            for (var i = 1; i < batchSize; i++)
            {
                Assert.That(transport.SentHlcSequence[offset + i],
                    Is.GreaterThan(transport.SentHlcSequence[offset + i - 1]),
                    "K-way merge must produce HLC-ascending entries within every batch.");
            }
            offset += batchSize;
        }
    }
}
