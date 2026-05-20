using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Pins the one-encode fast path: when
/// <see cref="LatticeReplicationOptions.OneEncodeFastPath"/> is set,
/// the shipper must drain via
/// <see cref="IWalShardGrain.ReadShippingAsync"/>, populate
/// <see cref="ReplicationBatch.EncodedEnvelope"/> from the
/// pre-encoded WAL bytes, leave <see cref="ReplicationBatch.Envelope"/>
/// null, and never invoke the producer-side
/// <see cref="IReplicationBatchEncoder.Encode"/>.
/// </summary>
[TestFixture]
public sealed class ReplicationShipperGrainOneEncodeFastPathTests
{
    private const string Tree = "fp-tree";
    private const string Peer = "fp-peer";
    private const string LocalCluster = "fp-local";

    private sealed class CountingEncoder : IReplicationBatchEncoder
    {
        public string ContentType => "application/x-test";
        public int CurrentWireVersion => 1;
        public int Encodes { get; private set; }

        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)
        {
            Encodes++;
            writer.Write(new byte[] { 1, 2, 3 });
        }

        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload) =>
            throw new NotSupportedException();
    }

    private sealed class CountingWalRecordEncoder : IWalRecordEncoder
    {
        private readonly IWalRecordEncoder _inner;
        public int Encodes { get; private set; }
        public int Decodes { get; private set; }

        public CountingWalRecordEncoder(IWalRecordEncoder inner) => _inner = inner;

        public void Encode(in WalRecord record, IBufferWriter<byte> writer)
        {
            Encodes++;
            _inner.Encode(record, writer);
        }

        public WalRecord Decode(ReadOnlySpan<byte> encoded)
        {
            Decodes++;
            return _inner.Decode(encoded);
        }
    }

    private sealed class ShippingShardGrain : IWalShardGrain
    {
        public List<WalRecord> Entries { get; } = new();
        public List<long> ShippingFromSequences { get; } = new();
        public int ShippingReadCalls { get; private set; }
        public int LegacyReadCalls { get; private set; }

        private readonly IWalRecordEncoder _encoder;

        public ShippingShardGrain(IWalRecordEncoder encoder) => _encoder = encoder;

        public Task<long> AppendAsync(WalRecord entry, CancellationToken cancellationToken)
        {
            Entries.Add(entry);
            return Task.FromResult((long)(Entries.Count - 1));
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
            LegacyReadCalls++;
            if (fromSequence >= Entries.Count)
            {
                return Task.FromResult(WalShardPage.Empty(fromSequence));
            }
            var endExclusive = (int)Math.Min(Entries.Count, fromSequence + maxEntries);
            var capacity = endExclusive - (int)fromSequence;
            var arr = new WalShardSequencedEntry[capacity];
            for (var i = 0; i < capacity; i++)
            {
                var seq = fromSequence + i;
                arr[i] = new WalShardSequencedEntry { Sequence = seq, Entry = Entries[(int)seq] };
            }
            return Task.FromResult(new WalShardPage { Entries = arr, NextSequence = endExclusive });
        }

        public Task<WalShardShippingPage> ReadShippingAsync(long fromSequence, int maxEntries, CancellationToken cancellationToken)
        {
            ShippingReadCalls++;
            ShippingFromSequences.Add(fromSequence);
            if (fromSequence >= Entries.Count)
            {
                return Task.FromResult(WalShardShippingPage.Empty(fromSequence));
            }
            var endExclusive = (int)Math.Min(Entries.Count, fromSequence + maxEntries);
            var capacity = endExclusive - (int)fromSequence;
            var arr = new WalShardShippingEntry[capacity];
            for (var i = 0; i < capacity; i++)
            {
                var seq = fromSequence + i;
                var writer = new ArrayBufferWriter<byte>();
                _encoder.Encode(Entries[(int)seq], writer);
                arr[i] = new WalShardShippingEntry
                {
                    Sequence = seq,
                    EncodedPayload = writer.WrittenSpan.ToArray(),
                };
            }
            return Task.FromResult(new WalShardShippingPage { Entries = arr, NextSequence = endExclusive });
        }

        public Task<long> GetNextSequenceAsync(CancellationToken cancellationToken) =>
            Task.FromResult((long)Entries.Count);

        public Task<long> GetLiveEntryCountAsync(CancellationToken cancellationToken) =>
            Task.FromResult((long)Entries.Count);

#pragma warning disable LATTICE0001
        public Task<long> GetEntryCountAsync(CancellationToken cancellationToken) =>
            Task.FromResult((long)Entries.Count);
#pragma warning restore LATTICE0001
    }

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(LatticeReplicationOptions opts)
    {
        var m = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        m.CurrentValue.Returns(opts);
        m.Get(Arg.Any<string>()).Returns(opts);
        return m;
    }

    private static WalRecord MakeEntry(string key, long ticks, string origin = LocalCluster) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 7 },
        Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = 0 },
        OriginClusterId = origin,
    };

    private sealed class CapturingTransport : IReplicationTransport
    {
        public ReplicationBatch? LastBatch { get; private set; }
        public int Calls { get; private set; }

        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        {
            Calls++;
            LastBatch = batch;
            return Task.FromResult(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
            });
        }
    }

    private static (
        ReplicationShipperGrain Grain,
        ShippingShardGrain Feed,
        CapturingTransport Transport,
        CountingEncoder BatchEncoder,
        CountingWalRecordEncoder WalEncoder) BuildFastPathGrain(
            bool fastPath = true,
            ReplicationShipperState? seedState = null)
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var inner = new OrleansBinaryWalRecordEncoder(
            services.GetRequiredService<Serializer<WalRecord>>());
        var walEncoder = new CountingWalRecordEncoder(inner);

        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var reminders = Substitute.For<IReminderRegistry>();
        var monitor = Monitor(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            OneEncodeFastPath = fastPath,
            ShipBatchSize = 32,
        });
        var feed = new ShippingShardGrain(inner);
        var transport = new CapturingTransport();
        var batchEncoder = new CountingEncoder();
        var registry = Substitute.For<IWalCursorRegistry>();
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        if (seedState is not null)
        {
            fakeState.State = seedState;
        }
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(feed);
        var grain = new ReplicationShipperGrain(
            ctx, reminders, NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, batchEncoder, walEncoder, registry, factory, fakeState,
            new ReplicationPeerStats());
        grain.InitializeForTesting(Tree, Peer);
        return (grain, feed, transport, batchEncoder, walEncoder);
    }

    [Test]
    public async Task PumpOnceAsync_fastpath_uses_shipping_read_and_skips_batch_encoder()
    {
        var (grain, feed, transport, batchEncoder, walEncoder) = BuildFastPathGrain();
        feed.Entries.Add(MakeEntry("k1", ticks: 1));
        feed.Entries.Add(MakeEntry("k2", ticks: 2));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(feed.ShippingReadCalls, Is.GreaterThan(0),
                "fast path must drain via ReadShippingAsync");
            Assert.That(feed.LegacyReadCalls, Is.EqualTo(0),
                "fast path must not call the typed ReadAsync seam");
            Assert.That(batchEncoder.Encodes, Is.EqualTo(0),
                "producer-side batch encoder must not run on the fast path");
            Assert.That(transport.Calls, Is.EqualTo(1));
            Assert.That(transport.LastBatch, Is.Not.Null);
            Assert.That(transport.LastBatch!.Value.EncodedEnvelope, Is.Not.Null,
                "EncodedEnvelope must be populated on the fast path");
            Assert.That(transport.LastBatch!.Value.Envelope, Is.Null,
                "typed Envelope must remain null on the fast path");
            Assert.That(transport.LastBatch!.Value.Payload.IsEmpty, Is.True,
                "Payload must be empty on the fast path");
            Assert.That(transport.LastBatch!.Value.EncodedEnvelope!.Value.EncodedEntries.Length,
                Is.EqualTo(2),
                "EncodedEntries must mirror the two shipped WAL entries");
            // Per-entry decode: one decode per head per partition advance.
            Assert.That(walEncoder.Decodes, Is.GreaterThanOrEqualTo(2),
                "each head entry must be decoded once for filter predicates");
        });
    }

    [Test]
    public async Task PumpOnceAsync_fastpath_off_uses_legacy_read_and_keeps_typed_envelope()
    {
        var (grain, feed, transport, batchEncoder, walEncoder) =
            BuildFastPathGrain(fastPath: false);
        feed.Entries.Add(MakeEntry("k1", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(feed.LegacyReadCalls, Is.GreaterThan(0));
            Assert.That(feed.ShippingReadCalls, Is.EqualTo(0));
            Assert.That(transport.LastBatch, Is.Not.Null);
            Assert.That(transport.LastBatch!.Value.Envelope, Is.Not.Null,
                "legacy path keeps the typed envelope populated");
            Assert.That(transport.LastBatch!.Value.EncodedEnvelope, Is.Null,
                "EncodedEnvelope must remain null on the legacy path");
            Assert.That(walEncoder.Decodes, Is.EqualTo(0),
                "WAL record encoder is not exercised on the legacy read path");
            _ = batchEncoder;
        });
    }

    [Test]
    public async Task PumpOnceAsync_fastpath_filters_cycle_break_origin_from_segments()
    {
        // Entries authored by the peer cluster must be dropped by
        // ShouldShip; the corresponding encoded segments must NOT
        // appear in EncodedEnvelope.
        var (grain, feed, transport, _, _) = BuildFastPathGrain();
        feed.Entries.Add(MakeEntry("local-1", ticks: 1, origin: LocalCluster));
        feed.Entries.Add(MakeEntry("peer-2", ticks: 2, origin: Peer));      // filtered out
        feed.Entries.Add(MakeEntry("local-3", ticks: 3, origin: LocalCluster));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(transport.LastBatch, Is.Not.Null);
        var segs = transport.LastBatch!.Value.EncodedEnvelope!.Value.EncodedEntries;
        Assert.That(segs.Length, Is.EqualTo(2),
            "filtered cycle-break entry must not appear in the encoded segments");
    }

    [Test]
    public async Task PumpOnceAsync_fastpath_segments_align_with_drain_buffer_after_filter()
    {
        // KeyPrefixes filter drops 'skip-*'; remaining entries must
        // decode to the same keys present in the drain buffer.
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var canonical = new OrleansBinaryWalRecordEncoder(
            services.GetRequiredService<Serializer<WalRecord>>());

        var (grain, feed, transport, _, _) = BuildFastPathGrainWith(opts =>
        {
            opts.KeyPrefixes = new[] { "keep-" };
        });
        feed.Entries.Add(MakeEntry("keep-1", ticks: 1));
        feed.Entries.Add(MakeEntry("skip-1", ticks: 2));   // filtered
        feed.Entries.Add(MakeEntry("keep-2", ticks: 3));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(transport.LastBatch, Is.Not.Null);
        var segs = transport.LastBatch!.Value.EncodedEnvelope!.Value.EncodedEntries;
        Assert.That(segs.Length, Is.EqualTo(2));
        var keys = new List<string?>();
        for (var i = 0; i < segs.Length; i++)
        {
            var seg = segs.Span[i];
            var rec = canonical.Decode(new ReadOnlySpan<byte>(seg.Array!, seg.Offset, seg.Count));
            keys.Add(rec.Key);
        }
        Assert.That(keys, Is.EqualTo(new[] { "keep-1", "keep-2" }));
    }

    [Test]
    public async Task PumpOnceAsync_fastpath_reuses_scratch_array_across_pumps()
    {
        // The encoded-envelope scratch array is activation-scoped and
        // grown lazily; consecutive pumps must not reallocate it
        // when batch sizes are bounded.
        var (grain, feed, transport, _, _) = BuildFastPathGrain();
        feed.Entries.Add(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);
        var firstSegs = transport.LastBatch!.Value.EncodedEnvelope!.Value.EncodedEntries;

        feed.Entries.Add(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);
        var secondSegs = transport.LastBatch!.Value.EncodedEnvelope!.Value.EncodedEntries;

        // Both Memory<T> handles point at the same activation-scoped
        // backing array.
        Assert.That(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(firstSegs, out var a),
            Is.True);
        Assert.That(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(secondSegs, out var b),
            Is.True);
        Assert.That(a.Array, Is.SameAs(b.Array),
            "fast-path scratch array must be reused across pumps");
    }

    [Test]
    public async Task PumpOnceAsync_fastpath_advances_partition_cursor_past_shipped_entries()
    {
        var (grain, feed, transport, _, _) = BuildFastPathGrain();
        feed.Entries.Add(MakeEntry("k1", ticks: 1));
        feed.Entries.Add(MakeEntry("k2", ticks: 2));

        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.That(transport.LastBatch, Is.Not.Null);
        var firstFrom = feed.ShippingFromSequences[0];
        Assert.That(firstFrom, Is.EqualTo(0L),
            "first ship reads from sequence 0");

        // Second tick - no new entries, but cursor must have advanced
        // past the previously shipped sequences.
        await grain.OnDoorbellAsync(CancellationToken.None);
        var lastFrom = feed.ShippingFromSequences[^1];
        Assert.That(lastFrom, Is.EqualTo(2L),
            "next-pump shipping read must resume past the durable cursor");
    }

    [Test]
    public async Task PumpOnceAsync_fastpath_no_op_when_no_entries()
    {
        var (grain, _, transport, batchEncoder, _) = BuildFastPathGrain();
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(transport.Calls, Is.EqualTo(0));
            Assert.That(batchEncoder.Encodes, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task PumpOnceAsync_fastpath_envelope_header_carries_origin_hash_and_entry_count()
    {
        var (grain, feed, transport, _, _) = BuildFastPathGrain();
        feed.Entries.Add(MakeEntry("k1", ticks: 1));
        feed.Entries.Add(MakeEntry("k2", ticks: 2));
        feed.Entries.Add(MakeEntry("k3", ticks: 3));

        await grain.OnDoorbellAsync(CancellationToken.None);

        var header = transport.LastBatch!.Value.EncodedEnvelope!.Value.Header;
        Assert.Multiple(() =>
        {
            Assert.That(header.Magic, Is.EqualTo(EncodedBatchHeader.MagicValue));
            Assert.That(header.WireVersion, Is.EqualTo(EncodedBatchHeader.CurrentWireVersion));
            Assert.That(header.EntryCount, Is.EqualTo(3));
            Assert.That(header.OriginClusterIdHash,
                Is.EqualTo(EncodedBatchHeader.HashClusterId(LocalCluster)));
        });
    }

    private static (
        ReplicationShipperGrain Grain,
        ShippingShardGrain Feed,
        CapturingTransport Transport,
        CountingEncoder BatchEncoder,
        CountingWalRecordEncoder WalEncoder) BuildFastPathGrainWith(
            Action<LatticeReplicationOptions> tweak)
    {
        var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var inner = new OrleansBinaryWalRecordEncoder(
            services.GetRequiredService<Serializer<WalRecord>>());
        var walEncoder = new CountingWalRecordEncoder(inner);

        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var reminders = Substitute.For<IReminderRegistry>();
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            OneEncodeFastPath = true,
            ShipBatchSize = 32,
        };
        tweak(opts);
        var monitor = Monitor(opts);

        var feed = new ShippingShardGrain(inner);
        var transport = new CapturingTransport();
        var batchEncoder = new CountingEncoder();
        var registry = Substitute.For<IWalCursorRegistry>();
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(feed);
        var grain = new ReplicationShipperGrain(
            ctx, reminders, NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, batchEncoder, walEncoder, registry, factory, fakeState,
            new ReplicationPeerStats());
        grain.InitializeForTesting(Tree, Peer);
        return (grain, feed, transport, batchEncoder, walEncoder);
    }
}