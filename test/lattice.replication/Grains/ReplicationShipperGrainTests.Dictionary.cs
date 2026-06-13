using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Per-peer shared-dictionary negotiation coverage for the shipper. Verifies
/// that with negotiation off the configured dictionary id is stamped verbatim
/// (byte-identical to before the feature), that an unknown or non-matching
/// peer capability falls back to dictionary-less <see cref="LatticeCompression.Zstd"/>
/// (never shipping a frame the peer cannot decode), and that the negotiation
/// adapts once the peer advertises the configured id.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private static uint CapturedHeaderDictionaryId(IReplicationTransport transport)
    {
        var calls = transport.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
            .ToList();
        Assert.That(calls, Is.Not.Empty,
            "shipper must have invoked the transport at least once before this assertion");
        var batch = (ReplicationBatch)calls[^1].GetArguments()[0]!;
        Assert.That(batch.EncodedEnvelope, Is.Not.Null);
        return batch.EncodedEnvelope!.Value.Header.DictionaryId;
    }

    private static void AdvertiseDictionaryIds(IReplicationTransport transport, params uint[] ids)
    {
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                AdvertisedDictionaryIds = ids,
            });
    }

    [Test]
    public async Task PumpOnceAsync_stamps_configured_dictionary_id_when_negotiation_disabled()
    {
        // Negotiation off: the shipper must stamp the configured dictionary
        // id verbatim regardless of what (if anything) the peer advertises -
        // the bytes on the wire are byte-identical to before the feature.
        var (grain, _, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            FramingCompression = LatticeCompression.ZstdDictionary,
            FramingCompressionDictionaryId = 7u,
            FramingCompressionMinBatchBytes = 0,
            DictionaryNegotiationEnabled = false,
        });
        feed.Append(MakeEntry("k", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.ZstdDictionary));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(7u));
        });
    }

    [Test]
    public async Task PumpOnceAsync_falls_back_to_dictionary_less_zstd_when_peer_capability_unknown()
    {
        // Negotiation on, but the peer has not advertised a dictionary
        // capability (the default ack omits the slot). The shipper must
        // conservatively fall back to dictionary-less Zstd so the peer can
        // always decode the frame.
        var (grain, _, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            FramingCompression = LatticeCompression.ZstdDictionary,
            FramingCompressionDictionaryId = 7u,
            FramingCompressionMinBatchBytes = 0,
            DictionaryNegotiationEnabled = true,
        });
        feed.Append(MakeEntry("k", ticks: 1));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.Zstd));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(0u));
        });
    }

    [Test]
    public async Task PumpOnceAsync_stamps_negotiated_dictionary_id_after_peer_advertises_it()
    {
        var (grain, _, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            FramingCompression = LatticeCompression.ZstdDictionary,
            FramingCompressionDictionaryId = 7u,
            FramingCompressionMinBatchBytes = 0,
            DictionaryNegotiationEnabled = true,
        });
        AdvertiseDictionaryIds(transport, 7u);

        // Tick 1: capability not yet captured -> conservative fallback. The
        // ack from this tick advertises id 7, captured for the next tick.
        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.Zstd));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(0u));
        });

        // Tick 2: the peer advertised id 7, so the shipper now compresses
        // with the negotiated dictionary.
        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.ZstdDictionary));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(7u));
        });
    }

    [Test]
    public async Task PumpOnceAsync_falls_back_to_dictionary_less_zstd_when_peer_advertises_a_different_id()
    {
        var (grain, _, feed, transport, _, _, _) = Create(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            FramingCompression = LatticeCompression.ZstdDictionary,
            FramingCompressionDictionaryId = 7u,
            FramingCompressionMinBatchBytes = 0,
            DictionaryNegotiationEnabled = true,
        });
        AdvertiseDictionaryIds(transport, 3u);

        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);

        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.Zstd));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(0u));
        });
    }

    private static void AdvertiseDictionaries(
        IReplicationTransport transport, params AdvertisedCompressionDictionary[] dictionaries)
    {
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                AdvertisedDictionaries = dictionaries,
            });
    }

    private static OperatorSuppliedCompressionDictionaryProvider DictionaryProvider(
        uint id, byte[] bytes) =>
        new(new Dictionary<uint, ReadOnlyMemory<byte>> { [id] = bytes });

    [Test]
    public async Task PumpOnceAsync_stamps_dictionary_id_when_peer_advertises_matching_fingerprint()
    {
        var bytes = new byte[] { 9, 8, 7, 6, 5 };
        var provider = DictionaryProvider(7u, bytes);
        var fingerprint = CompressionDictionaryFingerprint.Compute(bytes);
        var (grain, _, feed, transport, _, _, _) = Create(
            new LatticeReplicationOptions
            {
                ClusterId = LocalCluster,
                ShipCursorWriteInterval = 1,
                FramingCompression = LatticeCompression.ZstdDictionary,
                FramingCompressionDictionaryId = 7u,
                FramingCompressionMinBatchBytes = 0,
                DictionaryNegotiationEnabled = true,
            },
            dictionaryProvider: provider);
        AdvertiseDictionaries(transport, new AdvertisedCompressionDictionary(7u, fingerprint));

        // Tick 1: capability not yet captured -> conservative fallback.
        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);

        // Tick 2: the peer advertised id 7 with a matching fingerprint, so the
        // shipper compresses with the negotiated dictionary.
        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.ZstdDictionary));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(7u));
        });
    }

    [Test]
    public async Task PumpOnceAsync_falls_back_when_peer_advertises_same_id_different_fingerprint()
    {
        var bytes = new byte[] { 9, 8, 7, 6, 5 };
        var provider = DictionaryProvider(7u, bytes);
        var senderFingerprint = CompressionDictionaryFingerprint.Compute(bytes);
        var negotiationState = new SharedDictionaryNegotiationState();
        var (grain, _, feed, transport, _, _, _) = Create(
            new LatticeReplicationOptions
            {
                ClusterId = LocalCluster,
                ShipCursorWriteInterval = 1,
                FramingCompression = LatticeCompression.ZstdDictionary,
                FramingCompressionDictionaryId = 7u,
                FramingCompressionMinBatchBytes = 0,
                DictionaryNegotiationEnabled = true,
            },
            dictionaryNegotiationState: negotiationState,
            dictionaryProvider: provider);
        // The peer maps id 7 to *different* bytes (a different fingerprint):
        // the guaranteed collision when two clusters each auto-train id 7.
        AdvertiseDictionaries(transport,
            new AdvertisedCompressionDictionary(7u, senderFingerprint ^ 0xFFFFFFFFFFFFFFFFUL));

        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);

        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);

        // The fingerprints disagree, so the shipper must fall back to
        // dictionary-less Zstd rather than ship a frame the peer would
        // hard-fail to decode, and surface the distinct mismatch signal.
        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.Zstd));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(0u));
            var snap = negotiationState.Snapshot().Single();
            Assert.That(snap.FellBack, Is.True);
            Assert.That(snap.FingerprintMismatch, Is.True);
        });
    }

    private sealed class FakeActiveDictionaryProvider
        : ILatticeCompressionDictionaryProvider, ILatticeActiveCompressionDictionary, ILatticeCompressionDictionarySink
    {
        private readonly Dictionary<uint, byte[]> _dictionaries = new();

        public FakeActiveDictionaryProvider(uint activeId, byte[]? activeBytes = null)
        {
            ActiveDictionaryId = activeId;
            if (activeId != 0u && activeBytes is not null)
            {
                _dictionaries[activeId] = activeBytes;
            }
        }

        public uint ActiveDictionaryId { get; }

        public bool TryGetDictionary(uint dictionaryId, out ReadOnlyMemory<byte> dictionary)
        {
            if (dictionaryId != 0u && _dictionaries.TryGetValue(dictionaryId, out var bytes))
            {
                dictionary = bytes;
                return true;
            }

            dictionary = ReadOnlyMemory<byte>.Empty;
            return false;
        }

        public bool TryInstall(uint dictionaryId, ReadOnlyMemory<byte> dictionary)
        {
            if (dictionaryId == 0u || dictionary.IsEmpty)
            {
                return false;
            }

            _dictionaries[dictionaryId] = dictionary.ToArray();
            return true;
        }
    }

    private sealed class FakePullDictionaryTransport(
        IReadOnlyDictionary<uint, byte[]> served) : IReplicationDigestProbeTransport
    {
        public Task<DigestProbeResponse> ProbeDigestAsync(
            string targetClusterId, DigestProbeRequest request, CancellationToken cancellationToken)
            => Task.FromResult(default(DigestProbeResponse));

        public Task<CompressionDictionaryPullResponse> PullCompressionDictionaryAsync(
            string targetClusterId,
            CompressionDictionaryPullRequest request,
            CancellationToken cancellationToken)
        {
            if (served.TryGetValue(request.DictionaryId, out var bytes))
            {
                return Task.FromResult(new CompressionDictionaryPullResponse
                {
                    ExchangeSupported = true,
                    Found = true,
                    DictionaryId = request.DictionaryId,
                    Fingerprint = CompressionDictionaryFingerprint.Compute(bytes),
                    Dictionary = bytes,
                });
            }

            return Task.FromResult(CompressionDictionaryPullResponse.NotHeld);
        }
    }

    [Test]
    public async Task PumpOnceAsync_auto_active_forces_dictionary_framing_and_negotiates_active_id()
    {
        // Auto-shared-dictionary on with an active trained id overrides both the
        // configured framing (plain Zstd here) and the negotiation switch (off
        // here): the ship path frames with ZstdDictionary and negotiates the
        // provider's live active id once the peer advertises it.
        var bytes = new byte[] { 4, 4, 2, 2, 1 };
        var provider = new FakeActiveDictionaryProvider(5u, bytes);
        var fingerprint = CompressionDictionaryFingerprint.Compute(bytes);
        var (grain, _, feed, transport, _, _, _) = Create(
            new LatticeReplicationOptions
            {
                ClusterId = LocalCluster,
                ShipCursorWriteInterval = 1,
                FramingCompression = LatticeCompression.Zstd,
                FramingCompressionMinBatchBytes = 0,
                DictionaryNegotiationEnabled = false,
                AutoSharedDictionaryEnabled = true,
            },
            dictionaryProvider: provider);
        AdvertiseDictionaries(transport, new AdvertisedCompressionDictionary(5u, fingerprint));

        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);

        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.ZstdDictionary));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(5u));
        });
    }

    [Test]
    public async Task PumpOnceAsync_off_path_ignores_active_provider_and_stays_byte_identical()
    {
        // With the auto switch off, the presence of an auto-trainer with an
        // active id must not change the wire: plain Zstd, dictionary id 0,
        // byte-identical to the pre-feature build.
        var provider = new FakeActiveDictionaryProvider(5u, new byte[] { 1, 2, 3 });
        var (grain, _, feed, transport, _, _, _) = Create(
            new LatticeReplicationOptions
            {
                ClusterId = LocalCluster,
                ShipCursorWriteInterval = 1,
                FramingCompression = LatticeCompression.Zstd,
                FramingCompressionMinBatchBytes = 0,
                DictionaryNegotiationEnabled = false,
                AutoSharedDictionaryEnabled = false,
            },
            dictionaryProvider: provider);
        AdvertiseDictionaries(transport,
            new AdvertisedCompressionDictionary(5u, CompressionDictionaryFingerprint.Compute(new byte[] { 1, 2, 3 })));

        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);

        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.Zstd));
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(0u));
        });
    }

    [Test]
    public async Task PumpOnceAsync_auto_active_converges_onto_an_unheld_advertised_dictionary()
    {
        // The peer advertises a dictionary id the local provider does not hold.
        // With the auto switch on, the ship path pulls the bytes over the
        // digest-probe transport and installs them so the very next negotiation
        // can compress with the adopted dictionary.
        var bytes = new byte[] { 7, 7, 7, 1, 2, 3 };
        var fingerprint = CompressionDictionaryFingerprint.Compute(bytes);
        var provider = new FakeActiveDictionaryProvider(0u);
        var pullTransport = new FakePullDictionaryTransport(
            new Dictionary<uint, byte[]> { [9u] = bytes });
        var (grain, _, feed, transport, _, _, _) = Create(
            new LatticeReplicationOptions
            {
                ClusterId = LocalCluster,
                ShipCursorWriteInterval = 1,
                FramingCompression = LatticeCompression.Zstd,
                FramingCompressionMinBatchBytes = 0,
                AutoSharedDictionaryEnabled = true,
            },
            digestProbeTransport: pullTransport,
            dictionaryProvider: provider);
        AdvertiseDictionaries(transport, new AdvertisedCompressionDictionary(9u, fingerprint));

        // Tick 1 captures the peer advertisement; tick 2 converges on it.
        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);

        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(provider.TryGetDictionary(9u, out var stored), Is.True);
        Assert.That(stored.ToArray(), Is.EqualTo(bytes));
    }
}
