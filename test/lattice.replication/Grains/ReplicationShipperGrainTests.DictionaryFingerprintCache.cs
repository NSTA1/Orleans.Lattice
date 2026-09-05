namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage for the per-id memoisation fast-path in
/// <c>ResolveConfiguredDictionaryFingerprint</c>: once the sender has resolved
/// and hashed its configured dictionary bytes for an id, a later negotiation for
/// the same id must reuse the cached fingerprint instead of re-resolving. Driven
/// by three ticks where the peer's advertised capability set changes between the
/// second and third tick (bumping the negotiation epoch) while the configured
/// dictionary id stays constant, so negotiation re-runs and takes the cache hit.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    [Test]
    public async Task ResolveConfiguredDictionaryFingerprint_reuses_cache_on_second_negotiation_for_same_id()
    {
        var bytes = new byte[] { 9, 8, 7, 6, 5 };
        var provider = DictionaryProvider(7u, bytes);
        var fp7 = CompressionDictionaryFingerprint.Compute(bytes);
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

        // Tick 1: peer advertises id 7 with the matching fingerprint. This is
        // captured for the tick-2 negotiation.
        AdvertiseDictionaries(transport, new AdvertisedCompressionDictionary(7u, fp7));
        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.PumpForTestingAsync(CancellationToken.None);

        // Change the advertised SET (add id 8) so the tick-2 ship captures a
        // changed capability, bumping the negotiation epoch so tick 3 re-runs
        // negotiation for the still-configured id 7.
        AdvertiseDictionaries(
            transport,
            new AdvertisedCompressionDictionary(7u, fp7),
            new AdvertisedCompressionDictionary(8u, fp7 ^ 0x1UL));

        // Tick 2: negotiation resolves the configured fingerprint for id 7 for
        // the first time (resolve + cache) and stamps the negotiated id.
        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.PumpForTestingAsync(CancellationToken.None);
        Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(7u),
            "tick 2 must negotiate the matching dictionary id 7");

        // Tick 3: the changed capability bumped the epoch, so negotiation runs
        // again and ResolveConfiguredDictionaryFingerprint(7) returns the cached
        // fingerprint - which must still negotiate dictionary id 7.
        feed.Append(MakeEntry("k3", ticks: 3));
        await grain.PumpForTestingAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(CapturedHeaderCompression(transport), Is.EqualTo(LatticeCompression.ZstdDictionary),
                "the cached configured fingerprint must still frame with the shared dictionary on tick 3");
            Assert.That(CapturedHeaderDictionaryId(transport), Is.EqualTo(7u),
                "the cached configured fingerprint must still negotiate dictionary id 7 on tick 3");
        });
    }
}
