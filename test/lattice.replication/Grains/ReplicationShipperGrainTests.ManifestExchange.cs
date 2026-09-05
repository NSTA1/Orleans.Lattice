using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage for two early-exit legs of <c>TryElideViaManifestExchangeAsync</c>:
/// a drained batch with no value-carrying point-Set entries (an all-Delete
/// batch) whose empty manifest skips the exchange round trip entirely, and an
/// exchange RPC that throws, which is swallowed so the full batch still ships
/// verbatim rather than stalling the stream.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private static WalRecord MakeDelete(string key, int ticks) => new()
    {
        TreeId = Tree,
        Op = MutationKind.Delete,
        Key = key,
        Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = 0 },
        OriginClusterId = LocalCluster,
    };

    [Test]
    public async Task TryElideViaManifestExchangeAsync_empty_manifest_skips_exchange_and_ships_verbatim()
    {
        // An all-Delete batch produces an empty content manifest (only
        // value-carrying point Sets are manifested), so the shipper must skip
        // the exchange round trip and ship the deletes verbatim.
        var opts = ElisionPipelinedOptions(shipMaxInFlight: 1, shipBatchSize: 10);
        var fake = ManifestTransportHolding("d1", "d2");
        var (grain, _, feed, transport, _, _, _) = Create(opts, digestProbeTransport: fake);
        feed.Append(MakeDelete("d1", ticks: 1));
        feed.Append(MakeDelete("d2", ticks: 2));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(fake.ExchangeCalls, Is.Zero,
                "an empty manifest must not pay for a manifest exchange round trip");
            Assert.That(SendAsyncCallCount(transport), Is.EqualTo(1),
                "the delete batch must ship verbatim");
            Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
                "no entry may be elided when the manifest is empty");
        });
    }

    [Test]
    public async Task TryElideViaManifestExchangeAsync_exchange_throw_is_swallowed_and_ships_full_batch()
    {
        // The exchange RPC throws; the shipper must swallow it, skip elision,
        // and ship the full batch verbatim (SendAsync handles a real transport
        // fault separately).
        var opts = ElisionPipelinedOptions(shipMaxInFlight: 1, shipBatchSize: 10);
        var fake = new FakeManifestExchangeTransport(
            _ => throw new InvalidOperationException("exchange-boom"));
        var (grain, state, feed, transport, _, _, _) = Create(opts, digestProbeTransport: fake);
        feed.Append(MakeEntryWithValue("k1", new byte[] { 1 }, ticks: 1));
        feed.Append(MakeEntryWithValue("k2", new byte[] { 2 }, ticks: 2));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(fake.ExchangeCalls, Is.EqualTo(1),
                "the shipper must have attempted the exchange before it threw");
            Assert.That(SendAsyncCallCount(transport), Is.EqualTo(1),
                "a failed exchange must not stop the batch from shipping verbatim");
            Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
                "no entry may be elided when the exchange failed");
            Assert.That(state.State.Cursor,
                Is.EqualTo(new HybridLogicalClock { WallClockTicks = 2, Counter = 0 }),
                "the cursor must advance past the verbatim-shipped batch");
        });
    }
}
