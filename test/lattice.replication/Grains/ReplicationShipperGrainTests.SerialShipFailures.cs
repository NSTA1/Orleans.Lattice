using System.IO;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage for the strict-serial ship path's failure and short-circuit legs
/// (<c>ShipMaxInFlight</c> collapses to a window of one): a mid-batch WAL read
/// throw that backs off and abandons the tick, a fully-elided serial batch that
/// advances the cursor without shipping an envelope, and the
/// <c>ShouldShip</c> null-key rejection under a configured key-prefix allow-list.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    [Test]
    public async Task PumpSerialOnceAsync_merge_read_throw_backs_off_and_abandons_tick()
    {
        // Window collapses to serial (ShipMaxInFlight unset). The tick primes
        // one page (read #1) then MergeOneBatchAsync refills mid-batch (read
        // #2, because ShipBatchSize 2 > ShipPartitionPageSize 1) - the refill
        // read throws, driving the serial loop's own drain catch (not the
        // InitializeDrainTick catch), which backs off and returns.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            ShipPartitionPageSize = 1,
            ShipBatchSize = 2,
        };
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeEntry("k1", ticks: 1));
        feed.Append(MakeEntry("k2", ticks: 2));
        feed.OnReadShipping = _ =>
        {
            if (feed.ReadCalls == 2)
            {
                throw new IOException("boom-mid-batch");
            }
            return Task.CompletedTask;
        };

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ConsecutiveFailures, Is.GreaterThan(0),
                "a mid-batch WAL read throw on the serial path must apply backoff");
            Assert.That(SendAsyncCallCount(transport), Is.Zero,
                "a drain failure must abandon the tick before any batch ships");
        });
    }

    [Test]
    public async Task PumpSerialOnceAsync_full_elision_advances_cursor_without_shipping()
    {
        // Serial window (ShipMaxInFlight 1) with elision on and a receiver that
        // holds every key: TryElideViaManifestExchangeAsync empties the drain
        // buffer, so ShipMergedSerialBatchAsync takes the full-elision leg -
        // advance the cursor past the drained range, reset the failure budget,
        // and return without shipping an envelope.
        var opts = ElisionPipelinedOptions(shipMaxInFlight: 1, shipBatchSize: 10);
        var fake = ManifestTransportHolding("e1", "e2");
        var (grain, state, feed, transport, _, _, _) = Create(opts, digestProbeTransport: fake);
        feed.Append(MakeEntryWithValue("e1", new byte[] { 1 }, ticks: 1));
        feed.Append(MakeEntryWithValue("e2", new byte[] { 2 }, ticks: 2));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(SendAsyncCallCount(transport), Is.Zero,
                "a fully-elided serial batch must not ship an envelope");
            Assert.That(fake.ExchangeCalls, Is.EqualTo(1),
                "the shipper must have performed exactly one manifest exchange");
            Assert.That(state.State.Cursor,
                Is.EqualTo(new HybridLogicalClock { WallClockTicks = 2, Counter = 0 }),
                "the cursor must advance past the full drained range even when everything was elided");
        });
    }

    [Test]
    public async Task ShouldShip_null_key_under_key_prefix_allow_list_is_filtered_out()
    {
        // A configured KeyPrefixes allow-list (and no KeyFilter) forces the
        // key-prefix leg of ShouldShip. A local-origin Set whose Key is null
        // hits the `entry.Key is null` guard and is rejected, while a
        // prefix-matching sibling ships - proving the null-key rejection is
        // selective, not a blanket drop.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            KeyPrefixes = new List<string> { "p" },
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        var nullKey = new WalRecord
        {
            TreeId = Tree,
            Op = MutationKind.Set,
            Value = new byte[] { 9 },
            Timestamp = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
            OriginClusterId = LocalCluster,
        };
        feed.Append(nullKey);
        feed.Append(MakeEntry("p-yes", ticks: 2));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(SendAsyncCallCount(transport), Is.EqualTo(1),
                "the null-key entry must be filtered while the prefix-matching entry ships");
            Assert.That(LastShippedEntryCount(transport), Is.EqualTo(1),
                "only the prefix-matching entry may appear in the shipped batch");
        });
    }
}
