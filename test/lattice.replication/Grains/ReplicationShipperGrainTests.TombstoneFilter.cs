using Orleans.Lattice.BPlusTree.Grains;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Producer-side filter tests for tombstone-reap envelopes
/// (<see cref="MutationKind.Tombstone"/>) flowing through
/// <c>ReplicationShipperGrain.ShouldShip</c>.
///
/// Tombstone-reap envelopes are emitted by
/// <c>BPlusLeafGrain.CompactTombstonesAsync</c> to durably record a
/// local structural cleanup. They have no defined receiver-side
/// semantics: every peer cluster runs its own compaction pass against
/// its own copy of the data and reaps independently. The shipper must
/// drop them at the producer boundary so the receiver's apply path
/// never observes one.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private static WalRecord MakeTombstoneReapEntry(
        string key = "k1",
        string origin = LocalCluster,
        long ticks = 1,
        int counter = 0) => new()
        {
            TreeId = Tree,
            Op = MutationKind.Tombstone,
            Key = key,
            Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = counter },
            IsTombstone = true,
            OriginClusterId = origin,
            ShardIndex = 0,
        };

    [Test]
    public async Task PumpOnceAsync_does_not_ship_tombstone_reap_envelope()
    {
        // A tombstone-reap envelope is a local structural cleanup record;
        // shipping it would (a) generate apply-side dead-letters on the
        // receiver (no Tombstone apply rule), (b) pollute the per-origin
        // HWM, and (c) waste wire bandwidth. The shipper must drop it.
        var (grain, _, feed, transport, _, _, _) = Create();
        feed.Append(MakeTombstoneReapEntry(ticks: 5));

        await grain.PumpForTestingAsync(CancellationToken.None);

        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PumpOnceAsync_ships_user_entries_but_skips_interleaved_tombstone_reap()
    {
        // Mixed batch: a user Set and a tombstone-reap envelope sharing
        // the same partition stream. The shipper must apply the filter
        // per-entry, not per-batch - the user Set ships through, the
        // tombstone-reap is dropped, and the resulting batch carries
        // only the user-authored mutation.
        var (grain, _, feed, transport, encoder, _, _) = Create();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
        feed.Append(MakeEntry("user/k1", ticks: 5));
        feed.Append(MakeTombstoneReapEntry(key: "user/k2", ticks: 7));

        await grain.PumpForTestingAsync(CancellationToken.None);

        await transport.Received(1).SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(1),
            "tombstone-reap envelope must be filtered out at the shipper boundary - "
            + "only the user-authored Set must remain in the encoded batch");
    }

    [Test]
    public async Task PumpOnceAsync_does_not_ship_tombstone_reap_even_when_KeyFilter_admits_key()
    {
        // The tombstone-reap filter runs independently of the user
        // KeyFilter / KeyPrefixes admission rules. Even when the
        // envelope's key would pass the user filter, the structural
        // classification (Op==Tombstone) must drop the entry.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            KeyFilter = _ => true,
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeTombstoneReapEntry(key: "anything-passes-the-filter", ticks: 5));

        await grain.PumpForTestingAsync(CancellationToken.None);

        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }
}

