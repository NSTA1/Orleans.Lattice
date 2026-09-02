using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the <c>LeafCacheGrain.RefreshAsync</c> seal-lift branch: the
/// inverse of the moved-away prune, reached when an online shard
/// consolidation folds virtual slots back onto the primary leaf's shard.
/// <para>
/// The cache records a moved-away slot set so a stale-routed read surfaces
/// <see cref="StaleShardRoutingException"/> instead of a silent
/// <c>null</c>. That is right while the slot really has moved away, and
/// catastrophic once it has been folded back: the routing map would send the
/// reader to this shard and the cache would send it straight back to the
/// retired one, leaving the reclaimed keys permanently unreachable. The lift
/// signal is a delta that still carries the leaf's slot-space stamp but no
/// sealed slot - a shape only
/// <c>BPlusLeafGrain.UnmarkSlotsMovedAwayAsync</c> produces.
/// </para>
/// </summary>
public partial class LeafCacheGrainTests
{
    private static StateDelta SealLiftDelta(int vsc, params (string Key, byte[] Value)[] entries)
    {
        var map = new Dictionary<string, LwwValue<byte[]>>(entries.Length);
        var hlc = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.UtcTicks, Counter = 0 };
        foreach (var (key, value) in entries)
            map[key] = LwwValue<byte[]>.Create(value, hlc);

        return new StateDelta
        {
            Entries = map,
            Version = new VersionVector(),
            MovedAwaySlots = null,
            MovedAwayVsc = vsc,
        };
    }

    [Test]
    public async Task Cache_drops_its_moved_away_seal_when_the_primary_reports_the_slot_reclaimed()
    {
        var (grain, leaf) = CreateGrain();

        var foldedKey = KeyForVirtualSlot(2, "folded-");
        var keptKey = KeyForVirtualSlot(3, "kept-");

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(DeltaWith(
                (foldedKey, Encoding.UTF8.GetBytes("v1")),
                (keptKey, Encoding.UTF8.GetBytes("v2"))));
        await grain.GetAsync(foldedKey);

        // Slot 2 splits away: the cache seals it.
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MovedAwayDelta(new[] { 2 }, MovedAwayVsc));
        await grain.GetAsync(keptKey);
        Assert.That(async () => await grain.GetAsync(foldedKey),
            Throws.TypeOf<StaleShardRoutingException>(),
            "Precondition: the cache refuses the moved-away slot.");

        // A consolidation folds slot 2 back. The primary reports its stamp
        // with no sealed slot, and re-delivers the reclaimed key.
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(SealLiftDelta(MovedAwayVsc, (foldedKey, Encoding.UTF8.GetBytes("v1"))));
        await grain.GetAsync(keptKey);

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());
        var value = await grain.GetAsync(foldedKey);

        Assert.That(value, Is.Not.Null,
            "After the fold the cache must serve the reclaimed key instead of refusing it, "
            + "or the reader ping-pongs between the routing map and the cache forever.");
        Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo("v1"));
    }

    [Test]
    public async Task Cache_keeps_its_moved_away_seal_on_an_ordinary_refresh_that_carries_no_stamp()
    {
        // The lift branch must key off the positive stamp signal, not merely
        // off an absent slot set: an ordinary steady-state delta carries
        // neither, and must leave a live seal exactly where it is.
        var (grain, leaf) = CreateGrain();

        var movedKey = KeyForVirtualSlot(2, "still-moved-");
        var keptKey = KeyForVirtualSlot(3, "still-kept-");

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(DeltaWith(
                (movedKey, Encoding.UTF8.GetBytes("v1")),
                (keptKey, Encoding.UTF8.GetBytes("v2"))));
        await grain.GetAsync(movedKey);

        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MovedAwayDelta(new[] { 2 }, MovedAwayVsc));
        await grain.GetAsync(keptKey);

        // Several ordinary refreshes carrying no moved-away metadata at all.
        leaf.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>()).Returns(EmptyDelta());
        await grain.GetAsync(keptKey);
        await grain.GetAsync(keptKey);

        Assert.That(async () => await grain.GetAsync(movedKey),
            Throws.TypeOf<StaleShardRoutingException>(),
            "An ordinary refresh must never be mistaken for a seal lift.");
    }
}
