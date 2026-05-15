using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the <c>LeafCacheGrain.RefreshAsync</c> moved-away
/// prune branch: cached keys whose virtual slot has been moved away
/// from the primary leaf's shard must be evicted on the next refresh
/// so the cache cannot serve stale reads after a shard split.
/// Subsequent reads for those keys must surface a
/// <see cref="StaleShardRoutingException"/> rather than returning
/// a silent <c>null</c>, so <see cref="LatticeGrain"/>'s retry loop
/// can invalidate its shard map and re-route to the new owner.
/// </summary>
public partial class LeafCacheGrainTests
{
    private const int MovedAwayVsc = 16;

    /// <summary>
    /// Finds a key string that hashes to the given virtual slot under
    /// <see cref="ShardMap.GetVirtualSlot"/> with <see cref="MovedAwayVsc"/>.
    /// </summary>
    private static string KeyForVirtualSlot(int targetSlot, string prefix = "k")
    {
        for (int i = 0; i < 100_000; i++)
        {
            var k = $"{prefix}{i}";
            if (ShardMap.GetVirtualSlot(k, MovedAwayVsc) == targetSlot)
                return k;
        }
        throw new InvalidOperationException($"Could not find a key for virtual slot {targetSlot}.");
    }

    private static StateDelta MovedAwayDelta(int[] movedSlots, int vsc) => new()
    {
        Entries = new Dictionary<string, LwwValue<byte[]>>(),
        Version = new VersionVector(),
        MovedAwaySlots = movedSlots,
        MovedAwayVsc = vsc
    };

    [Test]
    public async Task Cache_prunes_entries_whose_slot_is_in_MovedAwaySlots()
    {
        var (grain, leaf) = CreateGrain();

        // Pick three keys: two on a moved slot, one on a retained slot.
        var movedKey1 = KeyForVirtualSlot(2, "moved-a");
        var movedKey2 = KeyForVirtualSlot(2, "moved-b");
        var keptKey = KeyForVirtualSlot(3, "kept-");

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(
                (movedKey1, Encoding.UTF8.GetBytes("v1")),
                (movedKey2, Encoding.UTF8.GetBytes("v2")),
                (keptKey, Encoding.UTF8.GetBytes("v3"))));
        await grain.GetAsync(movedKey1); // triggers initial refresh

        // Now the primary reports that slot 2 has moved away.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(MovedAwayDelta(new[] { 2 }, MovedAwayVsc));
        await grain.GetAsync(keptKey); // triggers refresh with moved slots

        // Subsequent reads for moved-slot keys must surface a routing
        // exception so the LatticeGrain retry loop re-routes against
        // the new owner. The retained key still resolves locally.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());
        Assert.That(async () => await grain.GetAsync(movedKey1), Throws.TypeOf<StaleShardRoutingException>());
        Assert.That(async () => await grain.GetAsync(movedKey2), Throws.TypeOf<StaleShardRoutingException>());
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync(keptKey))!), Is.EqualTo("v3"));
    }

    [Test]
    public async Task Cache_moved_away_pruning_is_idempotent()
    {
        var (grain, leaf) = CreateGrain();

        var movedKey = KeyForVirtualSlot(5, "moved-");
        var keptKey = KeyForVirtualSlot(6, "kept-");

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(
                (movedKey, Encoding.UTF8.GetBytes("v1")),
                (keptKey, Encoding.UTF8.GetBytes("v2"))));
        await grain.GetAsync(movedKey);

        // Report the moved slot repeatedly; the second/third invocations
        // must remain no-ops for the already-pruned entries.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(MovedAwayDelta(new[] { 5 }, MovedAwayVsc));
        await grain.GetAsync(keptKey);
        await grain.GetAsync(keptKey);
        await grain.GetAsync(keptKey);

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());
        Assert.That(async () => await grain.GetAsync(movedKey), Throws.TypeOf<StaleShardRoutingException>());
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync(keptKey))!), Is.EqualTo("v2"));
    }

    [Test]
    public async Task Cache_does_not_prune_when_MovedAwaySlots_is_null_or_empty()
    {
        var (grain, leaf) = CreateGrain();

        var keyA = KeyForVirtualSlot(1, "a-");
        var keyB = KeyForVirtualSlot(2, "b-");

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(
                (keyA, Encoding.UTF8.GetBytes("va")),
                (keyB, Encoding.UTF8.GetBytes("vb"))));
        await grain.GetAsync(keyA);

        // Empty MovedAwaySlots array must not prune anything.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(MovedAwayDelta(Array.Empty<int>(), MovedAwayVsc));
        await grain.GetAsync(keyA);

        // Null MovedAwaySlots / MovedAwayVsc must not prune anything either.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync(keyA))!), Is.EqualTo("va"));
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync(keyB))!), Is.EqualTo("vb"));
    }

    [Test]
    public async Task Cache_prune_multiple_moved_slots_only_evicts_matching_keys()
    {
        var (grain, leaf) = CreateGrain();

        var movedA = KeyForVirtualSlot(0, "ma-");
        var movedB = KeyForVirtualSlot(4, "mb-");
        var kept = KeyForVirtualSlot(7, "ke-");

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(
                (movedA, Encoding.UTF8.GetBytes("a")),
                (movedB, Encoding.UTF8.GetBytes("b")),
                (kept, Encoding.UTF8.GetBytes("c"))));
        await grain.GetAsync(movedA);

        // Move slots 0 and 4 away in a single refresh. The prune step
        // walks the cache once with BinarySearch on a sorted array, so
        // pass slots in sorted order.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(MovedAwayDelta(new[] { 0, 4 }, MovedAwayVsc));
        await grain.GetAsync(kept);

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());
        Assert.That(async () => await grain.GetAsync(movedA), Throws.TypeOf<StaleShardRoutingException>());
        Assert.That(async () => await grain.GetAsync(movedB), Throws.TypeOf<StaleShardRoutingException>());
        Assert.That(Encoding.UTF8.GetString((await grain.GetAsync(kept))!), Is.EqualTo("c"));
    }

    /// <summary>
    /// Regression: when a refresh has marked the primary leaf's slots as
    /// moved-away, the cache must throw <see cref="StaleShardRoutingException"/>
    /// on read for any key hashing into a moved slot. Silently returning
    /// <c>null</c> / an empty entry for the key is incorrect during the
    /// reshard window that lies between the leaf's
    /// <c>MarkSlotsMovedAwayAsync</c> call (driven from
    /// <c>ShardSplitPhase.Drain</c>) and the shard-root's own moved-away
    /// read-gate activation (which only fires from
    /// <c>ShardSplitPhase.Swap</c> onward). In that window the
    /// shard-root admits the read at the front door, the cache prunes its
    /// entry from <c>_cache</c> on refresh, and a silent drop hides the
    /// key from the caller for the duration of the drain. The fix
    /// surfaces the moved-away condition as a routing exception so
    /// <c>LatticeGrain.GetManyAsync</c>'s retry loop invalidates its
    /// shard map and re-routes against the new owner.
    /// </summary>
    [Test]
    public async Task GetManyAsync_throws_StaleShardRouting_for_keys_hashing_into_moved_slot()
    {
        var (grain, leaf) = CreateGrain();

        var movedKey = KeyForVirtualSlot(2, "mv-");
        var keptKey = KeyForVirtualSlot(3, "kp-");

        // Seed the cache with both keys, then publish a moved-away delta
        // covering the moved key's slot.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(
                (movedKey, Encoding.UTF8.GetBytes("v-moved")),
                (keptKey, Encoding.UTF8.GetBytes("v-kept"))));
        await grain.GetAsync(keptKey);

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(MovedAwayDelta(new[] { 2 }, MovedAwayVsc));
        await grain.GetAsync(keptKey); // refresh consumes the moved-away set

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());

        // Single-key request: must throw.
        Assert.That(async () => await grain.GetManyAsync(new List<string> { movedKey }),
            Throws.TypeOf<StaleShardRoutingException>(),
            "GetManyAsync must throw StaleShardRoutingException for a single moved-away key.");

        // Mixed request: must throw on the moved-away key regardless of
        // whether other keys would have been serviceable.
        Assert.That(async () => await grain.GetManyAsync(new List<string> { movedKey, keptKey }),
            Throws.TypeOf<StaleShardRoutingException>(),
            "GetManyAsync must throw StaleShardRoutingException when any requested key hashes to a moved-away slot.");

        // Sanity: a request that touches only the kept key still succeeds.
        var keptOnly = await grain.GetManyAsync(new List<string> { keptKey });
        Assert.That(keptOnly.TryGetValue(keptKey, out var keptBytes), Is.True);
        Assert.That(Encoding.UTF8.GetString(keptBytes!), Is.EqualTo("v-kept"));
    }

    /// <summary>
    /// Symmetric regression: single-key <see cref="ILeafCacheGrain.GetAsync"/>
    /// must throw <see cref="StaleShardRoutingException"/> on a moved-away
    /// slot for the same reason as <see cref="ILeafCacheGrain.GetManyAsync"/>:
    /// silently returning <c>null</c> during the drain window hides keys that
    /// are still authoritatively reachable through the new owner.
    /// </summary>
    [Test]
    public async Task GetAsync_throws_StaleShardRouting_for_key_hashing_into_moved_slot()
    {
        var (grain, leaf) = CreateGrain();

        var movedKey = KeyForVirtualSlot(2, "mv-");

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith((movedKey, Encoding.UTF8.GetBytes("v-moved"))));
        await grain.GetAsync(movedKey);

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(MovedAwayDelta(new[] { 2 }, MovedAwayVsc));

        Assert.That(async () => await grain.GetAsync(movedKey),
            Throws.TypeOf<StaleShardRoutingException>());
    }

    /// <summary>
    /// Symmetric regression: <see cref="ILeafCacheGrain.ExistsAsync"/> must
    /// throw <see cref="StaleShardRoutingException"/> on a moved-away slot
    /// rather than returning <c>false</c>, which would mis-signal the key
    /// as absent during a drain window.
    /// </summary>
    [Test]
    public async Task ExistsAsync_throws_StaleShardRouting_for_key_hashing_into_moved_slot()
    {
        var (grain, leaf) = CreateGrain();

        var movedKey = KeyForVirtualSlot(2, "mv-");

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith((movedKey, Encoding.UTF8.GetBytes("v-moved"))));
        await grain.GetAsync(movedKey);

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(MovedAwayDelta(new[] { 2 }, MovedAwayVsc));

        Assert.That(async () => await grain.ExistsAsync(movedKey),
            Throws.TypeOf<StaleShardRoutingException>());
    }
}
