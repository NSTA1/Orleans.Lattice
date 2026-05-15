using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the retroactive-sweep leaf-side primitive
/// <see cref="IBPlusLeafGrain.GetPendingMutationsForSlotsAsync"/>. The
/// sweep returns a per-(txid, key) snapshot of every prepared mutation
/// whose key hashes into one of the supplied moved virtual slots; the
/// shard-split coordinator invokes it at the entry of the
/// <c>BeginShadowWrite</c> phase to retroactively mirror in-flight
/// prepared writes onto the destination shard before the drain begins.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearPendingMutationSnapshotAmbientContext()
    {
        // Every test on this logical thread must start with a clean
        // transaction-context / origin-context slate so the
        // RequestContext-backed ambients from a prior test cannot leak
        // into the assertions below.
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeOriginContext.Current = null;
    }

    [Test]
    public async Task GetPendingMutationsForSlotsAsync_returns_empty_when_no_pending_bucket()
    {
        var grain = CreateGrain();
        var slots = new[] { 0, 1, 2, 3 };

        var snapshots = await grain.GetPendingMutationsForSlotsAsync(slots, 4096);

        Assert.That(snapshots, Is.Empty);
    }

    [Test]
    public async Task GetPendingMutationsForSlotsAsync_returns_empty_when_moved_slots_empty()
    {
        var grain = CreateGrain();
        var txid = Guid.NewGuid();

        // Populate _pendingTx via a saga prepare write.
        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await grain.SetAsync("k", [1, 2, 3]);
        }
        LatticeTransactionContext.Set(Guid.Empty);

        var snapshots = await grain.GetPendingMutationsForSlotsAsync(Array.Empty<int>(), 4096);

        Assert.That(snapshots, Is.Empty,
            "empty moved-slots array must short-circuit even when prepares are present");
    }

    [Test]
    public async Task GetPendingMutationsForSlotsAsync_includes_keys_in_moved_slots()
    {
        const int virtualShardCount = 4096;
        var grain = CreateGrain();
        var txid = Guid.NewGuid();

        // Prepare three keys; compute which virtual slot each one
        // hashes into so the test asserts deterministically against
        // the canonical slot routing.
        var keys = new[] { "alpha", "beta", "gamma" };
        var slotsByKey = keys.ToDictionary(k => k, k => ShardMap.GetVirtualSlot(k, virtualShardCount));

        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            foreach (var k in keys)
                await grain.SetAsync(k, [(byte)k[0]]);
        }
        LatticeTransactionContext.Set(Guid.Empty);

        // Request the sweep against the slot owned by "alpha" only.
        var alphaSlot = slotsByKey["alpha"];
        var sortedSlots = new[] { alphaSlot };

        var snapshots = await grain.GetPendingMutationsForSlotsAsync(sortedSlots, virtualShardCount);

        // Only the prepares whose key hashes into the requested slot
        // are returned. Adjacent keys on the same leaf but in other
        // slots are filtered out at the source.
        Assert.That(snapshots, Has.Count.GreaterThanOrEqualTo(1),
            "must include the key whose slot was requested");
        Assert.That(snapshots.All(s => slotsByKey[s.Key] == alphaSlot), Is.True,
            "every returned snapshot's key must hash into one of the requested slots");
    }

    [Test]
    public async Task GetPendingMutationsForSlotsAsync_excludes_keys_outside_moved_slots()
    {
        const int virtualShardCount = 4096;
        var grain = CreateGrain();
        var txid = Guid.NewGuid();

        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await grain.SetAsync("k", [1]);
        }
        LatticeTransactionContext.Set(Guid.Empty);

        var slot = ShardMap.GetVirtualSlot("k", virtualShardCount);

        // Build a moved-slots array that does NOT contain "k"'s slot
        // - every other slot in a small contiguous range.
        var otherSlots = Enumerable.Range(0, virtualShardCount).Where(s => s != slot).Take(8).ToArray();
        Array.Sort(otherSlots);

        var snapshots = await grain.GetPendingMutationsForSlotsAsync(otherSlots, virtualShardCount);

        Assert.That(snapshots, Is.Empty,
            "snapshot must filter out keys whose slot is not in the requested set");
    }

    [Test]
    public async Task GetPendingMutationsForSlotsAsync_returns_one_snapshot_per_txid_and_key()
    {
        const int virtualShardCount = 4096;
        var grain = CreateGrain();
        var txidA = Guid.NewGuid();
        var txidB = Guid.NewGuid();

        // Two distinct sagas prepare different keys on the same leaf.
        LatticeTransactionContext.Set(txidA);
        using (LatticePreparedContext.BeginScope())
        {
            await grain.SetAsync("alpha", [1]);
        }
        LatticeTransactionContext.Set(txidB);
        using (LatticePreparedContext.BeginScope())
        {
            await grain.SetAsync("beta", [2]);
        }
        LatticeTransactionContext.Set(Guid.Empty);

        // Request every slot in the virtual space (effectively unfiltered).
        var alphaSlot = ShardMap.GetVirtualSlot("alpha", virtualShardCount);
        var betaSlot = ShardMap.GetVirtualSlot("beta", virtualShardCount);
        var sortedSlots = new[] { alphaSlot, betaSlot };
        Array.Sort(sortedSlots);

        var snapshots = await grain.GetPendingMutationsForSlotsAsync(sortedSlots, virtualShardCount);

        // Two snapshots - one per (txid, key) - with the correct txid
        // attribution on each.
        Assert.That(snapshots, Has.Count.EqualTo(2));
        Assert.That(snapshots.Single(s => s.Key == "alpha").TransactionId, Is.EqualTo(txidA));
        Assert.That(snapshots.Single(s => s.Key == "beta").TransactionId, Is.EqualTo(txidB));
    }

    [Test]
    public async Task GetPendingMutationsForSlotsAsync_preserves_metadata_end_to_end()
    {
        const int virtualShardCount = 4096;
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        const string origin = "cluster-east";

        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        using (LatticeOriginContext.With(origin))
        {
            await grain.SetAsync("k", [1, 2, 3]);
        }
        LatticeTransactionContext.Set(Guid.Empty);

        var slot = ShardMap.GetVirtualSlot("k", virtualShardCount);
        var snapshots = await grain.GetPendingMutationsForSlotsAsync(new[] { slot }, virtualShardCount);

        Assert.That(snapshots, Has.Count.EqualTo(1));
        var s = snapshots[0];
        Assert.That(s.Key, Is.EqualTo("k"));
        Assert.That(s.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(s.IsTombstone, Is.False);
        Assert.That(s.TransactionId, Is.EqualTo(txid));
        Assert.That(s.OriginClusterId, Is.EqualTo(origin),
            "OriginClusterId must be preserved verbatim from the prepared LwwValue");
        Assert.That(s.Timestamp > HybridLogicalClock.Zero, Is.True,
            "Timestamp must be the prepare-time HLC tick, not the zero default");
        Assert.That(s.ExpiresAtTicks, Is.EqualTo(0L),
            "non-TTL prepares carry ExpiresAtTicks = 0");
    }

    [Test]
    public async Task GetPendingMutationsForSlotsAsync_preserves_expiresAtTicks_for_ttl_prepares()
    {
        const int virtualShardCount = 4096;
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        // IBPlusLeafGrain.SetAsync(string, byte[], long expiresAtTicks)
        // takes absolute UTC ticks (resolved on the silo handling the
        // call by the silo) rather than a relative TimeSpan - the latter
        // lives on the public ILattice extension surface, not on the
        // internal grain interface. Compute the absolute tick value
        // up-front so the test exercises the grain-level overload.
        var expiresAtTicks = DateTimeOffset.UtcNow.AddHours(1).UtcTicks;

        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await grain.SetAsync("k", [1], expiresAtTicks);
        }
        LatticeTransactionContext.Set(Guid.Empty);

        var slot = ShardMap.GetVirtualSlot("k", virtualShardCount);
        var snapshots = await grain.GetPendingMutationsForSlotsAsync(new[] { slot }, virtualShardCount);

        Assert.That(snapshots, Has.Count.EqualTo(1));
        Assert.That(snapshots[0].ExpiresAtTicks, Is.EqualTo(expiresAtTicks),
            "TTL prepares must carry the verbatim ExpiresAtTicks so the destination re-stamps it correctly");
    }

    [Test]
    public async Task GetPendingMutationsForSlotsAsync_preserves_tombstone_prepares()
    {
        const int virtualShardCount = 4096;
        var grain = CreateGrain();
        var txid = Guid.NewGuid();

        // Seed a live value first, then prepare a delete against it
        // under the saga's transaction context.
        await grain.SetAsync("k", [9]);

        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await grain.DeleteAsync("k");
        }
        LatticeTransactionContext.Set(Guid.Empty);

        var slot = ShardMap.GetVirtualSlot("k", virtualShardCount);
        var snapshots = await grain.GetPendingMutationsForSlotsAsync(new[] { slot }, virtualShardCount);

        Assert.That(snapshots, Has.Count.EqualTo(1));
        var s = snapshots[0];
        Assert.That(s.IsTombstone, Is.True,
            "prepared delete must surface as IsTombstone = true");
        Assert.That(s.Value, Is.Null,
            "tombstone snapshots null out Value so the destination's DeleteAsync path is taken");
        Assert.That(s.TransactionId, Is.EqualTo(txid));
    }

    [Test]
    public void GetPendingMutationsForSlotsAsync_throws_on_null_slot_array()
    {
        var grain = CreateGrain();

        Assert.ThrowsAsync<ArgumentNullException>(
            async () => await grain.GetPendingMutationsForSlotsAsync(null!, 4096));
    }

    [Test]
    public void GetPendingMutationsForSlotsAsync_throws_on_non_positive_virtualShardCount()
    {
        var grain = CreateGrain();

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await grain.GetPendingMutationsForSlotsAsync(Array.Empty<int>(), 0));
        Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            async () => await grain.GetPendingMutationsForSlotsAsync(Array.Empty<int>(), -1));
    }
}
