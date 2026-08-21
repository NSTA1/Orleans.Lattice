using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the pending-transaction read paths shared by every
/// leaf reader (<see cref="IBPlusLeafGrain.GetManyAsync"/>,
/// <see cref="IBPlusLeafGrain.GetKeysAsync"/>,
/// <see cref="IBPlusLeafGrain.GetEntriesAsync"/>,
/// <see cref="IBPlusLeafGrain.GetLiveEntriesAsync"/>,
/// <see cref="IBPlusLeafGrain.GetLiveRawEntriesAsync"/>,
/// <see cref="IBPlusLeafGrain.CountAsync()"/>, and
/// <see cref="IBPlusLeafGrain.GetStatsAsync"/>).
///
/// A prepared saga write buckets a mutation into <c>_pendingTx</c>; the
/// reader resolves each pending tx's terminal status through the
/// registry. Here the resolution is pinned deterministically with the
/// per-scan <see cref="LatticeRegistrySnapshotContext"/> ambient so a
/// committed pending mutation is surfaced, an in-flight one falls
/// through to the committed cache row, and a committed tombstone hides
/// the key - without a live registry grain.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearPendingReadCoverageAmbientContext()
    {
        LatticeTransactionContext.Set(Guid.Empty);
        LatticeOriginContext.Current = null;
        LatticePredicateContext.Current = null;
    }

    private static async Task PreparePendingSetAsync(BPlusLeafGrain grain, Guid txid, string key, byte[] value)
    {
        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await grain.SetAsync(key, value);
        }
        LatticeTransactionContext.Set(Guid.Empty);
    }

    private static async Task PreparePendingDeleteAsync(BPlusLeafGrain grain, Guid txid, string key)
    {
        LatticeTransactionContext.Set(txid);
        using (LatticePreparedContext.BeginScope())
        {
            await grain.DeleteAsync(key);
        }
        LatticeTransactionContext.Set(Guid.Empty);
    }

    private static IDisposable Committed(Guid txid) =>
        LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus> { [txid] = TxStatus.Committed });

    private static IDisposable InFlight(Guid txid) =>
        LatticeRegistrySnapshotContext.BeginScope(new Dictionary<Guid, TxStatus> { [txid] = TxStatus.InFlight });

    // ---- GetManyAsync ----

    [Test]
    public async Task GetManyAsync_committed_pending_fresh_key_returns_value()
    {
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "fresh", [7, 7]);

        using (Committed(txid))
        {
            var result = await grain.GetManyAsync(["fresh"]);
            Assert.That(result.ContainsKey("fresh"), Is.True);
            Assert.That(result["fresh"], Is.EqualTo(new byte[] { 7, 7 }));
        }
    }

    [Test]
    public async Task GetManyAsync_committed_pending_overrides_committed_cache_row()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "k", [2, 2]);

        using (Committed(txid))
        {
            var result = await grain.GetManyAsync(["k"]);
            Assert.That(result["k"], Is.EqualTo(new byte[] { 2, 2 }));
        }
    }

    [Test]
    public async Task GetManyAsync_committed_pending_tombstone_hides_key()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingDeleteAsync(grain, txid, "k");

        using (Committed(txid))
        {
            var result = await grain.GetManyAsync(["k"]);
            Assert.That(result.ContainsKey("k"), Is.False);
        }
    }

    [Test]
    public async Task GetManyAsync_inflight_pending_falls_through_to_committed_cache()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "k", [9, 9]);

        using (InFlight(txid))
        {
            var result = await grain.GetManyAsync(["k"]);
            Assert.That(result["k"], Is.EqualTo(new byte[] { 1 }),
                "in-flight pending must not shadow the committed cache value");
        }
    }

    // ---- GetKeysAsync ----

    [Test]
    public async Task GetKeysAsync_committed_pending_includes_fresh_and_override_excludes_tombstone()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);
        await grain.SetAsync("b", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "c", [1]);   // fresh committed
        await PreparePendingDeleteAsync(grain, txid, "b");     // committed tombstone

        using (Committed(txid))
        {
            var keys = await grain.GetKeysAsync();
            Assert.That(keys, Does.Contain("a"));
            Assert.That(keys, Does.Contain("c"));
            Assert.That(keys, Does.Not.Contain("b"));
        }
    }

    [Test]
    public async Task GetKeysAsync_fresh_committed_pending_outside_end_bound_excluded()
    {
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "zzz", [1]);

        using (Committed(txid))
        {
            var keys = await grain.GetKeysAsync(endExclusive: "m");
            Assert.That(keys, Does.Not.Contain("zzz"));
        }
    }

    [Test]
    public async Task GetKeysAsync_fresh_committed_pending_before_start_bound_excluded()
    {
        var grain = CreateGrain();
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "aaa", [1]);

        using (Committed(txid))
        {
            var keys = await grain.GetKeysAsync(startInclusive: "m");
            Assert.That(keys, Does.Not.Contain("aaa"));
        }
    }

    // ---- GetEntriesAsync ----

    [Test]
    public async Task GetEntriesAsync_committed_pending_fresh_and_override()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "a", [5]);   // override
        await PreparePendingSetAsync(grain, txid, "z", [9]);   // fresh

        using (Committed(txid))
        {
            var entries = await grain.GetEntriesAsync();
            var map = entries.ToDictionary(e => e.Key, e => e.Value);
            Assert.That(map["a"], Is.EqualTo(new byte[] { 5 }));
            Assert.That(map["z"], Is.EqualTo(new byte[] { 9 }));
        }
    }

    [Test]
    public async Task GetEntriesAsync_committed_pending_tombstone_excluded()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingDeleteAsync(grain, txid, "a");

        using (Committed(txid))
        {
            var entries = await grain.GetEntriesAsync();
            Assert.That(entries.Any(e => e.Key == "a"), Is.False);
        }
    }

    // ---- GetLiveEntriesAsync / GetLiveRawEntriesAsync ----

    [Test]
    public async Task GetLiveEntriesAsync_committed_pending_fresh_and_override()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "a", [5]);
        await PreparePendingSetAsync(grain, txid, "z", [9]);

        using (Committed(txid))
        {
            var live = await grain.GetLiveEntriesAsync();
            Assert.That(live["a"], Is.EqualTo(new byte[] { 5 }));
            Assert.That(live["z"], Is.EqualTo(new byte[] { 9 }));
        }
    }

    [Test]
    public async Task GetLiveEntriesAsync_committed_pending_tombstone_excluded()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingDeleteAsync(grain, txid, "a");

        using (Committed(txid))
        {
            var live = await grain.GetLiveEntriesAsync();
            Assert.That(live.ContainsKey("a"), Is.False);
        }
    }

    [Test]
    public async Task GetLiveRawEntriesAsync_committed_pending_fresh_and_override()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "a", [5]);
        await PreparePendingSetAsync(grain, txid, "z", [9]);

        using (Committed(txid))
        {
            var raw = await grain.GetLiveRawEntriesAsync();
            var map = raw.ToDictionary(e => e.Key, e => e.Value);
            Assert.That(map["a"], Is.EqualTo(new byte[] { 5 }));
            Assert.That(map["z"], Is.EqualTo(new byte[] { 9 }));
        }
    }

    [Test]
    public async Task GetLiveRawEntriesAsync_committed_pending_tombstone_excluded()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingDeleteAsync(grain, txid, "a");

        using (Committed(txid))
        {
            var raw = await grain.GetLiveRawEntriesAsync();
            Assert.That(raw.Any(e => e.Key == "a"), Is.False);
        }
    }

    // ---- CountAsync ----

    [Test]
    public async Task CountAsync_committed_pending_counts_fresh_and_override_excludes_tombstone()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);
        await grain.SetAsync("b", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "c", [1]);   // fresh committed
        await PreparePendingDeleteAsync(grain, txid, "b");     // committed tombstone

        using (Committed(txid))
        {
            var count = await grain.CountAsync();
            Assert.That(count, Is.EqualTo(2), "a + c live; b tombstoned");
        }
    }

    [Test]
    public async Task CountAsync_fresh_committed_pending_outside_end_bound_excluded()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "zzz", [1]);

        using (Committed(txid))
        {
            var count = await grain.CountAsync("a", "m");
            Assert.That(count, Is.EqualTo(1), "only 'a' falls inside [a, m)");
        }
    }

    // ---- GetStatsAsync ----

    [Test]
    public async Task GetStatsAsync_committed_pending_live_and_tombstone_counts()
    {
        var grain = CreateGrain();
        await grain.SetAsync("a", [1]);
        var txid = Guid.NewGuid();
        await PreparePendingSetAsync(grain, txid, "z", [9]);   // fresh live
        await PreparePendingDeleteAsync(grain, txid, "a");     // committed tombstone

        using (Committed(txid))
        {
            var stats = await grain.GetStatsAsync();
            Assert.That(stats.LiveKeys, Is.EqualTo(1), "z live");
            Assert.That(stats.Tombstones, Is.EqualTo(1), "a tombstoned");
        }
    }
}
