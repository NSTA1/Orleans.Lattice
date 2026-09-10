using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression coverage for the read-path and write-ahead-log half of the
/// internal-origin assertion.
/// <para>
/// The guard shipped for issue #1103 was applied <b>asymmetrically</b>: every
/// mutating entry point on <c>ShardRootGrain</c> and <c>BPlusLeafGrain</c>
/// asserted internal origin, but no <em>read</em> entry point did, and
/// <c>WalShardGrain</c> asserted it on nothing at all. Because all access-gate
/// enforcement lives on the <see cref="ILattice"/> facade, that left three
/// disclosure primitives reachable by any principal able to open a cluster-client
/// connection, with no Lattice credential and no tenant assertion:
/// </para>
/// <list type="number">
/// <item>
/// <c>ShardRootGrain</c> reads. The shard grain key is
/// <c>{physicalTreeId}/{shardIndex}</c>, so it is derivable from a tree name
/// alone: <c>GetAsync</c> returned another tenant's plaintext value, and the
/// unguarded scan surface (<c>GetSortedKeysBatchAsync</c> and friends) supplied
/// the keys to ask for.
/// </item>
/// <item>
/// <c>BPlusLeafGrain</c> reads. Leaf keys are <see cref="Guid"/>s, but the shard
/// grain handed them out through the equally unguarded
/// <c>GetLeftmostLeafIdAsync</c> / <c>GetLeafIdForKeyAsync</c>, completing the
/// discovery chain.
/// </item>
/// <item>
/// <c>WalShardGrain</c>, which had no guard on any entry point. Its key is
/// <c>{physicalTreeId}/{partition}</c>, so a single <c>ReadAsync</c> returned the
/// raw commit log - every key, value and mutation in order - and
/// <c>AppendAsync</c> allowed the mirror-image tampering primitive.
/// </item>
/// </list>
/// <para>
/// These tests mirror the existing mutation cases: they drive
/// <c>_cluster.GrainFactory</c> as a genuine external Orleans client, so the
/// capability-stripping filter stamps no internal-origin marker and the guard must
/// refuse. The facade-still-works case in the main partial proves the guard does
/// not break the legitimate silo-sourced call graph.
/// </para>
/// </summary>
public sealed partial class InternalOriginGuardIntegrationTests
{
    // --- Finding 1: ShardRootGrain read paths -------------------------------

    [Test]
    public void Shard_GetAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-read-get/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.GetAsync("k"));
    }

    [Test]
    public void Shard_GetWithVersionAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-read-getver/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.GetWithVersionAsync("k"));
    }

    [Test]
    public void Shard_GetRawEntryAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-read-getraw/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.GetRawEntryAsync("k"));
    }

    [Test]
    public void Shard_GetRawEntriesAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-read-getraws/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.GetRawEntriesAsync(["k"]));
    }

    [Test]
    public void Shard_ExistsAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-read-exists/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.ExistsAsync("k"));
    }

    [Test]
    public void Shard_GetManyAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-read-getmany/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.GetManyAsync(["k"]));
    }

    // The scan surface is the key-DISCOVERY half of the exploit: without it an
    // attacker must guess key names to call GetAsync with.

    [Test]
    public void Shard_GetSortedKeysBatchAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-scan-keys/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.GetSortedKeysBatchAsync(null, null, 10));
    }

    [Test]
    public void Shard_GetSortedEntriesBatchAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-scan-entries/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.GetSortedEntriesBatchAsync(null, null, 10));
    }

    [Test]
    public void Shard_CountBoundedAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-scan-count/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.CountBoundedAsync(null, null));
    }

    [Test]
    public void Shard_AnyBoundedAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-scan-any/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.AnyBoundedAsync(null));
    }

    // The leaf-id disclosure methods are what make the leaf grain's Guid key
    // addressable at all; without them finding 2 is not reachable from a tree name.

    [Test]
    public void Shard_GetLeftmostLeafIdAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-leafid-leftmost/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.GetLeftmostLeafIdAsync());
    }

    [Test]
    public void Shard_GetLeafIdForKeyAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-leafid-forkey/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.GetLeafIdForKeyAsync(null));
    }

    // --- Finding 2: BPlusLeafGrain read paths -------------------------------

    [Test]
    public void Leaf_GetAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await leaf.GetAsync("k"));
    }

    [Test]
    public void Leaf_GetRawEntryAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await leaf.GetRawEntryAsync("k"));
    }

    [Test]
    public void Leaf_GetKeysAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await leaf.GetKeysAsync());
    }

    [Test]
    public void Leaf_GetEntriesAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await leaf.GetEntriesAsync());
    }

    [Test]
    public void Leaf_GetLiveEntriesAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await leaf.GetLiveEntriesAsync());
    }

    [Test]
    public void Leaf_GetLiveRawEntriesAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await leaf.GetLiveRawEntriesAsync());
    }

    [Test]
    public void Leaf_GetDeltaSinceAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await leaf.GetDeltaSinceAsync(new VersionVector()));
    }

    [Test]
    public void Leaf_GetPendingKeysAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await leaf.GetPendingKeysAsync());
    }

    [Test]
    public void Leaf_CountAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var leaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid());

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await leaf.CountAsync());
    }

    // --- Finding 3: WalShardGrain (no guard on any entry point) -------------
    //
    // Strictly worse than the change feed, which does authorize: a single
    // ReadAsync returns the raw commit log for any tree whose name is known.

    [Test]
    public void Wal_ReadAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var wal = _cluster.GrainFactory.GetGrain<IWalShardGrain>("origin-guard-wal-read/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await wal.ReadAsync(0, 1000, CancellationToken.None));
    }

    [Test]
    public void Wal_ReadShippingAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var wal = _cluster.GrainFactory.GetGrain<IWalShardGrain>("origin-guard-wal-ship/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await wal.ReadShippingAsync(0, 1000, CancellationToken.None));
    }

    [Test]
    public void Wal_AppendAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var wal = _cluster.GrainFactory.GetGrain<IWalShardGrain>("origin-guard-wal-append/0");
        var record = new WalRecord
        {
            Key = "k",
            Value = Encoding.UTF8.GetBytes("v"),
            Timestamp = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
        };

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await wal.AppendAsync(record, CancellationToken.None));
    }

    [Test]
    public void Wal_AppendBatchAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var wal = _cluster.GrainFactory.GetGrain<IWalShardGrain>("origin-guard-wal-appendbatch/0");
        var record = new WalRecord
        {
            Key = "k",
            Value = Encoding.UTF8.GetBytes("v"),
            Timestamp = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 },
        };

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await wal.AppendBatchAsync([record], CancellationToken.None));
    }

    [Test]
    public void Wal_GetNextSequenceAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var wal = _cluster.GrainFactory.GetGrain<IWalShardGrain>("origin-guard-wal-nextseq/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await wal.GetNextSequenceAsync(CancellationToken.None));
    }

    [Test]
    public void Wal_GetLiveEntryCountAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var wal = _cluster.GrainFactory.GetGrain<IWalShardGrain>("origin-guard-wal-livecount/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await wal.GetLiveEntryCountAsync(CancellationToken.None));
    }

    [Test]
    public void Wal_GetRetainedByteSizeAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var wal = _cluster.GrainFactory.GetGrain<IWalShardGrain>("origin-guard-wal-bytes/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await wal.GetRetainedByteSizeAsync(CancellationToken.None));
    }

    // The administrative move surface is the tampering half: an unguarded
    // QuiesceForMoveAsync fences a live WAL shard against new appends, and
    // DeactivateForMoveAsync tears its activation down.

    [Test]
    public void Wal_QuiesceForMoveAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var wal = _cluster.GrainFactory.GetGrain<IWalShardGrain>("origin-guard-wal-quiesce/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await wal.QuiesceForMoveAsync(0, TimeSpan.FromSeconds(5), CancellationToken.None));
    }

    [Test]
    public void Wal_DeactivateForMoveAsync_direct_external_call_without_internal_origin_is_refused()
    {
        var wal = _cluster.GrainFactory.GetGrain<IWalShardGrain>("origin-guard-wal-deactivate/0");

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await wal.DeactivateForMoveAsync(CancellationToken.None));
    }

    // A forged internal-origin marker must not defeat the read or WAL guards
    // either: the capability-stripping filter removes it before the grain body
    // runs, exactly as it does for the mutation guards.

    [Test]
    public void Forged_internal_origin_marker_does_not_defeat_the_shard_read_guard()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("origin-guard-forged-read/0");
        RequestContext.Set(LatticeEventConstants.InternalGrainOriginRequestContextKey, true);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await shard.GetAsync("k"));
    }

    [Test]
    public void Forged_internal_origin_marker_does_not_defeat_the_wal_read_guard()
    {
        var wal = _cluster.GrainFactory.GetGrain<IWalShardGrain>("origin-guard-forged-wal/0");
        RequestContext.Set(LatticeEventConstants.InternalGrainOriginRequestContextKey, true);

        Assert.ThrowsAsync<LatticeAuthorizationDeniedException>(
            async () => await wal.ReadAsync(0, 1000, CancellationToken.None));
    }

    // The legitimate call graph must survive. A facade read fans out to the shard
    // and leaf grains over silo-sourced hops, and a facade write drives the WAL
    // shard the same way, so both must still succeed with the guards in place.

    [Test]
    public async Task Facade_read_and_scan_that_delegate_to_shard_leaf_and_wal_still_succeed()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("origin-guard-read-facade");

        await tree.SetAsync("a", Val("a"));
        await tree.SetAsync("b", Val("b"));

        var read = await tree.GetAsync("a");
        var exists = await tree.ExistsAsync("b");
        var count = await tree.CountAsync();
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
        {
            keys.Add(k);
        }

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(Val("a")));
            Assert.That(exists, Is.True);
            Assert.That(count, Is.EqualTo(2));
            Assert.That(keys, Is.EquivalentTo(new[] { "a", "b" }));
        });
    }
}
