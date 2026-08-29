using System.Text;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the cross-cluster LWW delivery bug fixed by
/// the activation-scoped delivery cursor on <see cref="BPlusLeafGrain"/>.
/// <para>
/// Pre-fix shape: when the destination leaf had already published a
/// <see cref="VersionVector"/> clock above an inbound cross-cluster
/// entry's preserved source HLC, the cache's HLC-based delta filter
/// (<c>lww.Timestamp &gt; callerClock</c>) silently dropped the
/// incoming value. The cache then served the stale local snapshot
/// indefinitely, even though the leaf's <c>Entries</c>
/// projection had merged the cross-cluster value correctly. The
/// destination cluster therefore failed the "0-or-all" visibility
/// invariant that <c>SetManyAtomicAsync</c> must guarantee end-to-end.
/// </para>
/// <para>
/// Post-fix shape: the cache pulls a delta keyed by a non-HLC
/// activation-scoped sequence (<see cref="LeafDeliveryCursor"/>). Every
/// projection-modifying call on the leaf (<c>StoreEntry</c>,
/// <c>RemoveEntry</c>) bumps that sequence in lock-step with the
/// projection, so any merged value - including one whose preserved
/// source HLC is below the destination leaf's published clock - is
/// delivered on the next refresh.
/// </para>
/// <para>
/// These tests construct a real <see cref="BPlusLeafGrain"/> as the
/// cache's primary so the cursor advances through the production code
/// path, and exercise the HLC-rewind shape via
/// <see cref="BPlusLeafGrain.MergeEntriesAsync"/>, which is the same
/// receiver-side seam used by replication / cross-leaf migration to
/// preserve source HLCs verbatim.
/// </para>
/// </summary>
public partial class LeafCacheGrainTests
{
    /// <summary>
    /// Builds a <see cref="LeafCacheGrain"/> whose primary leaf is a
    /// real <see cref="BPlusLeafGrain"/> sharing the same
    /// <see cref="GrainId"/>. The grain factory is wired so the cache
    /// resolves the real leaf for every cross-grain call, which is
    /// what the production hosting model produces under
    /// stateless-worker cache placement. Returns the cache and the
    /// real leaf so a test can mutate the leaf and assert reads
    /// through the cache.
    /// </summary>
    private static (LeafCacheGrain cache, BPlusLeafGrain leaf, GrainId leafId) CreateCacheOverRealLeaf(
        string testName,
        LatticeOptions? options = null)
    {
        var unique = $"{testName}-{Guid.NewGuid():N}";
        var leafId = GrainId.Create("leaf", unique);

        var leaf = BPlusLeafGrainTests.CreateLeafGrainForCrossFixtureUse(replicaId: unique);

        var cacheContext = Substitute.For<IGrainContext>();
        cacheContext.GrainId.Returns(GrainId.Create("cache", leafId.ToString()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        // CacheTtl is zero so every read exercises the refresh path
        // (no TTL fast-path masking the cursor behaviour). The
        // same-silo revision cookie is published by the real leaf
        // and consumed by the cache - the regression below requires
        // both gates to clear, which the real leaf does naturally.
        optionsMonitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeOptions { CacheTtl = TimeSpan.Zero });

        var resolver = CreateResolver(grainFactory, optionsMonitor);
        var cache = new LeafCacheGrain(cacheContext, grainFactory, optionsMonitor, resolver, TestOriginClusterIdResolver.Default());
        return (cache, leaf, leafId);
    }

    /// <summary>
    /// Crafts an <see cref="LwwValue{T}"/> whose <see cref="HybridLogicalClock"/>
    /// is strictly older than the supplied reference clock. Used to
    /// fabricate the "cross-cluster value carrying a source HLC below
    /// our leaf's published clock" shape that triggered the original
    /// bug.
    /// </summary>
    private static LwwValue<byte[]> StaleSourceValue(byte[] payload, HybridLogicalClock referenceClock)
    {
        // Subtract one tick from the reference clock's wall time so
        // the resulting stamp is unambiguously dominated along the
        // physical-time dimension. This produces the shape that
        // triggered the pre-fix bug: a merged value whose preserved
        // HLC is strictly older than the leaf's published clock, but
        // which is nonetheless a legitimate cross-cluster write that
        // must be delivered to the cache.
        var stale = new HybridLogicalClock
        {
            WallClockTicks = referenceClock.WallClockTicks - 1,
            Counter = 0,
        };
        return LwwValue<byte[]>.Create(payload, stale);
    }

    [Test]
    public async Task RefreshAsync_delivers_cross_cluster_entry_with_stale_source_HLC()
    {
        // Pre-fix repro: a local write advances the leaf's
        // Version[ReplicaId] clock above any plausible inbound HLC.
        // A subsequent cross-cluster merge for a new key arrives with
        // a strictly-older preserved source HLC. The old cache filter
        // would drop the new key on its next refresh; the cursor-
        // based filter must deliver it.
        var (cache, leaf, _) = CreateCacheOverRealLeaf(
            nameof(RefreshAsync_delivers_cross_cluster_entry_with_stale_source_HLC));

        // 1. Local write: lifts leaf.Version[ReplicaId] high.
        await leaf.SetAsync("local", Encoding.UTF8.GetBytes("local-v1"));

        // 2. First cache read: populates the cache and snaps its
        //    delivery cursor to the leaf's current sequence.
        var localObserved = await cache.GetAsync("local");
        Assert.That(localObserved, Is.Not.Null, "cache must serve the local write");
        Assert.That(Encoding.UTF8.GetString(localObserved!), Is.EqualTo("local-v1"));

        // 3. Cross-cluster merge: a new key arrives with a source HLC
        //    strictly below the leaf's now-published clock. The leaf
        //    accepts the merge (the value is for a new key, so the
        //    LWW comparison is vacuously the winner) and bumps the
        //    delivery sequence.
        var referenceClock = HybridLogicalClock.Tick(default);
        var staleRemote = StaleSourceValue(Encoding.UTF8.GetBytes("remote-v1"), referenceClock);
        await leaf.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["remote"] = staleRemote,
        });

        // 4. The cache's next read must deliver the cross-cluster
        //    value. Under the old HLC-based delta filter this
        //    assertion failed because the merged value's preserved
        //    HLC was below the cache's snapshot of
        //    leaf.Version[ReplicaId], so the leaf's
        //    GetDeltaSinceAsync returned Entries.Count == 0 and the
        //    cache served a null for the never-delivered key.
        var remoteObserved = await cache.GetAsync("remote");
        Assert.That(remoteObserved, Is.Not.Null,
            "cache must deliver the cross-cluster entry even when its preserved source HLC "
            + "is strictly older than the destination leaf's published clock");
        Assert.That(Encoding.UTF8.GetString(remoteObserved!), Is.EqualTo("remote-v1"));
    }

    [Test]
    public async Task RefreshAsync_delivers_cross_cluster_overwrite_when_local_clock_dominates()
    {
        // Stricter shape: the key already exists locally, the cache
        // has observed the local value, and a cross-cluster overwrite
        // with a strictly-newer source HLC but still below the
        // leaf's published clock arrives. The leaf's LWW merge
        // accepts the overwrite (per-entry timestamps win the
        // comparison), bumps the delivery sequence, and the cache
        // must observe the overwrite. This pins the post-fix
        // semantics that an HLC rewind below the published clock no
        // longer hides per-entry advances from the cache.
        var (cache, leaf, _) = CreateCacheOverRealLeaf(
            nameof(RefreshAsync_delivers_cross_cluster_overwrite_when_local_clock_dominates));

        // Seed an older local value first so the merged overwrite is
        // unambiguously newer at the per-entry level.
        var seedClock = HybridLogicalClock.Tick(default);
        await leaf.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("remote-v0"), seedClock),
        });

        // Lift the local replica's clock high via an unrelated
        // foreground write. This is the condition that produced the
        // pre-fix bug: leaf.Version[ReplicaId] now dominates any
        // strictly-older remote HLC.
        await leaf.SetAsync("unrelated", Encoding.UTF8.GetBytes("lift-clock"));

        // First read: cache observes the seeded value and snaps its
        // cursor.
        Assert.That(Encoding.UTF8.GetString((await cache.GetAsync("k1"))!), Is.EqualTo("remote-v0"));

        // Cross-cluster overwrite with a per-entry-newer HLC that is
        // nonetheless older than the leaf's published clock.
        var overwriteClock = HybridLogicalClock.Tick(seedClock);
        await leaf.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("remote-v1"), overwriteClock),
        });

        var observed = await cache.GetAsync("k1");
        Assert.That(observed, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(observed!), Is.EqualTo("remote-v1"),
            "cache must deliver the per-entry-newer cross-cluster overwrite even when the "
            + "leaf's published clock dominates its source HLC");
    }

    [Test]
    public async Task RefreshAsync_full_snapshot_on_first_contact_uses_epoch_mismatch_branch()
    {
        // The cursor's epoch-mismatch branch is the recovery path for
        // a fresh cache (or one that crossed an activation boundary).
        // A cache built with no prior cursor knowledge must trip the
        // mismatch and receive a full-projection snapshot, regardless
        // of how the leaf's HLC compares to the cursor's zero-value
        // sentinel.
        var (cache, leaf, _) = CreateCacheOverRealLeaf(
            nameof(RefreshAsync_full_snapshot_on_first_contact_uses_epoch_mismatch_branch));

        // Seed three entries on the leaf BEFORE the cache ever talks
        // to it. The cache's first refresh must therefore observe
        // every entry through the epoch-mismatch fall-back, not an
        // incremental delivery.
        await leaf.SetAsync("a", Encoding.UTF8.GetBytes("va"));
        await leaf.SetAsync("b", Encoding.UTF8.GetBytes("vb"));
        await leaf.SetAsync("c", Encoding.UTF8.GetBytes("vc"));

        Assert.That(Encoding.UTF8.GetString((await cache.GetAsync("a"))!), Is.EqualTo("va"));
        Assert.That(Encoding.UTF8.GetString((await cache.GetAsync("b"))!), Is.EqualTo("vb"));
        Assert.That(Encoding.UTF8.GetString((await cache.GetAsync("c"))!), Is.EqualTo("vc"));
    }

    [Test]
    public async Task RefreshAsync_no_delivery_when_sequence_unchanged()
    {
        // The cursor's same-epoch / same-sequence path must return an
        // empty delta - no entries should re-ship on a quiescent
        // refresh. This pins the steady-state efficiency invariant:
        // a tight read loop with no leaf-side writes does not
        // re-stream the entire projection over the cross-grain seam.
        var (cache, leaf, _) = CreateCacheOverRealLeaf(
            nameof(RefreshAsync_no_delivery_when_sequence_unchanged),
            options: new LatticeOptions { CacheTtl = TimeSpan.Zero });

        await leaf.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        // Drain into the cache once so the cursor snaps to the
        // current leaf sequence.
        Assert.That(Encoding.UTF8.GetString((await cache.GetAsync("k1"))!), Is.EqualTo("v1"));

        // A second read must still serve from the cache (the value
        // is unchanged) without the leaf re-shipping its full
        // projection. We can't directly observe the delta entry
        // count from the cache surface, but we can assert the value
        // continues to read correctly with no further leaf-side
        // writes, which is the externally-visible contract.
        Assert.That(Encoding.UTF8.GetString((await cache.GetAsync("k1"))!), Is.EqualTo("v1"));
    }
}
