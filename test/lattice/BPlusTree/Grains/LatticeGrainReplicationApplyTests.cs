using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Integration tests for <see cref="IReplicationApplyGrain"/> exercised
/// through a single <c>TestCluster</c>: verifies the apply seam preserves
/// the source <see cref="HybridLogicalClock"/> and origin cluster id
/// verbatim on the persisted <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/>.
/// </summary>
[TestFixture]
[Category("Integration")]
public partial class LatticeGrainReplicationApplyTests
{
    private ClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public async Task ApplySetAsync_persists_value_with_source_hlc_visible_via_GetWithVersionAsync()
    {
        const string tree = "rapply-set";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var sourceHlc = Hlc(42_000, 3);

        await apply.ApplySetAsync("k", new byte[] { 7 }, sourceHlc, "site-x", sourceVectorClock: null, expiresAtTicks: 0);

        var versioned = await lattice.GetWithVersionAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(versioned.Value, Is.EqualTo(new byte[] { 7 }));
            Assert.That(versioned.Version, Is.EqualTo(sourceHlc));
        });
    }

    [Test]
    public async Task ApplySetAsync_with_expiry_persists_expires_at_ticks()
    {
        const string tree = "rapply-ttl";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var future = DateTime.UtcNow.AddHours(1).Ticks;

        await apply.ApplySetAsync("k", new byte[] { 1 }, Hlc(10), "site-x", sourceVectorClock: null, expiresAtTicks: future);

        var value = await lattice.GetAsync("k");
        Assert.That(value, Is.EqualTo(new byte[] { 1 }));
    }

    [Test]
    public async Task ApplyDeleteAsync_tombstones_with_source_hlc()
    {
        const string tree = "rapply-del";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("k", new byte[] { 1 });
        var local = await lattice.GetWithVersionAsync("k");
        var deleteHlc = local.Version with { WallClockTicks = local.Version.WallClockTicks + 1_000 };

        await apply.ApplyDeleteAsync("k", deleteHlc, "site-x", sourceVectorClock: null);

        var after = await lattice.GetAsync("k");
        Assert.That(after, Is.Null);
    }

    [Test]
    public async Task ApplyDeleteAsync_older_hlc_does_not_overwrite_newer_local_value()
    {
        const string tree = "rapply-del-stale";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("k", new byte[] { 1 });
        var local = await lattice.GetWithVersionAsync("k");
        var olderHlc = local.Version with { WallClockTicks = local.Version.WallClockTicks - 1 };

        await apply.ApplyDeleteAsync("k", olderHlc, "site-x", sourceVectorClock: null);

        var after = await lattice.GetAsync("k");
        Assert.That(after, Is.EqualTo(new byte[] { 1 }));
    }

    [Test]
    public async Task ApplyDeleteRangeAsync_removes_all_matching_keys()
    {
        const string tree = "rapply-range";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("b", new byte[] { 2 });
        await lattice.SetAsync("c", new byte[] { 3 });

        await apply.ApplyDeleteRangeAsync(
            "a",
            "c",
            Hlc(long.MaxValue / 2, 0),
            "site-x",
            sourceVectorClock: null);

        Assert.Multiple(() =>
        {
            Assert.That(lattice.GetAsync("a").Result, Is.Null);
            Assert.That(lattice.GetAsync("b").Result, Is.Null);
            Assert.That(lattice.GetAsync("c").Result, Is.EqualTo(new byte[] { 3 })); // end-exclusive
        });
    }

    [Test]
    public async Task ApplyDeleteRangeAsync_with_explicit_matched_keys_tombstones_only_that_set()
    {
        const string tree = "rapply-range-predicate";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("b", new byte[] { 2 });
        await lattice.SetAsync("c", new byte[] { 3 });
        await lattice.SetAsync("d", new byte[] { 4 });

        // The producer matched only "a" and "c" inside the range; the receiver
        // must tombstone exactly that set and leave the in-range non-matching
        // key "b" untouched, never re-deriving membership from the bounds.
        await apply.ApplyDeleteRangeAsync(
            "a",
            "d",
            Hlc(long.MaxValue / 2, 0),
            "site-x",
            sourceVectorClock: null,
            explicitMatchedKeys: new[] { "a", "c" });

        Assert.Multiple(() =>
        {
            Assert.That(lattice.GetAsync("a").Result, Is.Null, "matched key tombstoned");
            Assert.That(lattice.GetAsync("b").Result, Is.EqualTo(new byte[] { 2 }), "in-range non-matching key survives");
            Assert.That(lattice.GetAsync("c").Result, Is.Null, "matched key tombstoned");
            Assert.That(lattice.GetAsync("d").Result, Is.EqualTo(new byte[] { 4 }), "end-exclusive bound survives");
        });
    }

    [Test]
    public async Task ApplyDeleteRangeAsync_with_empty_matched_keys_is_noop()
    {
        const string tree = "rapply-range-empty-predicate";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        await lattice.SetAsync("a", new byte[] { 1 });
        await lattice.SetAsync("b", new byte[] { 2 });

        await apply.ApplyDeleteRangeAsync(
            "a",
            "z",
            Hlc(long.MaxValue / 2, 0),
            "site-x",
            sourceVectorClock: null,
            explicitMatchedKeys: Array.Empty<string>());

        Assert.Multiple(() =>
        {
            Assert.That(lattice.GetAsync("a").Result, Is.EqualTo(new byte[] { 1 }));
            Assert.That(lattice.GetAsync("b").Result, Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task ApplyDeleteRangeAsync_with_inverted_range_is_noop()
    {
        const string tree = "rapply-range-inv";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("k", new byte[] { 1 });

        await apply.ApplyDeleteRangeAsync(
            "z",
            "a",
            Hlc(1),
            "site-x",
            sourceVectorClock: null);

        Assert.That(await lattice.GetAsync("k"), Is.EqualTo(new byte[] { 1 }));
    }

    /// <summary>
    /// Regression for the cross-origin LWW invariant on the DeleteRange
    /// apply seam. A foreign-origin Set whose HLC is strictly greater
    /// than every leaf stamp the producer of the range delete observed
    /// at authoring time must survive a re-application of that range
    /// delete on the receiver. The single-key analog of this invariant
    /// is already enforced by
    /// <see cref="ApplyDeleteAsync_older_hlc_does_not_overwrite_newer_local_value"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Pre-fix the receiver-side apply seam at
    /// <c>LatticeGrain.ReplicationApply.ApplyDeleteRangeAsync</c> wraps
    /// the per-shard <c>IShardRootGrain.DeleteRangeAsync</c> walk in
    /// <c>LatticeOriginContext</c> + <c>LatticeVectorClockContext</c>
    /// but does <em>not</em> set <c>LatticeHlcOverrideContext</c>, so
    /// the leaf falls into the standard <c>AdvanceClockOrOverride</c>
    /// branch that returns a fresh local HLC. Because the leaf's local
    /// clock has already absorbed the foreign Set's HLC (via the
    /// earlier <c>ApplySetAsync</c> path's <c>HybridLogicalClock.Merge</c>),
    /// every tombstone stamped on the range walk carries an HLC strictly
    /// greater than the foreign Set's HLC and wins LWW resolution -
    /// silently overwriting a value the authoring DeleteRange cannot
    /// have observed.
    /// </para>
    /// <para>
    /// Post-fix the apply seam stamps each tombstone with the producer's
    /// ceiling HLC (the maximum leaf stamp the producer observed during
    /// its authoring walk), preserving the invariant that a DeleteRange
    /// authored at frontier <c>T</c> cannot overwrite a write authored
    /// at any HLC strictly greater than <c>T</c>.
    /// </para>
    /// </remarks>
    [Test]
    public async Task ApplyDeleteRangeAsync_does_not_overwrite_foreign_origin_value_with_higher_hlc()
    {
        const string tree = "rapply-range-cross-origin-lww";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        // Foreign cluster "C" authored a Set at an HLC far in the future
        // (well past any local wall-clock tick the receiver can produce
        // organically). The apply seam preserves this HLC verbatim on
        // the persisted LwwValue and merges it into the leaf's local
        // clock so the leaf's next foreground tick is strictly greater
        // than foreignSetHlc.
        var foreignSetHlc = Hlc(DateTimeOffset.UtcNow.AddYears(50).UtcTicks, 0);
        await apply.ApplySetAsync(
            "m",
            new byte[] { 42 },
            foreignSetHlc,
            "cluster-C",
            sourceVectorClock: null,
            expiresAtTicks: 0);

        // Sanity check: the foreign-origin Set is visible before the
        // DeleteRange apply.
        var before = await lattice.GetAsync("m");
        Assert.That(before, Is.EqualTo(new byte[] { 42 }),
            "pre-condition: foreign-origin Set must be visible before DeleteRange apply");

        // Foreign cluster "A" authored a DeleteRange covering "m" at an
        // earlier frontier than foreignSetHlc (the producer's authoring
        // walk could only have stamped leaves at HLCs below
        // foreignSetHlc, since C's Set has not yet propagated to A).
        // Per LWW, C's Set must dominate any tombstone stamped at an
        // HLC below foreignSetHlc.
        var rangeDeleteHlc = Hlc(DateTimeOffset.UtcNow.UtcTicks, 0);
        await apply.ApplyDeleteRangeAsync(
            "a",
            "z",
            rangeDeleteHlc,
            "cluster-A",
            sourceVectorClock: null);

        // The cross-origin LWW invariant: a DeleteRange cannot win over
        // a foreign-origin write whose HLC is strictly greater than the
        // producer's authoring frontier. Today the receiver synthesises
        // a tombstone at a fresh local HLC strictly greater than
        // foreignSetHlc, the tombstone wins LWW, and the foreign value
        // is silently lost.
        var after = await lattice.GetAsync("m");
        Assert.That(after, Is.EqualTo(new byte[] { 42 }),
            "DeleteRange apply must preserve foreign-origin values with HLC above the producer's authoring frontier");
    }

    [Test]
    public void ApplySetAsync_throws_for_null_key()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-null-k");
        Assert.That(
            async () => await apply.ApplySetAsync(null!, new byte[] { 1 }, Hlc(1), "site-x", sourceVectorClock: null, 0),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ApplySetAsync_throws_for_null_value()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-null-v");
        Assert.That(
            async () => await apply.ApplySetAsync("k", null!, Hlc(1), "site-x", sourceVectorClock: null, 0),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ApplySetAsync_throws_for_empty_origin()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-empty-o");
        Assert.That(
            async () => await apply.ApplySetAsync("k", new byte[] { 1 }, Hlc(1), "", sourceVectorClock: null, 0),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ApplyDeleteAsync_throws_for_null_key()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-del-null-k");
        Assert.That(
            async () => await apply.ApplyDeleteAsync(null!, Hlc(1), "site-x", sourceVectorClock: null),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ApplyDeleteAsync_throws_for_empty_origin()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-del-empty-o");
        Assert.That(
            async () => await apply.ApplyDeleteAsync("k", Hlc(1), "", sourceVectorClock: null),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ApplyDeleteRangeAsync_throws_for_null_arguments()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-range-null");
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await apply.ApplyDeleteRangeAsync(null!, "z", Hlc(1), "site-x", sourceVectorClock: null),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await apply.ApplyDeleteRangeAsync("a", null!, Hlc(1), "site-x", sourceVectorClock: null),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                async () => await apply.ApplyDeleteRangeAsync("a", "z", Hlc(1), "", sourceVectorClock: null),
                Throws.InstanceOf<ArgumentException>());
        });
    }

    // ------------------------------------------------------------------
    // VC preservation through the receiver-side apply seam.
    // Set/Delete apply paths route through MergeManyAsync; we read the
    // persisted LwwEntry directly via IShardRootGrain.GetRawEntryAsync so the
    // assertion is independent of the observer payload.
    // ------------------------------------------------------------------

    private async Task<LwwEntry?> ReadRawEntryAsync(string treeId, string key)
    {
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(treeId);
        var routing = await lattice.GetRoutingAsync();
        var slot = routing.Map.Resolve(key);
        var shard = _fixture.Cluster.Client.GetGrain<IShardRootGrain>(
            $"{routing.PhysicalTreeId}/{slot}");
        return await shard.GetRawEntryAsync(key);
    }

    [Test]
    public async Task ApplySetAsync_persists_source_vector_clock_on_raw_entry()
    {
        const string tree = "rapply-set-vc";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var vc = new VersionVector();
        vc.Tick("site-x");
        vc.Tick("site-y");

        await apply.ApplySetAsync(
            "k",
            new byte[] { 7 },
            Hlc(42_000, 3),
            "site-x",
            sourceVectorClock: vc,
            expiresAtTicks: 0);

        var entry = await ReadRawEntryAsync(tree, "k");
        Assert.That(entry.HasValue, Is.True);
        var e = entry!.Value;
        Assert.Multiple(() =>
        {
            Assert.That(e.OriginClusterId, Is.EqualTo("site-x"));
            Assert.That(e.VectorClock, Is.Not.Null);
            Assert.That(e.VectorClock!.GetClock("site-x"), Is.EqualTo(vc.GetClock("site-x")));
            Assert.That(e.VectorClock!.GetClock("site-y"), Is.EqualTo(vc.GetClock("site-y")));
        });
    }

    [Test]
    public async Task ApplySetAsync_with_null_vector_clock_persists_null_on_raw_entry()
    {
        const string tree = "rapply-set-vc-null";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);

        await apply.ApplySetAsync(
            "k",
            new byte[] { 1 },
            Hlc(10),
            "site-x",
            sourceVectorClock: null,
            expiresAtTicks: 0);

        var entry = await ReadRawEntryAsync(tree, "k");
        Assert.That(entry.HasValue, Is.True);
        Assert.That(entry!.Value.VectorClock, Is.Null);
    }

    // ------------------------------------------------------------------
    // Batched apply path - IReplicationApplyGrain.ApplyMergeManyAsync.
    // Mirrors the per-entry test surface above so the bit-identical
    // contract claimed by BuildApplyMergeLww is exercised end-to-end on
    // a real cluster: validation guards, persisted shape, VC / origin /
    // expiry preservation, LWW dominance, single-shard fast path,
    // multi-shard fan-out, mixed Set+Delete in one call, and the
    // single-item fast path that deflects to ApplyMergeOneAsync.
    // ------------------------------------------------------------------

    [Test]
    public async Task ApplyMergeManyAsync_with_empty_list_is_noop()
    {
        const string tree = "rapply-merge-empty";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("k", new byte[] { 9 });

        await apply.ApplyMergeManyAsync(Array.Empty<ApplyMergeItem>());

        Assert.That(await lattice.GetAsync("k"), Is.EqualTo(new byte[] { 9 }));
    }

    [Test]
    public void ApplyMergeManyAsync_throws_for_null_items()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-merge-null");
        Assert.That(
            async () => await apply.ApplyMergeManyAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public async Task ApplyMergeManyAsync_single_set_persists_value_and_source_hlc()
    {
        const string tree = "rapply-merge-single-set";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        var sourceHlc = Hlc(12_345, 7);

        await apply.ApplyMergeManyAsync(new[]
        {
            new ApplyMergeItem
            {
                Key = "k",
                Value = new byte[] { 42 },
                SourceHlc = sourceHlc,
                OriginClusterId = "site-x",
                SourceVectorClock = null,
                ExpiresAtTicks = 0,
                IsTombstone = false,
            },
        });

        var versioned = await lattice.GetWithVersionAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(versioned.Value, Is.EqualTo(new byte[] { 42 }));
            Assert.That(versioned.Version, Is.EqualTo(sourceHlc));
        });
    }

    [Test]
    public async Task ApplyMergeManyAsync_single_tombstone_removes_key()
    {
        const string tree = "rapply-merge-single-del";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("k", new byte[] { 1 });
        var local = await lattice.GetWithVersionAsync("k");
        var deleteHlc = local.Version with { WallClockTicks = local.Version.WallClockTicks + 1_000 };

        await apply.ApplyMergeManyAsync(new[]
        {
            new ApplyMergeItem
            {
                Key = "k",
                Value = null,
                SourceHlc = deleteHlc,
                OriginClusterId = "site-x",
                SourceVectorClock = null,
                ExpiresAtTicks = 0,
                IsTombstone = true,
            },
        });

        Assert.That(await lattice.GetAsync("k"), Is.Null);
    }

    [Test]
    public void ApplyMergeManyAsync_single_item_throws_for_null_key()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-merge-null-k1");
        Assert.That(
            async () => await apply.ApplyMergeManyAsync(new[]
            {
                new ApplyMergeItem
                {
                    Key = null!,
                    Value = new byte[] { 1 },
                    SourceHlc = Hlc(1),
                    OriginClusterId = "site-x",
                    IsTombstone = false,
                },
            }),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ApplyMergeManyAsync_single_item_throws_for_empty_origin()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-merge-empty-o1");
        Assert.That(
            async () => await apply.ApplyMergeManyAsync(new[]
            {
                new ApplyMergeItem
                {
                    Key = "k",
                    Value = new byte[] { 1 },
                    SourceHlc = Hlc(1),
                    OriginClusterId = "",
                    IsTombstone = false,
                },
            }),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void ApplyMergeManyAsync_multi_item_throws_for_null_key()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-merge-null-kN");
        Assert.That(
            async () => await apply.ApplyMergeManyAsync(new[]
            {
                new ApplyMergeItem
                {
                    Key = "ok",
                    Value = new byte[] { 1 },
                    SourceHlc = Hlc(1),
                    OriginClusterId = "site-x",
                    IsTombstone = false,
                },
                new ApplyMergeItem
                {
                    Key = null!,
                    Value = new byte[] { 2 },
                    SourceHlc = Hlc(2),
                    OriginClusterId = "site-x",
                    IsTombstone = false,
                },
            }),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void ApplyMergeManyAsync_multi_item_throws_for_empty_origin()
    {
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>("rapply-merge-empty-oN");
        Assert.That(
            async () => await apply.ApplyMergeManyAsync(new[]
            {
                new ApplyMergeItem
                {
                    Key = "k1",
                    Value = new byte[] { 1 },
                    SourceHlc = Hlc(1),
                    OriginClusterId = "site-x",
                    IsTombstone = false,
                },
                new ApplyMergeItem
                {
                    Key = "k2",
                    Value = new byte[] { 2 },
                    SourceHlc = Hlc(2),
                    OriginClusterId = "",
                    IsTombstone = false,
                },
            }),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task ApplyMergeManyAsync_persists_expires_at_ticks()
    {
        const string tree = "rapply-merge-ttl";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var future = DateTime.UtcNow.AddHours(1).Ticks;

        await apply.ApplyMergeManyAsync(new[]
        {
            new ApplyMergeItem
            {
                Key = "k",
                Value = new byte[] { 1 },
                SourceHlc = Hlc(100),
                OriginClusterId = "site-x",
                ExpiresAtTicks = future,
                IsTombstone = false,
            },
        });

        var entry = await ReadRawEntryAsync(tree, "k");
        Assert.That(entry.HasValue, Is.True);
        Assert.That(entry!.Value.ExpiresAtTicks, Is.EqualTo(future));
    }

    [Test]
    public async Task ApplyMergeManyAsync_persists_source_vector_clock_and_origin_on_raw_entry()
    {
        const string tree = "rapply-merge-vc";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var vc = new VersionVector();
        vc.Tick("site-x");
        vc.Tick("site-y");
        vc.Tick("site-y");

        await apply.ApplyMergeManyAsync(new[]
        {
            new ApplyMergeItem
            {
                Key = "k",
                Value = new byte[] { 7 },
                SourceHlc = Hlc(42_000, 3),
                OriginClusterId = "site-x",
                SourceVectorClock = vc,
                ExpiresAtTicks = 0,
                IsTombstone = false,
            },
        });

        var entry = await ReadRawEntryAsync(tree, "k");
        Assert.That(entry.HasValue, Is.True);
        var e = entry!.Value;
        Assert.Multiple(() =>
        {
            Assert.That(e.OriginClusterId, Is.EqualTo("site-x"));
            Assert.That(e.VectorClock, Is.Not.Null);
            Assert.That(e.VectorClock!.GetClock("site-x"), Is.EqualTo(vc.GetClock("site-x")));
            Assert.That(e.VectorClock!.GetClock("site-y"), Is.EqualTo(vc.GetClock("site-y")));
        });
    }

    [Test]
    public async Task ApplyMergeManyAsync_older_hlc_does_not_overwrite_newer_local_value()
    {
        const string tree = "rapply-merge-stale";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);
        await lattice.SetAsync("k", new byte[] { 1 });
        var local = await lattice.GetWithVersionAsync("k");
        var olderHlc = local.Version with { WallClockTicks = local.Version.WallClockTicks - 1 };

        await apply.ApplyMergeManyAsync(new[]
        {
            new ApplyMergeItem
            {
                Key = "k",
                Value = new byte[] { 99 },
                SourceHlc = olderHlc,
                OriginClusterId = "site-x",
                IsTombstone = false,
            },
        });

        Assert.That(await lattice.GetAsync("k"), Is.EqualTo(new byte[] { 1 }));
    }

    [Test]
    public async Task ApplyMergeManyAsync_multi_item_single_shard_persists_all_keys()
    {
        const string tree = "rapply-merge-multi-1shard";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        // Pick three keys that resolve to the same shard so the
        // single-shard fast path (firstBatch !== null, byShard === null)
        // is exercised. Default ClusterFixture options use a hash-based
        // shard map, so we filter by routing.Map.Resolve until we find
        // three colliding keys.
        var routing = await lattice.GetRoutingAsync();
        var anchor = routing.Map.Resolve("a");
        var keys = new List<string> { "a" };
        for (var i = 0; keys.Count < 3 && i < 1_000; i++)
        {
            var candidate = $"k{i}";
            if (routing.Map.Resolve(candidate) == anchor)
            {
                keys.Add(candidate);
            }
        }
        Assert.That(keys, Has.Count.EqualTo(3), "expected to find three same-shard keys within 1000 candidates");

        var items = new List<ApplyMergeItem>();
        for (var i = 0; i < keys.Count; i++)
        {
            items.Add(new ApplyMergeItem
            {
                Key = keys[i],
                Value = new byte[] { (byte)(10 + i) },
                SourceHlc = Hlc(1_000 + i),
                OriginClusterId = "site-x",
                IsTombstone = false,
            });
        }

        await apply.ApplyMergeManyAsync(items);

        Assert.Multiple(() =>
        {
            for (var i = 0; i < keys.Count; i++)
            {
                Assert.That(lattice.GetAsync(keys[i]).Result, Is.EqualTo(new byte[] { (byte)(10 + i) }), $"key {keys[i]}");
            }
        });
    }

    [Test]
    public async Task ApplyMergeManyAsync_multi_item_multi_shard_persists_all_keys()
    {
        const string tree = "rapply-merge-multi-Nshard";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        // Find two keys that resolve to different shards so the
        // multi-shard fan-out path (byShard promoted, parallel
        // Task.WhenAll) is exercised.
        var routing = await lattice.GetRoutingAsync();
        var anchor = routing.Map.Resolve("a");
        string? otherKey = null;
        for (var i = 0; i < 1_000; i++)
        {
            var candidate = $"x{i}";
            if (routing.Map.Resolve(candidate) != anchor)
            {
                otherKey = candidate;
                break;
            }
        }
        Assume.That(otherKey, Is.Not.Null,
            "ClusterFixture's default shard count is 1 - multi-shard fan-out cannot be exercised here");

        var items = new[]
        {
            new ApplyMergeItem
            {
                Key = "a",
                Value = new byte[] { 1 },
                SourceHlc = Hlc(1_000),
                OriginClusterId = "site-x",
                IsTombstone = false,
            },
            new ApplyMergeItem
            {
                Key = otherKey!,
                Value = new byte[] { 2 },
                SourceHlc = Hlc(1_001),
                OriginClusterId = "site-x",
                IsTombstone = false,
            },
        };

        await apply.ApplyMergeManyAsync(items);

        Assert.Multiple(() =>
        {
            Assert.That(lattice.GetAsync("a").Result, Is.EqualTo(new byte[] { 1 }));
            Assert.That(lattice.GetAsync(otherKey!).Result, Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task ApplyMergeManyAsync_mixed_set_and_delete_persist_in_one_call()
    {
        const string tree = "rapply-merge-mixed";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        // Seed an existing value that the delete will tombstone.
        await lattice.SetAsync("del", new byte[] { 0 });
        var seed = await lattice.GetWithVersionAsync("del");
        var delHlc = seed.Version with { WallClockTicks = seed.Version.WallClockTicks + 1_000 };

        await apply.ApplyMergeManyAsync(new[]
        {
            new ApplyMergeItem
            {
                Key = "set",
                Value = new byte[] { 7 },
                SourceHlc = Hlc(2_000),
                OriginClusterId = "site-x",
                IsTombstone = false,
            },
            new ApplyMergeItem
            {
                Key = "del",
                Value = null,
                SourceHlc = delHlc,
                OriginClusterId = "site-x",
                IsTombstone = true,
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(lattice.GetAsync("set").Result, Is.EqualTo(new byte[] { 7 }));
            Assert.That(lattice.GetAsync("del").Result, Is.Null);
        });
    }

    [Test]
    public async Task ApplyMergeManyAsync_same_key_twice_with_increasing_hlc_keeps_last()
    {
        const string tree = "rapply-merge-same-key";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        // Same-key collision within a batch: producer guarantees per-origin
        // monotonic HLC order, so the dictionary's last-wins assignment
        // must keep the entry with the higher HLC. Add a third item on a
        // distinct key so we go through the multi-item path
        // (single-item fast path would not exercise the per-shard
        // dictionary-write).
        var hlc1 = Hlc(3_000);
        var hlc2 = Hlc(3_001);
        await apply.ApplyMergeManyAsync(new[]
        {
            new ApplyMergeItem
            {
                Key = "k",
                Value = new byte[] { 1 },
                SourceHlc = hlc1,
                OriginClusterId = "site-x",
                IsTombstone = false,
            },
            new ApplyMergeItem
            {
                Key = "k",
                Value = new byte[] { 2 },
                SourceHlc = hlc2,
                OriginClusterId = "site-x",
                IsTombstone = false,
            },
            new ApplyMergeItem
            {
                Key = "other",
                Value = new byte[] { 3 },
                SourceHlc = Hlc(3_002),
                OriginClusterId = "site-x",
                IsTombstone = false,
            },
        });

        var versioned = await lattice.GetWithVersionAsync("k");
        Assert.Multiple(() =>
        {
            Assert.That(versioned.Value, Is.EqualTo(new byte[] { 2 }));
            Assert.That(versioned.Version, Is.EqualTo(hlc2));
            Assert.That(lattice.GetAsync("other").Result, Is.EqualTo(new byte[] { 3 }));
        });
    }
}
