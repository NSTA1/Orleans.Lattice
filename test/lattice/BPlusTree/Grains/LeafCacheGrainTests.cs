using NSubstitute;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public class LeafCacheGrainTests
{
    private static readonly GrainId LeafGrainId = GrainId.Create("leaf", "primary-leaf");

    private static (LeafCacheGrain grain, IBPlusLeafGrain leaf) CreateGrain(LatticeOptions? options = null)
    {
        var leafMock = Substitute.For<IBPlusLeafGrain>();
        leafMock.GetTreeIdAsync().Returns("test-tree");

        var context = Substitute.For<IGrainContext>();
        // The cache grain key is the string form of the primary leaf's GrainId.
        context.GrainId.Returns(GrainId.Create("cache", LeafGrainId.ToString()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leafMock);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeOptions());

        var grain = new LeafCacheGrain(context, grainFactory, optionsMonitor);
        return (grain, leafMock);
    }

    private static StateDelta EmptyDelta() => new()
    {
        Entries = new Dictionary<string, LwwValue<byte[]>>(),
        Version = new VersionVector()
    };

    private static StateDelta DeltaWith(params (string key, byte[] value)[] entries)
    {
        var clock = new HybridLogicalClock();
        var version = new VersionVector();
        var dict = new Dictionary<string, LwwValue<byte[]>>();

        foreach (var (key, value) in entries)
        {
            clock = HybridLogicalClock.Tick(clock);
            version.Tick("primary");
            dict[key] = LwwValue<byte[]>.Create(value, clock);
        }

        return new StateDelta { Entries = dict, Version = version };
    }

    private static StateDelta TombstoneDelta(string key, HybridLogicalClock clock)
    {
        var version = new VersionVector();
        version.Tick("primary");
        return new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>>
            {
                [key] = LwwValue<byte[]>.Tombstone(clock)
            },
            Version = version
        };
    }

    // --- Basic reads ---

    [Fact]
    public async Task Get_returns_null_when_primary_has_no_data()
    {
        var (grain, leaf) = CreateGrain();
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());

        var result = await grain.GetAsync("k1");
        Assert.Null(result);
    }

    [Fact]
    public async Task Get_returns_value_from_primary_delta()
    {
        var (grain, leaf) = CreateGrain();
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(("k1", Encoding.UTF8.GetBytes("v1"))));

        var result = await grain.GetAsync("k1");
        Assert.NotNull(result);
        Assert.Equal("v1", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task Get_returns_null_for_key_not_in_delta()
    {
        var (grain, leaf) = CreateGrain();
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(("k1", Encoding.UTF8.GetBytes("v1"))));

        var result = await grain.GetAsync("missing");
        Assert.Null(result);
    }

    // --- Tombstone handling ---

    [Fact]
    public async Task Get_returns_null_for_tombstoned_entry()
    {
        var (grain, leaf) = CreateGrain();

        // First call returns a live entry.
        var clock = HybridLogicalClock.Tick(default);
        var liveDelta = new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>>
            {
                ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v1"), clock)
            },
            Version = new VersionVector()
        };
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(liveDelta);
        Assert.NotNull(await grain.GetAsync("k1"));

        // Second call returns a tombstone with higher timestamp.
        var tombClock = HybridLogicalClock.Tick(clock);
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(TombstoneDelta("k1", tombClock));

        var result = await grain.GetAsync("k1");
        Assert.Null(result);
    }

    // --- Delta refresh ---

    [Fact]
    public async Task Get_always_calls_delta_on_primary()
    {
        var (grain, leaf) = CreateGrain();
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());

        await grain.GetAsync("k1");
        await grain.GetAsync("k2");
        await grain.GetAsync("k3");

        await leaf.Received(3).GetDeltaSinceAsync(Arg.Any<VersionVector>());
    }

    [Fact]
    public async Task Get_skips_merge_when_delta_is_empty()
    {
        var (grain, leaf) = CreateGrain();

        // First call populates cache.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(("k1", Encoding.UTF8.GetBytes("v1"))));
        await grain.GetAsync("k1");

        // Second call returns empty delta — cached value should still be there.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());

        var result = await grain.GetAsync("k1");
        Assert.Equal("v1", Encoding.UTF8.GetString(result!));
    }

    // --- LWW merge ---

    [Fact]
    public async Task Cache_merges_updates_with_LWW_semantics()
    {
        var (grain, leaf) = CreateGrain();

        // First delta: k1 = v1.
        var clock1 = HybridLogicalClock.Tick(default);
        var delta1 = new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>>
            {
                ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v1"), clock1)
            },
            Version = new VersionVector()
        };
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(delta1);
        await grain.GetAsync("k1");

        // Second delta: k1 = v2 with higher clock.
        var clock2 = HybridLogicalClock.Tick(clock1);
        var delta2 = new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>>
            {
                ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v2"), clock2)
            },
            Version = new VersionVector()
        };
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(delta2);

        var result = await grain.GetAsync("k1");
        Assert.Equal("v2", Encoding.UTF8.GetString(result!));
    }

    [Fact]
    public async Task Cache_ignores_stale_updates()
    {
        var (grain, leaf) = CreateGrain();

        // Construct two clocks with guaranteed ordering: low < high.
        var lowClock = HybridLogicalClock.Tick(default);
        // Merge ensures highClock is strictly greater than lowClock.
        var highClock = HybridLogicalClock.Merge(lowClock, lowClock);

        // First delta: k1 = "newer" with high clock.
        var delta1 = new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>>
            {
                ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("newer"), highClock)
            },
            Version = new VersionVector()
        };
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(delta1);
        await grain.GetAsync("k1");

        // Second delta: k1 = "older" with lower clock — should be ignored by LWW merge.
        var delta2 = new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>>
            {
                ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("older"), lowClock)
            },
            Version = new VersionVector()
        };
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(delta2);

        var result = await grain.GetAsync("k1");
        Assert.Equal("newer", Encoding.UTF8.GetString(result!));
    }

    // --- Multiple keys ---

    [Fact]
    public async Task Cache_handles_multiple_keys_independently()
    {
        var (grain, leaf) = CreateGrain();
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(
                ("a", Encoding.UTF8.GetBytes("1")),
                ("b", Encoding.UTF8.GetBytes("2")),
                ("c", Encoding.UTF8.GetBytes("3"))));

        Assert.Equal("1", Encoding.UTF8.GetString((await grain.GetAsync("a"))!));

        // Subsequent reads return empty delta, but cache retains all keys.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());

        Assert.Equal("2", Encoding.UTF8.GetString((await grain.GetAsync("b"))!));
        Assert.Equal("3", Encoding.UTF8.GetString((await grain.GetAsync("c"))!));
    }

    // --- Split key pruning ---

    [Fact]
    public async Task Cache_prunes_entries_above_split_key()
    {
        var (grain, leaf) = CreateGrain();

        // Populate cache with three entries: a, m, z.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(
                ("a", Encoding.UTF8.GetBytes("1")),
                ("m", Encoding.UTF8.GetBytes("2")),
                ("z", Encoding.UTF8.GetBytes("3"))));
        await grain.GetAsync("a"); // triggers refresh

        // Now the primary reports a split at "m" — entries >= "m" belong to the sibling.
        var splitDelta = new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>>(),
            Version = new VersionVector(),
            SplitKey = "m"
        };
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(splitDelta);
        await grain.GetAsync("a"); // triggers refresh with split key

        // "a" should still be cached; "m" and "z" should be pruned.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());
        Assert.Equal("1", Encoding.UTF8.GetString((await grain.GetAsync("a"))!));
        Assert.Null(await grain.GetAsync("m"));
        Assert.Null(await grain.GetAsync("z"));
    }

    [Fact]
    public async Task Cache_pruning_is_idempotent()
    {
        var (grain, leaf) = CreateGrain();

        // Populate cache.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(("a", Encoding.UTF8.GetBytes("1")), ("z", Encoding.UTF8.GetBytes("2"))));
        await grain.GetAsync("a");

        // Report split key multiple times — should not crash or remove "a".
        var splitDelta = new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>>(),
            Version = new VersionVector(),
            SplitKey = "m"
        };
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(splitDelta);
        await grain.GetAsync("a");
        await grain.GetAsync("a");

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());
        Assert.Equal("1", Encoding.UTF8.GetString((await grain.GetAsync("a"))!));
        Assert.Null(await grain.GetAsync("z"));
    }

    [Fact]
    public async Task Cache_does_not_prune_when_split_key_is_null()
    {
        var (grain, leaf) = CreateGrain();

        // Populate cache.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(("a", Encoding.UTF8.GetBytes("1")), ("z", Encoding.UTF8.GetBytes("2"))));
        await grain.GetAsync("a");

        // Delta with no split key — nothing pruned.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());

        Assert.Equal("1", Encoding.UTF8.GetString((await grain.GetAsync("a"))!));
        Assert.Equal("2", Encoding.UTF8.GetString((await grain.GetAsync("z"))!));
    }

    // --- Sibling leaf after split (MergeEntriesAsync scenario) ---

    [Fact]
    public async Task Cache_populates_from_sibling_leaf_after_split()
    {
        // Simulates a sibling leaf that received entries via MergeEntriesAsync
        // during a split. The sibling's version vector must be non-empty so that
        // the cache's delta check doesn't short-circuit on empty-dominates-empty.
        var (grain, leaf) = CreateGrain();

        // The sibling has entries (transferred during split) and a ticked version.
        // This mirrors the fix in MergeEntriesAsync which now ticks the version.
        var clock = HybridLogicalClock.Tick(default);
        var siblingVersion = new VersionVector();
        siblingVersion.Tick("sibling-replica");

        var siblingDelta = new StateDelta
        {
            Entries = new Dictionary<string, LwwValue<byte[]>>
            {
                ["k1"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v1"), clock),
                ["k2"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v2"), HybridLogicalClock.Tick(clock))
            },
            Version = siblingVersion
        };
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(siblingDelta);

        var result = await grain.GetAsync("k1");
        Assert.NotNull(result);
        Assert.Equal("v1", Encoding.UTF8.GetString(result));

        var result2 = await grain.GetAsync("k2");
        Assert.NotNull(result2);
        Assert.Equal("v2", Encoding.UTF8.GetString(result2));
    }

    [Fact]
    public async Task Cache_fails_when_sibling_version_is_empty()
    {
        // Demonstrates the bug scenario: if MergeEntriesAsync did NOT tick the
        // version, the sibling's version would be empty. The cache's delta check
        // (empty.DominatesOrEquals(empty) → true) would short-circuit, returning
        // an empty delta, and the cache would never populate.
        var (grain, leaf) = CreateGrain();

        // Sibling has entries but empty version (the bug scenario).
        var clock = HybridLogicalClock.Tick(default);
        var emptyVersion = new VersionVector();

        // First call: cache version is empty, sibling version is empty.
        // DominatesOrEquals(empty) → true → empty delta returned.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(new StateDelta
            {
                Entries = new Dictionary<string, LwwValue<byte[]>>(),
                Version = emptyVersion
            });

        // Key exists in the leaf but the cache can't see it.
        var result = await grain.GetAsync("k1");
        Assert.Null(result);
    }

    // --- GetManyAsync tests ---

    [Fact]
    public async Task GetManyAsync_returns_existing_keys_from_cache()
    {
        var (grain, leaf) = CreateGrain();
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(
                ("a", Encoding.UTF8.GetBytes("1")),
                ("b", Encoding.UTF8.GetBytes("2")),
                ("c", Encoding.UTF8.GetBytes("3"))));

        var result = await grain.GetManyAsync(["a", "b", "c"]);

        Assert.Equal(3, result.Count);
        Assert.Equal("1", Encoding.UTF8.GetString(result["a"]));
        Assert.Equal("2", Encoding.UTF8.GetString(result["b"]));
        Assert.Equal("3", Encoding.UTF8.GetString(result["c"]));
    }

    [Fact]
    public async Task GetManyAsync_omits_missing_and_tombstoned_keys()
    {
        var (grain, leaf) = CreateGrain();

        var clock = HybridLogicalClock.Tick(default);
        var version = new VersionVector();
        version.Tick("primary");
        var tombClock = HybridLogicalClock.Tick(clock);

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(new StateDelta
            {
                Entries = new Dictionary<string, LwwValue<byte[]>>
                {
                    ["a"] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("1"), clock),
                    ["b"] = LwwValue<byte[]>.Tombstone(tombClock)
                },
                Version = version
            });

        var result = await grain.GetManyAsync(["a", "b", "missing"]);

        Assert.Single(result);
        Assert.True(result.ContainsKey("a"));
    }

    [Fact]
    public async Task GetManyAsync_returns_empty_for_empty_input()
    {
        var (grain, leaf) = CreateGrain();
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());

        var result = await grain.GetManyAsync([]);

        Assert.Empty(result);
    }

    // --- CacheTtl tests ---

    [Fact]
    public async Task Cache_skips_refresh_when_within_ttl()
    {
        var (grain, leaf) = CreateGrain(new LatticeOptions { CacheTtl = TimeSpan.FromSeconds(30) });

        // First call populates cache.
        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>())
            .Returns(DeltaWith(("k1", Encoding.UTF8.GetBytes("v1"))));
        await grain.GetAsync("k1");

        // Second call should use cached value without calling delta again.
        // (TTL is 30s, so within window)
        var result = await grain.GetAsync("k1");

        Assert.Equal("v1", Encoding.UTF8.GetString(result!));
        // Only 1 delta call — the second was skipped due to TTL.
        await leaf.Received(1).GetDeltaSinceAsync(Arg.Any<VersionVector>());
    }

    [Fact]
    public async Task Cache_refreshes_on_every_read_when_ttl_is_zero()
    {
        var (grain, leaf) = CreateGrain(new LatticeOptions { CacheTtl = TimeSpan.Zero });

        leaf.GetDeltaSinceAsync(Arg.Any<VersionVector>()).Returns(EmptyDelta());

        await grain.GetAsync("k1");
        await grain.GetAsync("k2");
        await grain.GetAsync("k3");

        await leaf.Received(3).GetDeltaSinceAsync(Arg.Any<VersionVector>());
    }
}
