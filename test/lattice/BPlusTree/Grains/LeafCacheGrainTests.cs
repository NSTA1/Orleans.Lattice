using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public class LeafCacheGrainTests
{
    private static readonly GrainId LeafGrainId = GrainId.Create("leaf", "primary-leaf");

    private static (LeafCacheGrain grain, IBPlusLeafGrain leaf) CreateGrain()
    {
        var leafMock = Substitute.For<IBPlusLeafGrain>();

        var context = Substitute.For<IGrainContext>();
        // The cache grain key is the string form of the primary leaf's GrainId.
        context.GrainId.Returns(GrainId.Create("cache", LeafGrainId.ToString()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leafMock);

        var grain = new LeafCacheGrain(context, grainFactory);
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
}
