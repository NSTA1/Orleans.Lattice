using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public class ShardRootGrainHotnessTests
{
    private static ShardRootGrain CreateGrain(
        FakePersistentState<ShardRootState>? state = null,
        string shardKey = "test-tree/0")
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", shardKey));
        state ??= new FakePersistentState<ShardRootState>();

        // Pre-populate state so EnsureRootAsync is a no-op (avoids calling
        // GetGrainId() on a substitute, which requires a real grain reference).
        state.State.RootNodeId ??= GrainId.Create("leaf", "test-leaf");
        state.State.RootIsLeaf = true;

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        // Stub leaf grain for read/write paths.
        var leafGrain = Substitute.For<IBPlusLeafGrain>();
        leafGrain.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        leafGrain.ExistsAsync(Arg.Any<string>()).Returns(Task.FromResult(false));
        leafGrain.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(null));
        leafGrain.DeleteAsync(Arg.Any<string>()).Returns(Task.FromResult(false));
        leafGrain.GetWithVersionAsync(Arg.Any<string>()).Returns(Task.FromResult(default(VersionedValue)!));
        leafGrain.CountAsync().Returns(Task.FromResult(0));
        leafGrain.GetKeysAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>())
            .Returns(Task.FromResult<List<string>>([]));
        leafGrain.GetEntriesAsync(Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>(), Arg.Any<string?>())
            .Returns(Task.FromResult<List<KeyValuePair<string, byte[]>>>([]));
        leafGrain.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        leafGrain.GetPrevSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        leafGrain.GetOrSetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(Task.FromResult(new GetOrSetResult { ExistingValue = null, Split = null }));
        leafGrain.SetIfVersionAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<HybridLogicalClock>())
            .Returns(Task.FromResult(new CasResult { Success = true, Split = null }));
        leafGrain.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>())
            .Returns(Task.FromResult(new RangeDeleteResult { Deleted = 0, PastRange = true }));
        leafGrain.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leafGrain);

        // Stub cache grain for read paths.
        var cacheGrain = Substitute.For<ILeafCacheGrain>();
        cacheGrain.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        cacheGrain.ExistsAsync(Arg.Any<string>()).Returns(Task.FromResult(false));
        cacheGrain.GetManyAsync(Arg.Any<List<string>>()).Returns(Task.FromResult(new Dictionary<string, byte[]>()));
        grainFactory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cacheGrain);

        return new ShardRootGrain(context, state, grainFactory, optionsMonitor);
    }

    // --- GetHotnessAsync (initial state) ---

    [Test]
    public async Task GetHotnessAsync_returns_zero_counters_on_fresh_grain()
    {
        var grain = CreateGrain();
        var hotness = await grain.GetHotnessAsync();

        Assert.That(hotness.Reads, Is.EqualTo(0));
        Assert.That(hotness.Writes, Is.EqualTo(0));
        Assert.That(hotness.Window, Is.GreaterThanOrEqualTo(TimeSpan.Zero));
    }

    // --- Read operations increment Reads ---

    [Test]
    public async Task GetAsync_increments_read_counter()
    {
        var grain = CreateGrain();
        await grain.GetAsync("key");

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(1));
        Assert.That(hotness.Writes, Is.EqualTo(0));
    }

    [Test]
    public async Task GetWithVersionAsync_increments_read_counter()
    {
        var grain = CreateGrain();
        await grain.GetWithVersionAsync("key");

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(1));
        Assert.That(hotness.Writes, Is.EqualTo(0));
    }

    [Test]
    public async Task ExistsAsync_increments_read_counter()
    {
        var grain = CreateGrain();
        await grain.ExistsAsync("key");

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(1));
        Assert.That(hotness.Writes, Is.EqualTo(0));
    }

    [Test]
    public async Task GetManyAsync_increments_read_counter()
    {
        var grain = CreateGrain();
        await grain.GetManyAsync(["a", "b"]);

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(1));
        Assert.That(hotness.Writes, Is.EqualTo(0));
    }

    [Test]
    public async Task CountAsync_increments_read_counter()
    {
        var grain = CreateGrain();
        await grain.CountAsync();

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(1));
        Assert.That(hotness.Writes, Is.EqualTo(0));
    }

    [Test]
    public async Task GetSortedKeysBatchAsync_increments_read_counter()
    {
        var grain = CreateGrain();
        await grain.GetSortedKeysBatchAsync(null, null, 10);

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(1));
        Assert.That(hotness.Writes, Is.EqualTo(0));
    }

    [Test]
    public async Task GetSortedKeysBatchReverseAsync_increments_read_counter()
    {
        var grain = CreateGrain();
        await grain.GetSortedKeysBatchReverseAsync(null, null, 10);

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(1));
        Assert.That(hotness.Writes, Is.EqualTo(0));
    }

    [Test]
    public async Task GetSortedEntriesBatchAsync_increments_read_counter()
    {
        var grain = CreateGrain();
        await grain.GetSortedEntriesBatchAsync(null, null, 10);

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(1));
        Assert.That(hotness.Writes, Is.EqualTo(0));
    }

    [Test]
    public async Task GetSortedEntriesBatchReverseAsync_increments_read_counter()
    {
        var grain = CreateGrain();
        await grain.GetSortedEntriesBatchReverseAsync(null, null, 10);

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(1));
        Assert.That(hotness.Writes, Is.EqualTo(0));
    }

    // --- Write operations increment Writes ---

    [Test]
    public async Task SetAsync_increments_write_counter()
    {
        var grain = CreateGrain();
        await grain.SetAsync("key", [1, 2, 3]);

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(0));
        Assert.That(hotness.Writes, Is.EqualTo(1));
    }

    [Test]
    public async Task DeleteAsync_increments_write_counter()
    {
        var grain = CreateGrain();
        await grain.DeleteAsync("key");

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(0));
        Assert.That(hotness.Writes, Is.EqualTo(1));
    }

    [Test]
    public async Task DeleteRangeAsync_increments_write_counter()
    {
        var grain = CreateGrain();
        await grain.DeleteRangeAsync("a", "z");

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(0));
        Assert.That(hotness.Writes, Is.EqualTo(1));
    }

    [Test]
    public async Task GetOrSetAsync_increments_write_counter()
    {
        var grain = CreateGrain();
        await grain.GetOrSetAsync("key", [1]);

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(0));
        Assert.That(hotness.Writes, Is.EqualTo(1));
    }

    [Test]
    public async Task SetIfVersionAsync_increments_write_counter()
    {
        var grain = CreateGrain();
        await grain.SetIfVersionAsync("key", [1], HybridLogicalClock.Zero);

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(0));
        Assert.That(hotness.Writes, Is.EqualTo(1));
    }

    [Test]
    public async Task SetManyAsync_increments_write_counter_per_entry()
    {
        var grain = CreateGrain();
        await grain.SetManyAsync([
            new("a", [1]),
            new("b", [2]),
            new("c", [3]),
        ]);

        var hotness = await grain.GetHotnessAsync();
        // SetManyAsync counts the whole batch as a single write (matches MergeManyAsync).
        Assert.That(hotness.Writes, Is.EqualTo(1));
    }

    [Test]
    public async Task MergeManyAsync_increments_write_counter()
    {
        var grain = CreateGrain();
        var entries = new Dictionary<string, LwwValue<byte[]>>
        {
            ["k1"] = new() { Value = [1], Timestamp = HybridLogicalClock.Zero },
        };
        await grain.MergeManyAsync(entries);

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Writes, Is.EqualTo(1));
    }

    // --- Mixed operations ---

    [Test]
    public async Task GetHotnessAsync_accumulates_mixed_reads_and_writes()
    {
        var grain = CreateGrain();

        await grain.GetAsync("a");
        await grain.GetAsync("b");
        await grain.SetAsync("c", [1]);
        await grain.ExistsAsync("d");
        await grain.DeleteAsync("e");

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(3));
        Assert.That(hotness.Writes, Is.EqualTo(2));
    }

    // --- Window ---

    [Test]
    public async Task GetHotnessAsync_window_is_positive_after_operations()
    {
        var grain = CreateGrain();
        await grain.GetAsync("key");

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Window, Is.GreaterThan(TimeSpan.Zero));
    }

    // --- GetHotnessAsync does not affect counters ---

    [Test]
    public async Task GetHotnessAsync_does_not_increment_any_counter()
    {
        var grain = CreateGrain();
        await grain.GetHotnessAsync();
        await grain.GetHotnessAsync();

        var hotness = await grain.GetHotnessAsync();
        Assert.That(hotness.Reads, Is.EqualTo(0));
        Assert.That(hotness.Writes, Is.EqualTo(0));
    }
}

