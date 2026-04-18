using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeRegistryGrainTests
{
    // --- Shard map persistence tests ---

    [Test]
    public async Task GetShardMapAsync_returns_null_when_not_registered()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));

        var result = await grain.GetShardMapAsync("my-tree");

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task GetShardMapAsync_returns_null_when_entry_has_no_map()
    {
        var (grain, tree) = CreateGrain();
        var entry = new TreeRegistryEntry { MaxLeafKeys = 128 };
        var bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(entry);
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(bytes));

        var result = await grain.GetShardMapAsync("my-tree");

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task SetShardMapAsync_persists_map_to_entry()
    {
        var (grain, tree) = CreateGrain();
        var map = ShardMap.CreateDefault(8, 4);

        byte[]? captured = null;
        await tree.SetAsync(Arg.Any<string>(), Arg.Do<byte[]>(b => captured = b));
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));

        await grain.SetShardMapAsync("my-tree", map);

        // Roundtrip — Set should have been called and the persisted entry
        // should contain the shard map.
        await tree.Received().SetAsync("my-tree", Arg.Any<byte[]>());
        tree.GetAsync("my-tree").Returns(Task.FromResult(captured));
        var roundtrip = await grain.GetShardMapAsync("my-tree");
        Assert.That(roundtrip, Is.Not.Null);
        Assert.That(roundtrip!.Slots, Is.EqualTo(map.Slots));
    }

    [Test]
    public async Task SetShardMapAsync_preserves_other_entry_fields()
    {
        var (grain, tree) = CreateGrain();
        var existing = new TreeRegistryEntry { MaxLeafKeys = 256, ShardCount = 32 };
        var existingBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(existing);
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(existingBytes));

        byte[]? captured = null;
        await tree.SetAsync(Arg.Any<string>(), Arg.Do<byte[]>(b => captured = b));

        await grain.SetShardMapAsync("my-tree", ShardMap.CreateDefault(8, 4));

        tree.GetAsync("my-tree").Returns(Task.FromResult(captured));
        var entry = await grain.GetEntryAsync("my-tree");
        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.MaxLeafKeys, Is.EqualTo(256));
        Assert.That(entry.ShardCount, Is.EqualTo(32));
        Assert.That(entry.ShardMap, Is.Not.Null);
    }

    [Test]
    public void GetShardMapAsync_throws_when_tree_id_null()
    {
        var (grain, _) = CreateGrain();
        Assert.That(() => grain.GetShardMapAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void SetShardMapAsync_throws_when_tree_id_null()
    {
        var (grain, _) = CreateGrain();
        Assert.That(
            () => grain.SetShardMapAsync(null!, ShardMap.CreateDefault(8, 4)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void SetShardMapAsync_throws_when_map_null()
    {
        var (grain, _) = CreateGrain();
        Assert.That(
            () => grain.SetShardMapAsync("my-tree", null!),
            Throws.ArgumentNullException);
    }
}
