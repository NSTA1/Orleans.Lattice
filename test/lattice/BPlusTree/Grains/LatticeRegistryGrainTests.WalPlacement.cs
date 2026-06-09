using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the WAL placement read/compare-and-swap surface of
/// <see cref="Orleans.Lattice.BPlusTree.Grains.LatticeRegistryGrain"/>:
/// <c>GetWalPlacementAsync</c> and the version-checked
/// <c>UpdateWalPlacementAsync</c>.
/// </summary>
public partial class LatticeRegistryGrainTests
{
    // Wires the registry tree substitute as a tiny in-memory key/value store so
    // a Get after an Update observes the bytes the Update persisted.
    private static void BackWithInMemoryStore(ISystemLattice tree)
    {
        var store = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        tree.ExistsAsync(Arg.Any<string>()).Returns(ci => store.ContainsKey(ci.Arg<string>()));
        tree.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(ci =>
        {
            store[ci.ArgAt<string>(0)] = ci.ArgAt<byte[]>(1);
            return Task.CompletedTask;
        });
        tree.GetAsync(Arg.Any<string>()).Returns(ci =>
            Task.FromResult(store.TryGetValue(ci.Arg<string>(), out var bytes) ? bytes : null));
    }

    [Test]
    public async Task GetWalPlacementAsync_returns_default_pin_for_unregistered_tree()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("ghost").Returns(Task.FromResult<byte[]?>(null));

        var pin = await grain.GetWalPlacementAsync("ghost");

        Assert.That(pin.Version, Is.EqualTo(0));
        Assert.That(pin.ResolveKey(0), Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
    }

    [Test]
    public async Task UpdateWalPlacementAsync_flips_partition_and_bumps_version()
    {
        var (grain, tree) = CreateGrain();
        BackWithInMemoryStore(tree);
        await grain.RegisterAsync("tree-a", new TreeRegistryEntry { ShardCount = 1 });

        var updated = await grain.UpdateWalPlacementAsync("tree-a", expectedVersion: 0, partition: 0, providerKey: "secondary");

        Assert.That(updated.Version, Is.EqualTo(1));
        Assert.That(updated.ResolveKey(0), Is.EqualTo("secondary"));

        // The flip is durable: a fresh read observes the new placement.
        var reread = await grain.GetWalPlacementAsync("tree-a");
        Assert.That(reread.Version, Is.EqualTo(1));
        Assert.That(reread.ResolveKey(0), Is.EqualTo("secondary"));
    }

    [Test]
    public void UpdateWalPlacementAsync_rejects_stale_expected_version()
    {
        var (grain, tree) = CreateGrain();
        BackWithInMemoryStore(tree);

        Assert.That(async () =>
        {
            await grain.RegisterAsync("tree-b", new TreeRegistryEntry { ShardCount = 1 });
            await grain.UpdateWalPlacementAsync("tree-b", expectedVersion: 0, partition: 0, providerKey: "secondary");
            // Second update still passes the now-stale version 0 (current is 1).
            await grain.UpdateWalPlacementAsync("tree-b", expectedVersion: 0, partition: 0, providerKey: "third");
        }, Throws.InvalidOperationException);
    }

    [Test]
    public async Task UpdateWalPlacementAsync_reversal_to_default_restores_default_key()
    {
        var (grain, tree) = CreateGrain();
        BackWithInMemoryStore(tree);
        await grain.RegisterAsync("tree-c", new TreeRegistryEntry { ShardCount = 1 });

        await grain.UpdateWalPlacementAsync("tree-c", 0, 0, "secondary");
        var reverted = await grain.UpdateWalPlacementAsync("tree-c", 1, 0, IWalStorageProviderCatalog.DefaultProviderKey);

        Assert.That(reverted.Version, Is.EqualTo(2));
        Assert.That(reverted.ResolveKey(0), Is.EqualTo(IWalStorageProviderCatalog.DefaultProviderKey));
        Assert.That(reverted.Overrides, Is.Null);
    }
}
