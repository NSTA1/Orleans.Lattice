using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class LatticeRegistryGrainTests
{
    private static (LatticeRegistryGrain grain, ISystemLattice registryTree) CreateGrain()
    {
        var grainFactory = Substitute.For<IGrainFactory>();
        var registryTree = Substitute.For<ISystemLattice>();
        grainFactory.GetGrain<ISystemLattice>(LatticeConstants.RegistryTreeId).Returns(registryTree);

        var grain = new LatticeRegistryGrain(grainFactory);
        return (grain, registryTree);
    }

    [Test]
    public async Task RegisterAsync_sets_key_in_registry_tree()
    {
        var (grain, tree) = CreateGrain();
        tree.ExistsAsync("my-tree").Returns(false);

        await grain.RegisterAsync("my-tree");

        await tree.Received(1).SetAsync("my-tree", Arg.Any<byte[]>());
    }

    [Test]
    public async Task RegisterAsync_is_idempotent_when_already_registered()
    {
        var (grain, tree) = CreateGrain();
        tree.ExistsAsync("my-tree").Returns(true);

        await grain.RegisterAsync("my-tree");

        await tree.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }

    [Test]
    public async Task RegisterAsync_with_entry_stores_config()
    {
        var (grain, tree) = CreateGrain();
        tree.ExistsAsync("my-tree").Returns(false);

        var entry = new TreeRegistryEntry { MaxLeafKeys = 256, MaxInternalChildren = 64 };
        await grain.RegisterAsync("my-tree", entry);

        await tree.Received(1).SetAsync("my-tree", Arg.Is<byte[]>(b =>
            b.Length > 0));
    }

    [Test]
    public async Task UpdateAsync_registers_tree_when_not_yet_registered()
    {
        var (grain, tree) = CreateGrain();

        var entry = new TreeRegistryEntry { MaxLeafKeys = 256 };
        await grain.UpdateAsync("my-tree", entry);

        await tree.Received(1).SetAsync("my-tree", Arg.Any<byte[]>());
    }

    [Test]
    public async Task UpdateAsync_sets_new_value_when_registered()
    {
        var (grain, tree) = CreateGrain();
        tree.ExistsAsync("my-tree").Returns(true);

        var entry = new TreeRegistryEntry { MaxLeafKeys = 512 };
        await grain.UpdateAsync("my-tree", entry);

        await tree.Received(1).SetAsync("my-tree", Arg.Any<byte[]>());
    }

    [Test]
    public async Task UnregisterAsync_deletes_key()
    {
        var (grain, tree) = CreateGrain();

        await grain.UnregisterAsync("my-tree");

        await tree.Received(1).DeleteAsync("my-tree");
    }

    [Test]
    public async Task ExistsAsync_delegates_to_registry_tree()
    {
        var (grain, tree) = CreateGrain();
        tree.ExistsAsync("my-tree").Returns(true);

        var result = await grain.ExistsAsync("my-tree");

        Assert.That(result, Is.True);
    }

    [Test]
    public async Task ExistsAsync_returns_false_when_not_registered()
    {
        var (grain, tree) = CreateGrain();
        tree.ExistsAsync("my-tree").Returns(false);

        var result = await grain.ExistsAsync("my-tree");

        Assert.That(result, Is.False);
    }

    [Test]
    public async Task GetEntryAsync_returns_null_when_not_registered()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));

        var result = await grain.GetEntryAsync("my-tree");

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task GetEntryAsync_roundtrips_entry()
    {
        var (grain, tree) = CreateGrain();
        var original = new TreeRegistryEntry
        {
            MaxLeafKeys = 256,
            MaxInternalChildren = 64,
            ShardCount = 32
        };

        // Capture the bytes written by RegisterAsync.
        byte[]? capturedBytes = null;
        tree.ExistsAsync("my-tree").Returns(false);
        await tree.SetAsync("my-tree", Arg.Do<byte[]>(b => capturedBytes = b));
        await grain.RegisterAsync("my-tree", original);

        // Return those bytes on GetAsync.
        tree.GetAsync("my-tree").Returns(Task.FromResult(capturedBytes));
        var result = await grain.GetEntryAsync("my-tree");

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.MaxLeafKeys, Is.EqualTo(256));
        Assert.That(result.MaxInternalChildren, Is.EqualTo(64));
        Assert.That(result.ShardCount, Is.EqualTo(32));
    }

    [Test]
    public async Task GetAllTreeIdsAsync_returns_keys_from_registry()
    {
        var (grain, tree) = CreateGrain();
        tree.KeysAsync(null, null, false).Returns(ToAsyncEnumerable("alpha", "beta", "gamma"));

        var result = await grain.GetAllTreeIdsAsync();

        Assert.That(result, Is.EqualTo(new[] { "alpha", "beta", "gamma" }));
    }

    private static async IAsyncEnumerable<string> ToAsyncEnumerable(params string[] items)
    {
        foreach (var item in items)
        {
            yield return item;
        }
        await Task.CompletedTask;
    }

    // --- Alias tests ---

    [Test]
    public async Task SetAliasAsync_updates_entry_with_physical_tree_id()
    {
        var (grain, tree) = CreateGrain();

        // Capture the bytes written by SetAlias to verify PhysicalTreeId.
        byte[]? capturedBytes = null;
        await tree.SetAsync(Arg.Any<string>(), Arg.Do<byte[]>(b => capturedBytes = b));
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));
        tree.GetAsync("physical-tree").Returns(Task.FromResult<byte[]?>(null));

        await grain.SetAliasAsync("my-tree", "physical-tree");

        // Verify the entry was written.
        await tree.Received().SetAsync("my-tree", Arg.Any<byte[]>());

        // Roundtrip: verify the stored entry has PhysicalTreeId set.
        tree.GetAsync("my-tree").Returns(Task.FromResult(capturedBytes));
        var entry = await grain.GetEntryAsync("my-tree");
        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.PhysicalTreeId, Is.EqualTo("physical-tree"));
    }

    [Test]
    public void SetAliasAsync_throws_when_target_is_aliased()
    {
        var (grain, tree) = CreateGrain();

        // Target tree has a PhysicalTreeId — it's already aliased.
        var targetEntry = new TreeRegistryEntry { PhysicalTreeId = "another-tree" };
        var targetBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(targetEntry);
        tree.GetAsync("physical-tree").Returns(Task.FromResult<byte[]?>(targetBytes));

        Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.SetAliasAsync("my-tree", "physical-tree"));
    }

    [Test]
    public void SetAliasAsync_throws_when_same_ids()
    {
        var (grain, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.SetAliasAsync("my-tree", "my-tree"));
    }

    [Test]
    public async Task RemoveAliasAsync_clears_physical_tree_id()
    {
        var (grain, tree) = CreateGrain();

        // Setup existing entry with alias.
        var existingEntry = new TreeRegistryEntry { MaxLeafKeys = 256, PhysicalTreeId = "physical-tree" };
        var existingBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(existingEntry);
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(existingBytes));

        byte[]? capturedBytes = null;
        await tree.SetAsync(Arg.Any<string>(), Arg.Do<byte[]>(b => capturedBytes = b));

        await grain.RemoveAliasAsync("my-tree");

        // Verify the updated entry has null PhysicalTreeId but preserves other fields.
        tree.GetAsync("my-tree").Returns(Task.FromResult(capturedBytes));
        var entry = await grain.GetEntryAsync("my-tree");
        Assert.That(entry, Is.Not.Null);
        Assert.That(entry!.PhysicalTreeId, Is.Null);
        Assert.That(entry.MaxLeafKeys, Is.EqualTo(256));
    }

    [Test]
    public async Task RemoveAliasAsync_noop_when_no_alias()
    {
        var (grain, tree) = CreateGrain();

        // Entry exists but has no alias.
        var existingEntry = new TreeRegistryEntry { MaxLeafKeys = 128 };
        var existingBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(existingEntry);
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(existingBytes));

        await grain.RemoveAliasAsync("my-tree");

        // Should not have written anything.
        await tree.DidNotReceive().SetAsync("my-tree", Arg.Any<byte[]>());
    }

    [Test]
    public async Task ResolveAsync_returns_physical_tree_id_when_aliased()
    {
        var (grain, tree) = CreateGrain();

        var entry = new TreeRegistryEntry { PhysicalTreeId = "physical-tree" };
        var bytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(entry);
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(bytes));

        var result = await grain.ResolveAsync("my-tree");

        Assert.That(result, Is.EqualTo("physical-tree"));
    }

    [Test]
    public async Task ResolveAsync_returns_tree_id_when_not_aliased()
    {
        var (grain, tree) = CreateGrain();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));

        var result = await grain.ResolveAsync("my-tree");

        Assert.That(result, Is.EqualTo("my-tree"));
    }

    [Test]
    public async Task GetEntryAsync_roundtrips_entry_with_physical_tree_id()
    {
        var (grain, tree) = CreateGrain();
        var original = new TreeRegistryEntry
        {
            MaxLeafKeys = 256,
            MaxInternalChildren = 64,
            ShardCount = 32,
            PhysicalTreeId = "phys-1"
        };

        byte[]? capturedBytes = null;
        tree.ExistsAsync("my-tree").Returns(false);
        await tree.SetAsync("my-tree", Arg.Do<byte[]>(b => capturedBytes = b));
        await grain.RegisterAsync("my-tree", original);

        tree.GetAsync("my-tree").Returns(Task.FromResult(capturedBytes));
        var result = await grain.GetEntryAsync("my-tree");

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.PhysicalTreeId, Is.EqualTo("phys-1"));
    }

    [Test]
    public async Task GetAllTreeIdsAsync_excludes_system_trees()
    {
        var (grain, tree) = CreateGrain();
        tree.KeysAsync(null, null, false).Returns(
            ToAsyncEnumerable("_lattice_trees", "alpha", "beta"));

        var result = await grain.GetAllTreeIdsAsync();

        Assert.That(result, Is.EqualTo(new[] { "alpha", "beta" }));
    }
}
