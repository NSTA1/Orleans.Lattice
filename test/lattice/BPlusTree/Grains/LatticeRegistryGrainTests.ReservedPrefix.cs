using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Reserved tree-name prefix guards. Ensures user-supplied tree IDs
/// beginning with <see cref="LatticeConstants.SystemTreePrefix"/> (which
/// subsumes <see cref="LatticeConstants.ReplogTreePrefix"/>) are rejected at
/// the registry so downstream internal trees (the registry itself, the
/// replication WAL) have a collision-free namespace.
/// </summary>
public partial class LatticeRegistryGrainTests
{
    [Test]
    public void RegisterAsync_throws_when_tree_id_starts_with_system_prefix()
    {
        var (grain, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.RegisterAsync("_lattice_my_tree"));
    }

    [Test]
    public void RegisterAsync_throws_when_tree_id_starts_with_replog_prefix()
    {
        var (grain, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.RegisterAsync("_lattice_replog_mytree"));
    }

    [Test]
    public void RegisterAsync_throws_when_tree_id_equals_registry_tree_id()
    {
        var (grain, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.RegisterAsync(LatticeConstants.RegistryTreeId));
    }

    [Test]
    public void RegisterAsync_throws_with_entry_when_tree_id_is_reserved()
    {
        var (grain, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.RegisterAsync("_lattice_foo", new TreeRegistryEntry { MaxLeafKeys = 32 }));
    }

    [Test]
    public void UpdateAsync_throws_when_tree_id_starts_with_system_prefix()
    {
        var (grain, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.UpdateAsync("_lattice_my_tree", new TreeRegistryEntry()));
    }

    [Test]
    public void UpdateAsync_throws_when_tree_id_starts_with_replog_prefix()
    {
        var (grain, _) = CreateGrain();

        Assert.ThrowsAsync<ArgumentException>(
            () => grain.UpdateAsync("_lattice_replog_mytree", new TreeRegistryEntry()));
    }

    [Test]
    public async Task RegisterAsync_accepts_tree_id_with_embedded_prefix()
    {
        // Only a *leading* underscore-prefixed namespace is reserved. A tree
        // ID that merely contains the literal elsewhere is a valid user name.
        var (grain, tree) = CreateGrain();
        tree.ExistsAsync("my_lattice_replog_tree").Returns(false);

        await grain.RegisterAsync("my_lattice_replog_tree");

        await tree.Received(1).SetAsync("my_lattice_replog_tree", Arg.Any<byte[]>());
    }

    [Test]
    public void ReplogTreePrefix_is_subsumed_by_SystemTreePrefix()
    {
        Assert.That(
            LatticeConstants.ReplogTreePrefix.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal),
            Is.True);
    }
}