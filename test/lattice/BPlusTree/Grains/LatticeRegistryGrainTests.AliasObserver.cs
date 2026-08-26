using System.Text.Json;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the tree registry firing <see cref="ITreeAliasObserver"/> on
/// an effective physical-identity change (issue #1665). The observer is the
/// core-to-replication inversion that lets the cross-cluster shipper rebind
/// reactively instead of polling the registry every pump tick, so the
/// registry must fire it exactly once per genuine alias change and never on a
/// no-op.
/// </summary>
public partial class LatticeRegistryGrainTests
{
    [Test]
    public async Task SetAliasAsync_fires_observer_with_old_and_new_physical_on_first_alias()
    {
        var (grain, tree, observer) = CreateGrainWithAliasObserver();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));
        tree.GetAsync("physical-tree").Returns(Task.FromResult<byte[]?>(null));

        await grain.SetAliasAsync("my-tree", "physical-tree");

        Assert.That(observer.Changes, Has.Count.EqualTo(1));
        var change = observer.Changes[0];
        Assert.Multiple(() =>
        {
            Assert.That(change.TreeId, Is.EqualTo("my-tree"));
            Assert.That(change.OldPhysicalTreeId, Is.EqualTo("my-tree"),
                "An unaliased tree resolves to its own id, so the old effective physical is the logical id.");
            Assert.That(change.NewPhysicalTreeId, Is.EqualTo("physical-tree"));
        });
    }

    [Test]
    public async Task SetAliasAsync_fires_observer_with_prior_alias_as_old_physical()
    {
        var (grain, tree, observer) = CreateGrainWithAliasObserver();
        var existing = new TreeRegistryEntry { PhysicalTreeId = "physical-old" };
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(JsonSerializer.SerializeToUtf8Bytes(existing)));
        tree.GetAsync("physical-new").Returns(Task.FromResult<byte[]?>(null));

        await grain.SetAliasAsync("my-tree", "physical-new");

        Assert.That(observer.Changes, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(observer.Changes[0].OldPhysicalTreeId, Is.EqualTo("physical-old"));
            Assert.That(observer.Changes[0].NewPhysicalTreeId, Is.EqualTo("physical-new"));
        });
    }

    [Test]
    public async Task SetAliasAsync_does_not_fire_observer_on_noop_re_set_of_same_alias()
    {
        var (grain, tree, observer) = CreateGrainWithAliasObserver();
        var existing = new TreeRegistryEntry { PhysicalTreeId = "physical-tree" };
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(JsonSerializer.SerializeToUtf8Bytes(existing)));
        tree.GetAsync("physical-tree").Returns(Task.FromResult<byte[]?>(null));

        await grain.SetAliasAsync("my-tree", "physical-tree");

        Assert.That(observer.Changes, Is.Empty,
            "Re-setting the current alias is not an identity change and must not fire the observer.");
    }

    [Test]
    public async Task RemoveAliasAsync_fires_observer_with_logical_id_as_new_physical()
    {
        var (grain, tree, observer) = CreateGrainWithAliasObserver();
        var existing = new TreeRegistryEntry { MaxLeafKeys = 256, PhysicalTreeId = "physical-tree" };
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(JsonSerializer.SerializeToUtf8Bytes(existing)));

        await grain.RemoveAliasAsync("my-tree");

        Assert.That(observer.Changes, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(observer.Changes[0].TreeId, Is.EqualTo("my-tree"));
            Assert.That(observer.Changes[0].OldPhysicalTreeId, Is.EqualTo("physical-tree"));
            Assert.That(observer.Changes[0].NewPhysicalTreeId, Is.EqualTo("my-tree"),
                "Removing the alias repoints the logical tree back to itself.");
        });
    }

    [Test]
    public async Task RemoveAliasAsync_does_not_fire_observer_when_no_alias_present()
    {
        var (grain, tree, observer) = CreateGrainWithAliasObserver();
        var existing = new TreeRegistryEntry { MaxLeafKeys = 128 };
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(JsonSerializer.SerializeToUtf8Bytes(existing)));

        await grain.RemoveAliasAsync("my-tree");

        Assert.That(observer.Changes, Is.Empty,
            "Removing an alias from an already-unaliased tree is a no-op and must not fire the observer.");
    }

    [Test]
    public async Task SetAliasAsync_without_observer_dispatcher_does_not_throw()
    {
        // The dispatcher is optional (null when the replication package is not
        // installed); the alias path must stay correct and allocation-neutral.
        var (grain, tree) = CreateGrain();
        tree.GetAsync("my-tree").Returns(Task.FromResult<byte[]?>(null));
        tree.GetAsync("physical-tree").Returns(Task.FromResult<byte[]?>(null));

        Assert.That(() => grain.SetAliasAsync("my-tree", "physical-tree"), Throws.Nothing);
    }
}
