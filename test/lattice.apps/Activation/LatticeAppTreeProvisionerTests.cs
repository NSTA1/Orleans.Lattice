using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Apps.Tests;

[TestFixture]
public sealed class LatticeAppTreeProvisionerTests
{
    [TestCase(true, false)]
    [TestCase(false, false)]
    [TestCase(false, true)]
    public async Task Ensure_registers_in_one_call_and_recovers_only_an_existing_deleted_tree(bool created, bool deleted)
    {
        const string tree = "app-records";
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        var deletion = Substitute.For<ITreeDeletionGrain>();
        var lattice = Substitute.For<ILattice>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        factory.GetGrain<ITreeDeletionGrain>(tree).Returns(deletion);
        factory.GetGrain<ILattice>(tree).Returns(lattice);
        registry.RegisterAndGetEntryAsync(tree, Arg.Any<TreeRegistryEntry?>())
            .Returns((created, new TreeRegistryEntry { ShardCount = 7 }));
        deletion.IsDeletedAsync().Returns(deleted);
        var systemOrigin = false;
        registry.When(r => r.RegisterAndGetEntryAsync(tree, Arg.Any<TreeRegistryEntry?>()))
            .Do(_ => systemOrigin = LatticeAccessGateContext.IsSystemOrigin);

        await new LatticeAppTreeProvisioner(factory).EnsureAsync(tree,
            new AppTreeDeclaration { Name = "records", ShardCount = 2 }, CancellationToken.None);

        await registry.Received(1).RegisterAndGetEntryAsync(tree,
            Arg.Is<TreeRegistryEntry?>(entry => entry != null && entry.ShardCount == 2));
        await registry.DidNotReceive().ExistsAsync(Arg.Any<string>());
        await registry.DidNotReceive().RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>());
        await registry.DidNotReceive().GetEntryAsync(Arg.Any<string>());
        await deletion.Received(created ? 0 : 1).IsDeletedAsync();
        await lattice.Received(!created && deleted ? 1 : 0).RecoverTreeAsync(CancellationToken.None);
        Assert.That(systemOrigin, Is.True);
        Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False);
    }

    [Test]
    public void Ensure_registration_failure_propagates_without_recovery()
    {
        var factory = Substitute.For<IGrainFactory>();
        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.RegisterAndGetEntryAsync("records", Arg.Any<TreeRegistryEntry?>())
            .ThrowsAsync(new InvalidOperationException("registry unavailable"));

        Assert.That(() => new LatticeAppTreeProvisioner(factory).EnsureAsync("records",
            new AppTreeDeclaration { Name = "records" }, CancellationToken.None),
            Throws.TypeOf<InvalidOperationException>().With.Message.EqualTo("registry unavailable"));
        Assert.That(LatticeAccessGateContext.IsSystemOrigin, Is.False);
    }
}
