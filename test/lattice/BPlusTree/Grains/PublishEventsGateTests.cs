using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="PublishEventsGate"/>. These pin the system-tree
/// short-circuit that prevents a registry-deadlock regression: system trees
/// (e.g. <c>_lattice_trees</c>) must not route gate lookups through the
/// non-reentrant registry activation that is currently servicing the write.
/// </summary>
[TestFixture]
public sealed class PublishEventsGateTests
{
    [Test]
    public void IsEnabledAsync_system_tree_returns_silo_option_without_touching_grain_factory()
    {
        var gate = new PublishEventsGate();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(Arg.Any<string>())
            .Throws(new InvalidOperationException("registry must not be consulted for system trees"));
        var opts = new LatticeOptions { PublishEvents = true };

        var task = gate.IsEnabledAsync(factory, LatticeConstants.RegistryTreeId, opts);

        Assert.That(task.IsCompletedSuccessfully, Is.True);
        Assert.That(task.Result, Is.True);
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILatticeRegistry>(default!);
    }

    [Test]
    public void IsEnabledAsync_system_tree_honours_disabled_silo_option()
    {
        var gate = new PublishEventsGate();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(Arg.Any<string>())
            .Throws(new InvalidOperationException("registry must not be consulted for system trees"));
        var opts = new LatticeOptions { PublishEvents = false };

        var task = gate.IsEnabledAsync(factory, "_lattice_anything", opts);

        Assert.That(task.IsCompletedSuccessfully, Is.True);
        Assert.That(task.Result, Is.False);
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILatticeRegistry>(default!);
    }

    [Test]
    public void IsEnabledAsync_user_tree_does_not_short_circuit()
    {
        var gate = new PublishEventsGate();
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(null));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var opts = new LatticeOptions { PublishEvents = true };

        // User tree names (no "_lattice_" prefix) must consult the registry.
        var result = gate.IsEnabledAsync(factory, "user-tree", opts).AsTask().GetAwaiter().GetResult();

        Assert.That(result, Is.True);
        factory.Received().GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
    }

    [Test]
    public void IsEnabledAsync_null_arguments_throw()
    {
        var gate = new PublishEventsGate();
        var factory = Substitute.For<IGrainFactory>();
        var opts = new LatticeOptions();

        Assert.That(() => gate.IsEnabledAsync(null!, "t", opts), Throws.ArgumentNullException);
        Assert.That(() => gate.IsEnabledAsync(factory, null!, opts), Throws.ArgumentNullException);
        Assert.That(() => gate.IsEnabledAsync(factory, "t", null!), Throws.ArgumentNullException);
    }
}
