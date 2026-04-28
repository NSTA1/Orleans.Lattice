using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of the public <see cref="LatticeBootstrapCoordinator"/>
/// façade. The façade is a thin forwarder over the per-tree
/// <see cref="ILatticeBootstrapCoordinatorGrain"/> activation; the
/// state-machine behaviour is tested directly on the grain in
/// <see cref="Grains.LatticeBootstrapCoordinatorGrainTests"/>.
/// </summary>
[TestFixture]
public class LatticeBootstrapCoordinatorTests
{
    private const string Tree = "boot-tree";
    private const string SourceCluster = "site-a";

    private static (
        LatticeBootstrapCoordinator Coordinator,
        IGrainFactory Factory,
        ILatticeBootstrapCoordinatorGrain Grain) Create()
    {
        var factory = Substitute.For<IGrainFactory>();
        var grain = Substitute.For<ILatticeBootstrapCoordinatorGrain>();
        factory.GetGrain<ILatticeBootstrapCoordinatorGrain>(Arg.Any<string>()).Returns(grain);
        return (new LatticeBootstrapCoordinator(factory), factory, grain);
    }

    [Test]
    public void Constructor_throws_when_grain_factory_is_null()
    {
        Assert.That(
            () => new LatticeBootstrapCoordinator(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void GetStateAsync_throws_when_tree_name_is_null()
    {
        var (coord, _, _) = Create();
        Assert.That(
            async () => await coord.GetStateAsync(null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetStateAsync_throws_when_tree_name_is_empty()
    {
        var (coord, _, _) = Create();
        Assert.That(
            async () => await coord.GetStateAsync(string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetStateAsync_observes_cancellation_before_dispatch()
    {
        var (coord, _, _) = Create();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await coord.GetStateAsync(Tree, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetStateAsync_resolves_grain_by_tree_name_and_forwards_call()
    {
        var (coord, factory, grain) = Create();
        grain.GetStateAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(LatticeBootstrapState.LiveIncremental));

        var state = await coord.GetStateAsync(Tree);

        Assert.That(state, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
        factory.Received(1).GetGrain<ILatticeBootstrapCoordinatorGrain>(Tree);
        await grain.Received(1).GetStateAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public void BootstrapAsync_throws_when_tree_name_is_null()
    {
        var (coord, _, _) = Create();
        Assert.That(
            async () => await coord.BootstrapAsync(null!, SourceCluster),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BootstrapAsync_throws_when_tree_name_is_empty()
    {
        var (coord, _, _) = Create();
        Assert.That(
            async () => await coord.BootstrapAsync(string.Empty, SourceCluster),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BootstrapAsync_throws_when_source_cluster_id_is_null()
    {
        var (coord, _, _) = Create();
        Assert.That(
            async () => await coord.BootstrapAsync(Tree, null!),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BootstrapAsync_throws_when_source_cluster_id_is_empty()
    {
        var (coord, _, _) = Create();
        Assert.That(
            async () => await coord.BootstrapAsync(Tree, string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void BootstrapAsync_observes_cancellation_before_dispatch()
    {
        var (coord, _, _) = Create();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(
            async () => await coord.BootstrapAsync(Tree, SourceCluster, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task BootstrapAsync_resolves_grain_by_tree_name_and_forwards_call()
    {
        var (coord, factory, grain) = Create();
        grain.BootstrapAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        await coord.BootstrapAsync(Tree, SourceCluster);

        factory.Received(1).GetGrain<ILatticeBootstrapCoordinatorGrain>(Tree);
        await grain.Received(1).BootstrapAsync(SourceCluster, Arg.Any<CancellationToken>());
    }
}
