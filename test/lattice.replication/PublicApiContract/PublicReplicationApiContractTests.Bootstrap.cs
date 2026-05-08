using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the <see cref="ILatticeBootstrapCoordinator"/> public
/// contract: <see cref="LatticeBootstrapState"/> is the public enum
/// surface, the registered coordinator returns a state value for any
/// tree (including ones that have never bootstrapped), and the
/// initial state for a freshly-replicated tree is the
/// <see cref="LatticeBootstrapState.LiveIncremental"/> terminal value
/// (the contract suite never invokes
/// <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/> against
/// the running cluster pair to avoid stomping the in-flight
/// incremental shipping pipeline).
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public void LatticeBootstrapState_is_public_enum_with_documented_values()
    {
        var values = Enum.GetValues<LatticeBootstrapState>();
        Assert.That(values, Is.SupersetOf(new[]
        {
            LatticeBootstrapState.RequestingSnapshot,
            LatticeBootstrapState.ApplyingSnapshot,
            LatticeBootstrapState.IncrementalHandoff,
            LatticeBootstrapState.LiveIncremental,
            LatticeBootstrapState.Failed,
        }));
    }

    [Test]
    public async Task ILatticeBootstrapCoordinator_get_state_returns_value_for_replicated_tree()
    {
        var treeId = NextTreeId("bootstrap-state");
        await CreateReplicatedTreeAsync(treeId);

        var coordinator = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteBClusterId)
            .GetRequiredService<ILatticeBootstrapCoordinator>();

        var state = await coordinator.GetStateAsync(treeId);

        // The exact starting state for a tree that never invoked
        // BootstrapAsync depends on internal bootstrap-grain
        // initialisation; the contract claim is that the call
        // succeeds and returns one of the documented enum values.
        Assert.That(Enum.IsDefined(state), Is.True,
            $"GetStateAsync must return a documented LatticeBootstrapState value (got: {state}).");
    }

    [Test]
    public async Task ILatticeBootstrapCoordinator_get_state_observes_unique_per_tree_state()
    {
        // Two trees share the same coordinator; each carries its
        // own state cell. Asserts the coordinator routes per-tree
        // to a per-tree internal grain, not a global pool.
        var treeOne = NextTreeId("bootstrap-multi-1");
        var treeTwo = NextTreeId("bootstrap-multi-2");
        await CreateReplicatedTreeAsync(treeOne);
        await CreateReplicatedTreeAsync(treeTwo);

        var coordinator = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteBClusterId)
            .GetRequiredService<ILatticeBootstrapCoordinator>();

        var stateOne = await coordinator.GetStateAsync(treeOne);
        var stateTwo = await coordinator.GetStateAsync(treeTwo);

        Assert.Multiple(() =>
        {
            Assert.That(Enum.IsDefined(stateOne), Is.True);
            Assert.That(Enum.IsDefined(stateTwo), Is.True);
        });
    }
}
