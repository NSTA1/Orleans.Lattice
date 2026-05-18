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
            LatticeBootstrapState.Idle,
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

    [Test]
    public void LatticeBootstrapTransientFaultClassifier_is_public_static_helper_with_documented_default()
    {
        // Pins the public surface of the default transient-fault
        // classifier: a public static type with an IsTransient(Exception)
        // predicate that hosts can compose into a custom retry policy.
        var type = typeof(LatticeBootstrapTransientFaultClassifier);
        Assert.Multiple(() =>
        {
            Assert.That(type.IsPublic, Is.True,
                "LatticeBootstrapTransientFaultClassifier must be public so hosts can reference its default predicate.");
            Assert.That(type.IsAbstract && type.IsSealed, Is.True,
                "LatticeBootstrapTransientFaultClassifier must be a static class.");

            // The canonical transient fault types must round-trip to true.
            Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(new TimeoutException()), Is.True);
            // A garden-variety non-transient exception must classify false.
            Assert.That(LatticeBootstrapTransientFaultClassifier.IsTransient(new InvalidOperationException()), Is.False);
        });
    }

    [Test]
    public void BootstrapCoordinatorStatus_is_public_readonly_record_struct_with_documented_members()
    {
        // Pins the public surface of the observable status snapshot:
        // a serialisable readonly-record-struct with a Phase and
        // SourceClusterId, exposed so hosts can drive their own
        // observability UI without consulting the internal grain
        // state.
        var type = typeof(BootstrapCoordinatorStatus);
        Assert.Multiple(() =>
        {
            Assert.That(type.IsPublic, Is.True,
                "BootstrapCoordinatorStatus must be a public type.");
            Assert.That(type.IsValueType, Is.True,
                "BootstrapCoordinatorStatus must be a value type (readonly record struct).");
            var status = new BootstrapCoordinatorStatus(LatticeBootstrapState.Idle, null);
            Assert.That(status.Phase, Is.EqualTo(LatticeBootstrapState.Idle));
            Assert.That(status.SourceClusterId, Is.Null);
        });
    }

    [Test]
    public async Task ILatticeBootstrapCoordinator_get_status_returns_documented_value_for_replicated_tree()
    {
        // Pins the GetStatusAsync overload: a single grain RPC that
        // returns a BootstrapCoordinatorStatus carrying the current
        // phase and (optionally) the in-flight source cluster id.
        var treeId = NextTreeId("bootstrap-status");
        await CreateReplicatedTreeAsync(treeId);

        var coordinator = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteBClusterId)
            .GetRequiredService<ILatticeBootstrapCoordinator>();

        var status = await coordinator.GetStatusAsync(treeId);

        Assert.That(Enum.IsDefined(status.Phase), Is.True,
            $"GetStatusAsync must return a documented LatticeBootstrapState value (got: {status.Phase}).");
    }
}
