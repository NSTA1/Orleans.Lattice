using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cluster fixture for the defense-in-depth internal-origin assertion on the
/// physical shard and leaf grains (issue #1103). It registers a real (non-null)
/// access gate - which activates the internal-origin guard - together with the
/// capability-stripping incoming call filter that the authorization layer
/// installs, so the guard sees exactly the trust-boundary signal it would in a
/// real auth-enabled cluster: a facade-to-shard silo hop is stamped internal and
/// passes, while a direct external client call to a shard or leaf key carries no
/// internal-origin marker (any forged one is stripped) and is refused.
/// </summary>
public sealed class InternalOriginGuardClusterFixture
{
    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            // Last-wins override of the null gate registered by AddLattice with a
            // real allow-all gate, so the shard / leaf internal-origin guard is
            // active (it short-circuits only for the null gate) while the
            // data-plane stays open for the facade-success path.
            siloBuilder.Services.AddSingleton<ILatticeAccessGate, AlwaysAllowAccessGate>();
            // Install the same capability-stripping filter the auth layer registers
            // so the internal-origin marker is derived from the real caller identity.
            siloBuilder.AddIncomingGrainCallFilter<LatticeCapabilityStrippingCallFilter>();
            // Register the sentinel that activates the shard / leaf internal-origin
            // assertion (the auth layer registers it beside the filter).
            siloBuilder.Services.AddSingleton<LatticeInternalOriginEnforcementMarker>();
        }
    }
}

/// <summary>
/// A minimal non-null <see cref="ILatticeAccessGate"/> that allows every
/// data-plane request. Being non-null is what activates the shard / leaf
/// internal-origin guard (which no-ops only for the default null gate); the
/// allow-all decision keeps the facade data path open so the guard, not the
/// gate, is the property under test.
/// </summary>
public sealed class AlwaysAllowAccessGate : ILatticeAccessGate
{
    /// <inheritdoc />
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default)
        => new(LatticeAccessDecision.Allow());
}
