using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Cluster fixture that overrides the default allow-all
/// <see cref="ILatticeAccessGate"/> with the test-controllable
/// <see cref="ConfigurableAccessGate"/> (a last-wins <c>AddSingleton</c> after
/// <c>AddLattice</c> has registered the null gate), so the read-path key-filter
/// seam can be driven per-test. Each test scopes its decision to its own tree id
/// so unrelated trees (and internal system trees) keep the allow-all default.
/// </summary>
public sealed class AccessGateKeyFilterClusterFixture
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
            // Last-wins override of the null gate registered by AddLattice.
            siloBuilder.Services.AddSingleton<ILatticeAccessGate, ConfigurableAccessGate>();
        }
    }
}

/// <summary>
/// A test <see cref="ILatticeAccessGate"/> whose per-request decision is driven
/// by a mutable static delegate. Defaults to a plain allow (no key-filter) so a
/// test that does not opt in behaves exactly like the null gate.
/// </summary>
public sealed class ConfigurableAccessGate : ILatticeAccessGate
{
    /// <summary>
    /// The per-request decision function. Tests set this in <c>SetUp</c> and
    /// reset it in <c>TearDown</c>. Defaults to allow-all.
    /// </summary>
    public static Func<LatticeAccessRequest, LatticeAccessDecision> Decide { get; set; }
        = static _ => LatticeAccessDecision.Allow();

    /// <summary>Restores the default allow-all decision.</summary>
    public static void Reset() => Decide = static _ => LatticeAccessDecision.Allow();

    /// <inheritdoc />
    public ValueTask<LatticeAccessDecision> AuthorizeAsync(
        in LatticeAccessRequest request,
        CancellationToken cancellationToken = default)
    {
        // Copy the in-parameter to a local so it can be passed by value to the
        // decision delegate.
        var req = request;
        return new ValueTask<LatticeAccessDecision>(Decide(req));
    }
}
