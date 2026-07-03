using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> like <see cref="AuthClusterFixture"/>
/// but with the audit sink and the durable <c>sys-auth-audit</c> trail enabled at
/// deny-only verbosity, so the observability integration tests can assert that
/// gated denials are recorded to the durable tree and that a configured
/// time-to-live is honoured. The trail time-to-live is supplied per fixture
/// instance.
/// </summary>
public sealed class AuditClusterFixture
{
    // Read by the silo configurator (which Orleans instantiates parameterless)
    // when the cluster is built inside InitializeAsync.
    private static TimeSpan? _configuredTtl;

    /// <summary>A bootstrap administrator subject id configured on the silo.</summary>
    public const string BootstrapAdmin = "root-admin";

    private readonly TimeSpan? _trailTimeToLive;

    /// <summary>Initializes the fixture with an optional durable-trail time-to-live.</summary>
    /// <param name="trailTimeToLive">The TTL applied to each audit row, or <c>null</c> for none.</param>
    public AuditClusterFixture(TimeSpan? trailTimeToLive = null)
    {
        _trailTimeToLive = trailTimeToLive;
    }

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>Opens a client-side <see cref="ILattice"/> handle for <paramref name="treeId"/>.</summary>
    public ILattice Lattice(string treeId) => Cluster.Client.GetGrain<ILattice>(treeId);

    /// <summary>Stamps <paramref name="subject"/> as the ambient caller for the returned scope.</summary>
    public static IDisposable AsSubject(string subject) =>
        LatticeCredentialContext.Use(subject, scheme: TestCredentialAuthenticator.Scheme, metadata: null);

    /// <summary>Deploys the cluster.</summary>
    public async Task InitializeAsync()
    {
        _configuredTtl = _trailTimeToLive;
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    /// <summary>Stops and disposes the cluster.</summary>
    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeMembership();
            siloBuilder.Services.AddSingleton<ILatticeCredentialAuthenticator, TestCredentialAuthenticator>();
            siloBuilder.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add(BootstrapAdmin);
                options.EnableAuditSink = true;
                options.AuditVerbosity = LatticeAuthAuditVerbosity.DenyOnly;
                options.EnableDurableAuditTrail = true;
                options.AuditTrailTimeToLive = _configuredTtl;
            });
        }
    }
}
