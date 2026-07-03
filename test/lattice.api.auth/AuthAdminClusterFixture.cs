using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, the
/// membership and authorization add-ons (with a deterministic in-test
/// authenticator), and the control facade under test - so the enforcing
/// <see cref="ILatticeAccessGate"/> is live and every facade operation is
/// authorized as the ambient caller's subject. Over in-memory grain storage; no
/// network or external store is involved.
/// </summary>
internal sealed class AuthAdminClusterFixture
{
    /// <summary>A bootstrap administrator subject id configured on the silo (root-of-trust bypass).</summary>
    public const string BootstrapAdmin = "root-admin";

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The control facade under test.</summary>
    public ILatticeAuthAdmin Admin => SiloServices.GetRequiredService<ILatticeAuthAdmin>();

    /// <summary>The silo-side authorization policy store (rule authoring outside the facade).</summary>
    public ILatticeAuthorizationPolicyStore Store =>
        SiloServices.GetRequiredService<ILatticeAuthorizationPolicyStore>();

    /// <summary>The silo-side access gate (the enforced decision engine the facade explains).</summary>
    public ILatticeAccessGate Gate => SiloServices.GetRequiredService<ILatticeAccessGate>();

    /// <summary>The silo-side membership directory (subject resolution outside the facade).</summary>
    public ILatticeMembershipDirectory Directory =>
        SiloServices.GetRequiredService<ILatticeMembershipDirectory>();

    private CompiledPolicySnapshotMaintainer Maintainer =>
        SiloServices.GetRequiredService<CompiledPolicySnapshotMaintainer>();

    /// <summary>
    /// Stamps <paramref name="subject"/> as the ambient caller for the lifetime of
    /// the returned scope, so facade operations inside it are authorized as that
    /// subject.
    /// </summary>
    public static IDisposable AsSubject(string subject) =>
        LatticeCredentialContext.Use(subject, scheme: AuthApiTestCredentialAuthenticator.Scheme);

    /// <summary>
    /// Authors one or more rules directly on the store (bypassing the facade) and
    /// forces a synchronous compiled-policy rebuild so a test observes them without
    /// polling the asynchronous change feed.
    /// </summary>
    public async Task GrantAsync(params LatticeAuthorizationRule[] rules)
    {
        foreach (var rule in rules)
        {
            await Store.PutRuleAsync(rule);
        }

        await Maintainer.RebuildNowAsync();
    }

    /// <summary>Forces a synchronous compiled-policy rebuild.</summary>
    public Task RebuildAsync() => Maintainer.RebuildNowAsync();

    /// <summary>Deploys the cluster.</summary>
    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
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
            siloBuilder.ConfigureLattice(o =>
            {
                o.DigestCoalescingWindowMs = 0;
                o.WalPartitions = 1;
            });
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeMembership();
            siloBuilder.Services.AddSingleton<ILatticeCredentialAuthenticator, AuthApiTestCredentialAuthenticator>();
            siloBuilder.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add(BootstrapAdmin);
            });
            siloBuilder.AddLatticeAuthApi();
        }
    }
}
