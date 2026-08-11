using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> with the deny-by-default control plane
/// and all-trees grants enabled
/// (<see cref="LatticeAuthOptions.AllTreesGrantsEnabled"/>). Used to prove the
/// facade honours a cluster-wide <c>Tree:*</c> grant: a bootstrap administrator
/// authors an all-trees rule, and the facade's Explain / EffectivePermissions /
/// per-tree listing then reflect the resolved verdict for an application tree.
/// Over in-memory grain storage; no network or external store is involved.
/// </summary>
internal sealed class AuthAdminAllTreesClusterFixture
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

    /// <summary>The silo-side compiled-policy snapshot maintainer.</summary>
    internal CompiledPolicySnapshotMaintainer Maintainer =>
        SiloServices.GetRequiredService<CompiledPolicySnapshotMaintainer>();

    /// <summary>
    /// Forces a synchronous rebuild of the compiled policy snapshot so a test
    /// observes an authored rule without polling the asynchronous change-feed.
    /// </summary>
    public Task<long> RebuildPolicyAsync() => Maintainer.RebuildNowAsync();

    /// <summary>Stamps <paramref name="subject"/> as the ambient caller for the returned scope.</summary>
    public static IDisposable AsSubject(string subject) =>
        LatticeCredentialContext.Use(subject, scheme: AuthApiTestCredentialAuthenticator.Scheme);

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
                options.AllTreesGrantsEnabled = true;
            });
            siloBuilder.AddLatticeAuthApi();
        }
    }
}
