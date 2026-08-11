using Orleans.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> identical to <see cref="AuthClusterFixture"/>
/// except that access-administration delegation is enabled
/// (<see cref="LatticeAuthOptions.AccessAdministrationDelegationEnabled"/> is set),
/// so the policy store permits authoring the one narrow delegation rule shape - a
/// whole-tree <c>Admin</c> grant on the reserved <c>sys-auth-policy</c> tree. Used
/// to prove the store honours the enabled option end-to-end.
/// </summary>
public sealed class AuthDelegationClusterFixture
{
    /// <summary>A bootstrap administrator subject id configured on the silo (root-of-trust).</summary>
    public const string BootstrapAdmin = "root-admin";

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The silo-side authorization policy store.</summary>
    public ILatticeAuthorizationPolicyStore Store =>
        SiloServices.GetRequiredService<ILatticeAuthorizationPolicyStore>();

    /// <summary>Deploys the cluster.</summary>
    public async Task InitializeAsync()
    {
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
                options.AccessAdministrationDelegationEnabled = true;
            });
        }
    }
}
