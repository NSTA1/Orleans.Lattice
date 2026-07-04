using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> deliberately configured with the
/// permissive data-plane default effect (<see cref="LatticeEffect.Allow"/>) so a
/// test can prove the control-plane isolation guarantee (issue #1103): the
/// reserved authorization namespace must never inherit the data-plane
/// default-allow, so an unmatched admin request is denied regardless of the
/// default effect, while ordinary data-plane reads and writes still enjoy the
/// permissive default. Over in-memory grain storage; no network or external store
/// is involved.
/// </summary>
internal sealed class AuthAdminControlPlaneClusterFixture
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

    /// <summary>
    /// Stamps <paramref name="subject"/> as the ambient caller for the lifetime of
    /// the returned scope, so facade operations inside it are authorized as that
    /// subject.
    /// </summary>
    public static IDisposable AsSubject(string subject) =>
        LatticeCredentialContext.Use(subject, scheme: AuthApiTestCredentialAuthenticator.Scheme);

    /// <summary>Registers a data tree and returns its grain handle.</summary>
    public async Task<ILattice> CreateTreeAsync(string treeId)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 4,
            ShardCount = 2,
            WalPartitions = 1,
        });

        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

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
                // Permissive data-plane default: the whole point of this fixture is
                // to prove the control plane does NOT inherit it.
                options.DefaultEffect = LatticeEffect.Allow;
                options.BootstrapAdministrators.Add(BootstrapAdmin);
            });
            siloBuilder.AddLatticeAuthApi();
        }
    }
}
