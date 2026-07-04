using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, the
/// materialised-view infrastructure, the read-only state API, the membership
/// add-on (with a deterministic in-test authenticator), and the authorization
/// add-on - so a state-API read of a <c>view-*</c> tree exercises the view-read
/// authorization boundary (issue #1103): a view read binds under a view-read
/// scope that bypasses the data-plane gate, so the read must instead be
/// authorized by the readability of the view's SOURCE tree. Over in-memory grain
/// storage; no network or external store is involved.
/// </summary>
internal sealed class AuthViewApiStateClusterFixture
{
    /// <summary>A bootstrap administrator subject id configured on the silo (root-of-trust bypass).</summary>
    public const string BootstrapAdmin = "root-admin";

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The state-API read facade under test.</summary>
    public ILatticeStateQuery Query => SiloServices.GetRequiredService<ILatticeStateQuery>();

    /// <summary>The silo-side authorization policy store (rule authoring).</summary>
    public ILatticeAuthorizationPolicyStore Store =>
        SiloServices.GetRequiredService<ILatticeAuthorizationPolicyStore>();

    private CompiledPolicySnapshotMaintainer Maintainer =>
        SiloServices.GetRequiredService<CompiledPolicySnapshotMaintainer>();

    /// <summary>
    /// Stamps <paramref name="subject"/> as the ambient caller for the lifetime of
    /// the returned scope, so state-API reads inside it are authorized as that
    /// subject.
    /// </summary>
    public static IDisposable AsSubject(string subject) =>
        LatticeCredentialContext.Use(subject, scheme: ApiStateTestCredentialAuthenticator.Scheme);

    /// <summary>Authors rules and forces a synchronous compiled-policy rebuild.</summary>
    public async Task GrantAsync(params LatticeAuthorizationRule[] rules)
    {
        foreach (var rule in rules)
        {
            await Store.PutRuleAsync(rule);
        }

        await Maintainer.RebuildNowAsync();
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

    /// <summary>
    /// Registers <paramref name="treeId"/> and writes <paramref name="keys"/> (each
    /// a distinct single-byte value) under the bootstrap administrator, so the
    /// write itself is authorized. Returns the grain handle.
    /// </summary>
    public async Task<ILattice> CreatePopulatedTreeAsync(string treeId, params string[] keys)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = 4,
            ShardCount = 2,
            WalPartitions = 1,
        });

        var tree = Cluster.Client.GetGrain<ILattice>(treeId);
        using (AsSubject(BootstrapAdmin))
        {
            byte value = 0;
            foreach (var key in keys)
            {
                await tree.SetAsync(key, new[] { value++ });
            }
        }

        return tree;
    }

    /// <summary>
    /// Creates and materialises a key-preserving predicate view over
    /// <paramref name="sourceTreeId"/>. Its reserved backing tree id is
    /// <c>view-{viewName}</c>. The rebuild runs under the bootstrap administrator so
    /// the view maintainer's source reads are authorized.
    /// </summary>
    public async Task<string> CreateViewAsync(string sourceTreeId, string viewName)
    {
        var factory = SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        var view = factory.Create(
            source, viewName, new LatticeViewDefinition(viewName, new PredicateLatticeViewProjection()));

        using (AsSubject(BootstrapAdmin))
        {
            await view.RebuildAsync();
        }

        return LatticeConstants.ViewTreePrefix + viewName;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeViews();
            siloBuilder.AddLatticeStateApi();
            siloBuilder.AddLatticeMembership();
            siloBuilder.Services.AddSingleton<ILatticeCredentialAuthenticator, ApiStateTestCredentialAuthenticator>();
            siloBuilder.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add(BootstrapAdmin);
            });
        }
    }
}
