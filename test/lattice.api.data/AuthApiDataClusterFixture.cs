using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Membership;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.Data.Tests;

/// <summary>
/// A single-silo <see cref="TestCluster"/> wired with the core lattice, the
/// write-capable data API, the membership add-on (with a deterministic in-test
/// authenticator), and the authorization add-on - so the enforcing
/// <see cref="ILatticeAccessGate"/> is live and every data-API mutation and read
/// is authorized as the ambient caller's subject. Over in-memory grain storage;
/// no network or external store is involved.
/// </summary>
internal sealed class AuthApiDataClusterFixture
{
    /// <summary>A bootstrap administrator subject id configured on the silo (root-of-trust bypass).</summary>
    public const string BootstrapAdmin = "root-admin";

    /// <summary>The deployed test cluster.</summary>
    public TestCluster Cluster { get; private set; } = null!;

    /// <summary>The primary in-process silo's service provider.</summary>
    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    /// <summary>The data-API read-write facade under test.</summary>
    public ILatticeDataApi Api => SiloServices.GetRequiredService<ILatticeDataApi>();

    /// <summary>The silo-side authorization policy store (rule authoring).</summary>
    public ILatticeAuthorizationPolicyStore Store =>
        SiloServices.GetRequiredService<ILatticeAuthorizationPolicyStore>();

    private CompiledPolicySnapshotMaintainer Maintainer =>
        SiloServices.GetRequiredService<CompiledPolicySnapshotMaintainer>();

    /// <summary>
    /// Stamps <paramref name="subject"/> as the ambient caller for the lifetime of
    /// the returned scope, so data-API operations inside it are authorized as that
    /// subject.
    /// </summary>
    public static IDisposable AsSubject(string subject) =>
        LatticeCredentialContext.Use(subject, scheme: ApiDataTestCredentialAuthenticator.Scheme);

    /// <summary>
    /// Authors one or more rules and forces a synchronous compiled-policy rebuild
    /// so a test observes them without polling the asynchronous change feed.
    /// </summary>
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

    /// <summary>Registers an empty tree and returns the grain handle.</summary>
    public async Task<ILattice> RegisterTreeAsync(string treeId)
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

    /// <summary>
    /// Reads the raw value at <paramref name="key"/> under the bootstrap admin, so
    /// a test can assert durability independently of the caller-scoped facade read
    /// path.
    /// </summary>
    public async Task<byte[]?> ReadRawAsync(string treeId, string key)
    {
        var tree = Cluster.Client.GetGrain<ILattice>(treeId);
        using (AsSubject(BootstrapAdmin))
        {
            return await tree.GetAsync(key);
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
            siloBuilder.AddLatticeDataApi();
            siloBuilder.AddLatticeMembership();
            siloBuilder.Services.AddSingleton<ILatticeCredentialAuthenticator, ApiDataTestCredentialAuthenticator>();
            siloBuilder.AddLatticeAuth(options =>
            {
                options.DefaultEffect = LatticeEffect.Deny;
                options.BootstrapAdministrators.Add(BootstrapAdmin);
            });
        }
    }
}
