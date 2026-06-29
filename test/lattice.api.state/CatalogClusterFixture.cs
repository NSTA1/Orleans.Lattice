using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Single-silo fixture for the discovery / catalog endpoint. Registers the core
/// lattice, the state API, and the materialised-view infrastructure so the
/// catalog can enumerate both trees (via the registry) and views (via the view
/// catalog). Exposes helpers to register trees, soft-delete them, set aliases,
/// and create views.
/// </summary>
internal sealed class CatalogClusterFixture
{
    public TestCluster Cluster { get; private set; } = null!;

    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    public ILatticeStateQuery Query => SiloServices.GetRequiredService<ILatticeStateQuery>();

    private ILatticeRegistry Registry =>
        Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    public async Task InitializeAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        if (Cluster is not null)
        {
            await Cluster.StopAllSilosAsync();
            await Cluster.DisposeAsync();
        }
    }

    /// <summary>Registers a tree with optional structural overrides.</summary>
    public Task RegisterTreeAsync(string treeId, int? shardCount = null, int? maxLeafKeys = null) =>
        Registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            ShardCount = shardCount,
            MaxLeafKeys = maxLeafKeys,
        });

    /// <summary>Registers a tree and writes <paramref name="keyCount"/> keys.</summary>
    public async Task<ILattice> CreatePopulatedTreeAsync(string treeId, int keyCount, int shardCount = 2)
    {
        await RegisterTreeAsync(treeId, shardCount, maxLeafKeys: 4);
        var tree = Cluster.Client.GetGrain<ILattice>(treeId);
        for (var i = 0; i < keyCount; i++)
        {
            await tree.SetAsync($"key-{i:D5}", new byte[] { (byte)i });
        }

        return tree;
    }

    /// <summary>Soft-deletes a populated tree through the public surface.</summary>
    public Task SoftDeleteTreeAsync(string treeId) =>
        Cluster.Client.GetGrain<ILattice>(treeId).DeleteTreeAsync();

    /// <summary>Points <paramref name="logicalTreeId"/> at <paramref name="physicalTreeId"/>.</summary>
    public Task SetAliasAsync(string logicalTreeId, string physicalTreeId) =>
        Registry.SetAliasAsync(logicalTreeId, physicalTreeId);

    /// <summary>Registers a reserved <c>view-</c> backing tree directly in the registry.</summary>
    public Task RegisterViewBackingTreeAsync(string viewTreeId) =>
        Registry.RegisterAsync(viewTreeId, new TreeRegistryEntry { ShardCount = 1 });

    /// <summary>Registers a reserved <c>tag-</c> index backing tree directly in the registry.</summary>
    public Task RegisterTagIndexTreeAsync(string indexName, int shardCount = 1) =>
        Registry.RegisterAsync(LatticeConstants.TagIndexTreePrefix + indexName, new TreeRegistryEntry { ShardCount = shardCount });

    /// <summary>Opens the tag index <paramref name="indexName"/> bound to <paramref name="sourceTreeId"/>.</summary>
    public ILatticeTagIndex CreateTagIndex(string sourceTreeId, string indexName)
    {
        var factory = SiloServices.GetRequiredService<ILatticeTagIndexFactory>();
        var source = Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        return factory.Create(source, indexName);
    }

    /// <summary>Creates a runtime materialised view over the given source tree.</summary>
    public ILatticeView CreateView(string sourceTreeId, string viewName)
    {
        var factory = SiloServices.GetRequiredService<ILatticeViewFactory>();
        var source = Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        return factory.Create(source, viewName, new LatticeViewDefinition(viewName, new PredicateLatticeViewProjection()));
    }

    /// <summary>
    /// Registers an OR-Set source tree (its id is prefixed <c>orset</c> so the
    /// fixture's merge-mode resolver declares it an <see cref="LatticeMergeMode.OrSet"/>)
    /// and writes one OR-Set element under <paramref name="key"/>, returning the tree.
    /// </summary>
    public async Task<ILattice> CreateOrSetSourceTreeAsync(string treeId, string key, string element)
    {
        await RegisterTreeAsync(treeId, shardCount: 1, maxLeafKeys: 4);
        var tree = Cluster.Client.GetGrain<ILattice>(treeId);
        await tree.OrSet(key).AddAsync(System.Text.Encoding.UTF8.GetBytes(element), "replica-a");
        return tree;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeViews();
            siloBuilder.AddLatticeStateApi();
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, OrSetPrefixMergeModeResolver>();
        }
    }
}
