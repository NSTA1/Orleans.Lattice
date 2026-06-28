using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Single-silo fixture for the per-key change-history endpoint
/// (<see cref="ILatticeStateQuery.GetEntryHistoryAsync"/>). Wires
/// <c>AddLattice</c> + <c>AddLatticeViews</c> + <c>AddLatticeStateApi</c> plus an
/// OR-Set merge-mode resolver (trees prefixed <c>orset</c>) so a history read
/// can exercise the durable history-view source, every retention mode, and the
/// CRDT member-change decode path end-to-end. Exposes the operator-style
/// history-view create / drain helpers so a revision timeline is materialised
/// before each read.
/// </summary>
internal sealed class EntryHistoryClusterFixture
{
    public const int SmallMaxLeafKeys = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public IServiceProvider SiloServices =>
        Cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    public ILatticeStateQuery Query => SiloServices.GetRequiredService<ILatticeStateQuery>();

    private ILatticeViewFactory Factory => SiloServices.GetRequiredService<ILatticeViewFactory>();

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

    public async Task<ILattice> RegisterTreeAsync(string treeId, int shardCount = 1)
    {
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            ShardCount = shardCount,
            MaxLeafKeys = SmallMaxLeafKeys,
            WalPartitions = 1,
        });

        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    public ILattice Source(string treeId) => Cluster.Client.GetGrain<ILattice>(treeId);

    /// <summary>
    /// Creates a durable per-key history view over <paramref name="sourceTreeId"/>
    /// and establishes the maintainer's write-ahead-log cursor before any source
    /// write so each mutation is tailed as its own revision.
    /// </summary>
    public async Task CreateHistoryViewAsync(string sourceTreeId, string viewName)
    {
        var source = Cluster.Client.GetGrain<ILattice>(sourceTreeId);
        Factory.Create(source, viewName, LatticeHistoryView.Definition(viewName, SiloServices));

        var maintainer = Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
    }

    /// <summary>Drains the history view maintainer until it has caught up to the source head.</summary>
    public async Task DrainToZeroAsync(string viewName)
    {
        var maintainer = Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        for (var attempt = 0; attempt < 50; attempt++)
        {
            await maintainer.DrainAsync();
            if (await maintainer.GetLagAsync() == 0)
            {
                return;
            }

            await Task.Delay(20);
        }

        Assert.Fail($"History view '{viewName}' did not catch up to the source head.");
    }

    public static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);

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
            siloBuilder.AddLatticeViews();
            siloBuilder.Services.ConfigureAll<LatticeViewOptions>(o =>
            {
                o.CoalesceWindow = TimeSpan.FromMinutes(5);
                o.ReadHandleCacheTtl = TimeSpan.FromMilliseconds(50);
                o.OldGenerationReclaimGrace = TimeSpan.FromMilliseconds(200);
            });
            siloBuilder.AddLatticeStateApi();
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, OrSetHistoryPrefixMergeModeResolver>();
        }
    }
}

/// <summary>
/// Test <see cref="ILatticeMergeModeResolver"/> that declares any tree whose id
/// starts with <c>"orset"</c> as an <see cref="LatticeMergeMode.OrSet"/> tree so
/// the history endpoint can prove the per-revision CRDT member-change decode
/// path without standing up the full replication package.
/// </summary>
internal sealed class OrSetHistoryPrefixMergeModeResolver : ILatticeMergeModeResolver
{
    public LatticeMergeMode? Resolve(string treeId) =>
        treeId.StartsWith("orset", StringComparison.Ordinal)
            ? LatticeMergeMode.OrSet
            : null;
}
