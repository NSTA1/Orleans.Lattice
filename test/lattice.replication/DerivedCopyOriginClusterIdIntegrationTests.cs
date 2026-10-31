using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Probes the local-origin stamp on a derived physical copy (the #5300 / #5306
/// class, epic #4908). <see cref="ConfiguredLatticeOriginClusterIdResolver"/>
/// resolves <see cref="LatticeReplicationOptions.ClusterId"/> per tree, and the
/// write-ahead log's writer and reader resolve it by the record's tree id, which on
/// a copy is the copy's physical id. A per-tree override registered under the
/// logical name would then not apply to the copy, and the tree's shipper - which
/// ships only records carrying that override - would never ship the copy's writes
/// (issue #5313).
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class DerivedCopyOriginClusterIdIntegrationTests
{
    private const string Tree = "dco-resized";
    private const string HeldTree = "dco-held";
    private const string GlobalClusterId = "dco-global";
    private const string TreeClusterId = "dco-tree";

    private TestCluster _cluster = null!;

    private IServiceProvider Services => _cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task Write_on_a_resized_copy_is_stamped_with_the_logical_trees_cluster_id()
    {
        var lattice = _cluster.Client.GetGrain<ILattice>(Tree);
        await lattice.SetAsync("seed", Encoding.UTF8.GetBytes("s"));
        var resize = _cluster.Client.GetGrain<ITreeResizeGrain>(Tree);
        await resize.ResizeAsync(64, 64);
        await resize.RunResizePassAsync();
        var physical = (await _cluster.Client.GetLatticeRegistry().GetEntryAsync(Tree))?.PhysicalTreeId;
        Assert.That(physical, Is.Not.Null.And.Not.EqualTo(Tree), "precondition: the tree resolves to the resize's copy");

        await lattice.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        await AssertStampedAndShipEligibleAsync(Tree, physical!, "k");
    }

    [Test]
    public async Task Writes_on_a_bootstrap_held_copy_are_stamped_with_the_logical_trees_cluster_id()
    {
        var lattice = _cluster.Client.GetGrain<ILattice>(HeldTree);
        await lattice.SetAsync("seed", Encoding.UTF8.GetBytes("s"));
        var resize = _cluster.Client.GetGrain<ITreeResizeGrain>(HeldTree);
        var operationId = Guid.NewGuid().ToString("N");
        await resize.BeginBootstrapCopyAsync(operationId);
        await resize.RunResizePassAsync();
        Assert.That(await resize.IsBootstrapCopyReadyAsync(operationId), Is.True, "precondition: the hold is active");
        var held = await resize.GetBootstrapCopyTreeIdAsync();
        Assert.That(held, Is.Not.Null.And.Not.EqualTo(HeldTree));

        // One write mirrored onto the held copy, one routed to it directly.
        await lattice.SetAsync("mirrored", Encoding.UTF8.GetBytes("m"));
        using (LatticeBootstrapShadowRouteContext.BeginScope(HeldTree, held!))
        {
            await lattice.SetAsync("routed", Encoding.UTF8.GetBytes("r"));
        }

        await AssertStampedAndShipEligibleAsync(HeldTree, held!, "mirrored");
        await AssertStampedAndShipEligibleAsync(HeldTree, held!, "routed");
        await resize.CompleteBootstrapCopyAsync(operationId);
    }

    /// <summary>
    /// Reads <paramref name="key"/>'s records from the copy's write-ahead log and
    /// asserts each carries the logical tree's configured cluster id, so the tree's
    /// shipper - which ships only local-origin records - would ship it.
    /// </summary>
    private async Task AssertStampedAndShipEligibleAsync(string logical, string physical, string key)
    {
        var records = new List<WalRecord>();
        var partitions = await Services.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(physical);
        for (var p = 0; p < partitions; p++)
        {
            var page = await _cluster.Client.GetGrain<IWalShardGrain>($"{physical}/{p}").ReadAsync(0, 1024, CancellationToken.None);
            records.AddRange(page.Entries.Where(e => e.Entry.Key == key).Select(e => e.Entry));
        }

        var shipperClusterId = Services.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>().Get(logical).ClusterId;
        Assert.Multiple(() =>
        {
            Assert.That(shipperClusterId, Is.EqualTo(TreeClusterId), "precondition: the per-tree override applies to the logical tree");
            Assert.That(records.Select(r => r.OriginClusterId), Is.Not.Empty.And.All.EqualTo(TreeClusterId),
                $"'{key}' committed on the copy carries the cluster id configured for its logical tree");
            Assert.That(records.Where(r => !ReplicationShipEligibility.IsShipEligible(r.OriginClusterId, r.Op, shipperClusterId)).Select(r => r.Key),
                Is.Empty, $"the tree's shipper ships '{key}' as a local-origin record");
        });
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = GlobalClusterId;
                opts.AutoBootstrapOnFallOffLog = false;
                opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
                {
                    [Tree] = LatticeMergeMode.LwwRegister,
                    [HeldTree] = LatticeMergeMode.LwwRegister,
                };
            });
            siloBuilder.ConfigureLatticeReplication(Tree, opts => opts.ClusterId = TreeClusterId);
            siloBuilder.ConfigureLatticeReplication(HeldTree, opts => opts.ClusterId = TreeClusterId);
        }
    }
}
