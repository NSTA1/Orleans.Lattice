using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Operations;
using Orleans.Lattice.Tests.Fakes;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Authorization model of the tag-index reconcile coordinator (#5613). A
/// coordinator sweep is index maintenance: the facade authorizes the caller to
/// start it (Admin over the index tree), and the sweep's repairs then run under
/// system origin, as a view reconcile's maintenance writes do. A caller holding
/// Read and Admin on the index tree but no Delete must therefore see a sweep
/// that needs repairs complete, while the on-demand reconcile of the public tag
/// index surface stays bound by the caller's own grants.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TagIndexReconcileAuthorizationIntegrationTests
{
    private const LatticeOperation MutatingMask =
        LatticeOperation.Write
        | LatticeOperation.Delete
        | LatticeOperation.RangeDelete
        | LatticeOperation.CrdtApply
        | LatticeOperation.AtomicWrite
        | LatticeOperation.BulkLoad;

    private AccessGateKeyFilterClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new AccessGateKeyFilterClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [TearDown]
    public void TearDown() => ConfigurableAccessGate.Reset();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private ILattice Tree(string id) => _cluster.GrainFactory.GetGrain<ILattice>(id);

    private ILatticeTagIndex TagIndex(ILattice tree, string name) =>
        new DefaultLatticeTagIndexFactory(_cluster.GrainFactory, FakeLatticeReplicationContext.Disabled).Create(tree, name);

    private ITagIndexReconcileGrain Coordinator(string indexName) =>
        _cluster.GrainFactory.GetGrain<ITagIndexReconcileGrain>(indexName);

    // The caller of #5613: Read and Admin on the index tree, no mutation of it.
    private static void GrantOnlyReadAndAdminOn(string indexTreeId) =>
        ConfigurableAccessGate.Decide = req =>
            req.TreeId == indexTreeId && (req.Operation & MutatingMask) != 0
                ? LatticeAccessDecision.Deny("index tree mutation not granted")
                : LatticeAccessDecision.Allow();

    private async Task<(ILatticeTagIndex Index, string IndexName)> SeedOrphanAsync()
    {
        var sfx = Guid.NewGuid().ToString("N");
        var indexName = $"status-{sfx}";
        var tree = Tree($"orders-{sfx}");
        await tree.SetAsync("o1", Bytes("1"));
        var index = TagIndex(tree, indexName);
        await index.Key("o1").AddAsync(["open"]);
        await tree.DeleteAsync("o1");
        return (index, indexName);
    }

    private async Task<LatticeOperationTicket> BeginOperationAsync(string indexName)
    {
        var ticket = LatticeOperationTicket.For("default", LatticeOperationKey.NewId());
        await _cluster.GrainFactory.GetGrain<ILatticeOperationGrain>(ticket.OperationKey).BeginAsync(new LatticeOperationBeginRequest
        {
            Kind = "treeadmin.tag-index-reconcile",
            TreeIds = [LatticeConstants.TagIndexTreePrefix + indexName],
            Phases = [LatticeMaintenanceProgress.Probing, LatticeMaintenanceProgress.Repairing],
        });
        return ticket;
    }

    [Test]
    public async Task A_tracked_sweep_started_by_an_index_admin_without_delete_completes_its_repairs()
    {
        var (index, indexName) = await SeedOrphanAsync();
        var ticket = await BeginOperationAsync(indexName);
        GrantOnlyReadAndAdminOn(LatticeConstants.TagIndexTreePrefix + indexName);

        var report = await Coordinator(indexName).RunTrackedSweepAsync(ticket);

        ConfigurableAccessGate.Reset();
        var remaining = await index.WithAnyTags("open").CountAsync();
        Assert.Multiple(() =>
        {
            Assert.That(report.OrphanRowsRemoved, Is.GreaterThanOrEqualTo(1));
            Assert.That(remaining, Is.Zero);
        });
    }

    [Test]
    public async Task A_sweep_driven_without_any_caller_grant_still_completes_its_repairs()
    {
        var (index, indexName) = await SeedOrphanAsync();
        var indexTreeId = LatticeConstants.TagIndexTreePrefix + indexName;
        ConfigurableAccessGate.Decide = req => req.TreeId == indexTreeId
            ? LatticeAccessDecision.Deny("no grant on the index tree")
            : LatticeAccessDecision.Allow();

        var report = await Coordinator(indexName).RunSweepAsync();

        ConfigurableAccessGate.Reset();
        var remaining = await index.WithAnyTags("open").CountAsync();
        Assert.Multiple(() =>
        {
            Assert.That(report.OrphanRowsRemoved, Is.GreaterThanOrEqualTo(1));
            Assert.That(remaining, Is.Zero);
        });
    }

    [Test]
    public async Task The_on_demand_reconcile_stays_bound_by_the_callers_own_grants()
    {
        var (index, indexName) = await SeedOrphanAsync();
        GrantOnlyReadAndAdminOn(LatticeConstants.TagIndexTreePrefix + indexName);

        Assert.That(async () => await index.ReconcileAsync(), Throws.InstanceOf<LatticeAuthorizationDeniedException>(),
            "Only the coordinator's admitted maintenance sweep runs under system origin; a direct reconcile removes rows as its caller.");
    }
}
