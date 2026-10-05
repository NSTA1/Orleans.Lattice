using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4683: a per-tree bootstrap or re-seed of one participant of a
/// replicated cross-tree atomic write settles that tree's sub-saga from the
/// export, in place of its terminal. The receiver's cross-tree barrier, which a
/// sibling tree may already have delegated to, must hear of it as it would of
/// the terminal, and the imported tree must stay read-fenced until the barrier
/// decides: otherwise the receiver serves the write split, the imported tree
/// post-saga and its sibling pre-saga, permanently once the imported tree's
/// terminal was trimmed at the source. Two real clusters: site A authors the
/// cross-tree write through the public API and exports through the real remote
/// snapshot service; site B imports one tree through the real bootstrap
/// coordinator, and receives the sibling tree's records through the replication
/// apply seam the live stream uses. Built from the #4442 confirmation probe.
/// </summary>
[TestFixture]
[Category("Integration")]
public class CrossTreeImportBarrierIntegrationTests
{
    private const string SiteAClusterId = "xtib-site-a";
    private const string SiteBClusterId = "xtib-site-b";

    private static readonly ConcurrentDictionary<string, IRemoteSnapshotTransport> SiteATransports = new();

    private TestCluster _siteA = null!;
    private TestCluster _siteB = null!;
    private LatticeSnapshotProvider _siteAProvider = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var aBuilder = new TestClusterBuilder(initialSilosCount: 1);
        aBuilder.AddSiloBuilderConfigurator<SiteASiloConfigurator>();
        _siteA = aBuilder.Build();
        await _siteA.DeployAsync();

        _siteAProvider = new LatticeSnapshotProvider(
            _siteA.Client,
            new InMemoryWalCursorRegistry(),
            LatticeSnapshotProviderUnitTests.TestOptions());
        SiteATransports[SiteAClusterId] = new LatticeRemoteSnapshotService(
            _siteAProvider,
            new StubReplicationContext(SiteAClusterId, LatticeMergeMode.LwwRegister),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        var bBuilder = new TestClusterBuilder(initialSilosCount: 1);
        bBuilder.AddSiloBuilderConfigurator<SiteBSiloConfigurator>();
        _siteB = bBuilder.Build();
        await _siteB.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_siteB is not null)
        {
            await _siteB.StopAllSilosAsync();
            await _siteB.DisposeAsync();
        }

        if (_siteA is not null)
        {
            await _siteA.StopAllSilosAsync();
            await _siteA.DisposeAsync();
        }

        SiteATransports.TryRemove(SiteAClusterId, out _);
    }

    private static int ShardOf(string key) => LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);

    private Task AuthorCrossTreeWriteAsync(string treeA, string treeB, string operationId) =>
        _siteA.Client.SetManyAtomicAsync(
            [
                new LatticeTreeBatch(treeA, [new("k", [1])]),
                new LatticeTreeBatch(treeB, [new("k", [2])]),
            ],
            operationId);

    /// <summary>Delivers tree B's sub-saga on site B as the live stream does: its prepare, then its cross-tree terminal.</summary>
    private async Task DeliverTreeBAsync(string treeA, string treeB, string operationId)
    {
        var txid = Guid.NewGuid();
        var stamp = HybridLogicalClock.Tick(new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks });
        var apply = _siteB.Client.GetGrain<IReplicationApplyGrain>(treeB);
        await apply.ApplyPreparedSetAsync(
            "k", [2], stamp, SiteAClusterId,
            sourceVectorClock: null, expiresAtTicks: 0, txid, atomicBatchSize: 0, atomicBatchIndex: 0);
        await apply.ApplyTxTerminalAsync(
            txid, committed: true, ShardOf("k"), HybridLogicalClock.Tick(stamp), SiteAClusterId,
            atomicShardCount: 0, crossTreeOperationId: operationId, crossTreeWaitSet: [treeA, treeB]);
    }

    private ILatticeBootstrapCoordinatorGrain Coordinator(string tree) =>
        _siteB.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree);

    private async Task StartBootstrapAsync(string tree) =>
        await Coordinator(tree).BootstrapAsync(SiteAClusterId, CancellationToken.None);

    private async Task<LatticeBootstrapState> AwaitPhaseAsync(string tree, params LatticeBootstrapState[] phases)
    {
        var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(60).TotalMilliseconds;
        LatticeBootstrapState state;
        do
        {
            await Task.Delay(200);
            state = await Coordinator(tree).GetStateAsync(CancellationToken.None);
        }
        while (!phases.Contains(state) && state != LatticeBootstrapState.Failed && Environment.TickCount64 < deadline);

        return state;
    }

    /// <summary>Reads <paramref name="key"/>, or reports the tree read-fenced.</summary>
    private async Task<(bool Fenced, byte[]? Value)> ReadAsync(string tree, string key)
    {
        try
        {
            return (false, await _siteB.Client.GetGrain<ILattice>(tree).GetAsync(key));
        }
        catch (LatticeTreeBootstrappingException)
        {
            return (true, null);
        }
    }

    [Test]
    public async Task Importing_one_cross_tree_participant_completes_the_barrier_its_sibling_delegated_to()
    {
        const string treeA = "xtib-delegated-a";
        const string treeB = "xtib-delegated-b";
        const string operationId = "xtib-delegated-op";
        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);

        // Tree B's terminal arrived first and delegated to the barrier, which
        // waits for tree A. Tree A's terminal was trimmed at the source, so tree
        // A reaches site B only through a bootstrap.
        await DeliverTreeBAsync(treeA, treeB, operationId);
        await StartBootstrapAsync(treeA);
        var phase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental);

        var a = await ReadAsync(treeA, "k");
        var b = await ReadAsync(treeB, "k");
        Assert.Multiple(() =>
        {
            Assert.That(phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "the import completes once the barrier decides");
            Assert.That(a, Is.EqualTo((false, (byte[]?)new byte[] { 1 })), "tree A is imported post-saga");
            Assert.That(b, Is.EqualTo((false, (byte[]?)new byte[] { 2 })),
                "tree B must flip with tree A: the import is tree A's arrival at the barrier");
        });
    }

    [Test]
    public async Task An_imported_cross_tree_participant_stays_read_fenced_until_its_sibling_terminal_arrives()
    {
        const string treeA = "xtib-fenced-a";
        const string treeB = "xtib-fenced-b";
        const string operationId = "xtib-fenced-op";
        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);

        // Tree A is imported while tree B's terminal is still on its own stream.
        await StartBootstrapAsync(treeA);
        var heldPhase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.IncrementalHandoff, LatticeBootstrapState.LiveIncremental);
        await Task.Delay(1500);
        var whileHeld = await ReadAsync(treeA, "k");
        var siblingWhileHeld = await ReadAsync(treeB, "k");
        var statusWhileHeld = await Coordinator(treeA).GetStatusAsync(CancellationToken.None);

        await DeliverTreeBAsync(treeA, treeB, operationId);
        var released = await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental);
        var a = await ReadAsync(treeA, "k");
        var b = await ReadAsync(treeB, "k");

        Assert.Multiple(() =>
        {
            Assert.That(heldPhase, Is.Not.EqualTo(LatticeBootstrapState.Failed));
            Assert.That(siblingWhileHeld, Is.EqualTo((false, (byte[]?)null)), "precondition: tree B is pre-saga");
            Assert.That(whileHeld.Fenced, Is.True,
                "tree A must not be served post-saga while tree B is pre-saga: it stays read-fenced until the barrier decides");
            Assert.That(statusWhileHeld.ReadFenced, Is.True);
            Assert.That(released, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "the fence lifts once tree B's terminal decides the barrier");
            Assert.That(a, Is.EqualTo((false, (byte[]?)new byte[] { 1 })));
            Assert.That(b, Is.EqualTo((false, (byte[]?)new byte[] { 2 })));
        });
    }

    [Test]
    public async Task A_re_driven_import_and_a_late_real_terminal_re_record_the_arrival_idempotently()
    {
        const string treeA = "xtib-idempotent-a";
        const string treeB = "xtib-idempotent-b";
        const string operationId = "xtib-idempotent-op";
        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);
        await DeliverTreeBAsync(treeA, treeB, operationId);
        await StartBootstrapAsync(treeA);
        Assert.That(await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental), Is.EqualTo(LatticeBootstrapState.LiveIncremental));

        // A re-seed of tree A drains the same decision row again.
        await StartBootstrapAsync(treeA);
        var again = await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental);

        // Tree A's real terminal, retained after all, arrives late.
        var txid = (await ExportedCrossTreeDecisionAsync(treeA, operationId)).TransactionId;
        await _siteB.Client.GetGrain<IReplicationApplyGrain>(treeA).ApplyTxTerminalAsync(
            txid, committed: true, ShardOf("k"),
            HybridLogicalClock.Tick(new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks }), SiteAClusterId,
            atomicShardCount: 0, crossTreeOperationId: operationId, crossTreeWaitSet: [treeA, treeB]);

        Assert.Multiple(async () =>
        {
            Assert.That(again, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "a re-driven import re-records the same arrival");
            Assert.That(await ReadAsync(treeA, "k"), Is.EqualTo((false, (byte[]?)new byte[] { 1 })));
            Assert.That(await ReadAsync(treeB, "k"), Is.EqualTo((false, (byte[]?)new byte[] { 2 })));
        });
    }

    [Test]
    public async Task The_export_names_the_cross_tree_operation_on_its_sub_saga_decision_row()
    {
        const string treeA = "xtib-export-a";
        const string treeB = "xtib-export-b";
        const string operationId = "xtib-export-op";
        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);
        await _siteA.Client.GetGrain<ILattice>(treeA).SetManyAtomicAsync([new("single-tree", [3])]);

        var exported = await ExportAsync(treeA);
        var decisions = exported.Where(e => e.IsDecision).ToList();
        var named = decisions.Where(e => e.CrossTreeOperationId is not null).ToList();

        Assert.Multiple(() =>
        {
            Assert.That(decisions, Has.Count.EqualTo(2), "precondition: the cross-tree and the single-tree saga are both stored");
            Assert.That(named, Has.Count.EqualTo(1), "only the cross-tree sub-saga's row names an operation");
            Assert.That(named[0].CrossTreeOperationId, Is.EqualTo(operationId));
            Assert.That(named[0].CrossTreeParticipants, Is.EqualTo(new[] { treeA, treeB }));
        });
    }

    private async Task<SnapshotEntry> ExportedCrossTreeDecisionAsync(string tree, string operationId) =>
        (await ExportAsync(tree)).Single(e => e.IsDecision && e.CrossTreeOperationId == operationId);

    private async Task<List<SnapshotEntry>> ExportAsync(string tree)
    {
        var exported = new List<SnapshotEntry>();
        var stream = await _siteAProvider.ExportAsync(tree, HybridLogicalClock.Zero);
        await foreach (var entry in stream.Entries)
        {
            exported.Add(entry);
        }

        return exported;
    }

    private sealed class SiteASiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = SiteAClusterId);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class SiteBSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = SiteBClusterId);
            if (SiteATransports.TryGetValue(SiteAClusterId, out var transport))
            {
                siloBuilder.Services.AddSingleton(transport);
            }

            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
