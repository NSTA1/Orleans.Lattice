using System.Collections.Concurrent;
using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4524: a snapshot bootstrap's drain records every saga decision row
/// the export carried on the receiver's transaction registry, so a pre-cut
/// prepare the source's retained log re-ships afterwards still settles. The
/// rows used to be kept forever. The export now carries its own tails on the
/// trailer, and the receiver forgets the rows once the incremental stream from
/// the source has passed them on every partition - its shipper vouched
/// acknowledged positions at or past every tail - and not before. Two real
/// clusters: site A exports through the real remote snapshot service and
/// cross-tree gate, site B imports through the real bootstrap coordinator. Site
/// B's trees keep no decision retention, so a forgotten row's tombstone expires
/// at once and reads Indeterminate, exactly as an aged-out decision does.
/// </summary>
[TestFixture]
[Category("Integration")]
public class ImportedDecisionRetirementIntegrationTests
{
    private const string SiteAClusterId = "idr-site-a";
    private const string SiteBClusterId = "idr-site-b";
    private const string SagaOrigin = "idr-origin";
    private const string PassTree = "idr-pass";
    private const string OtherLogTree = "idr-other-log";
    private const string DirectTree = "idr-direct";

    private static readonly ConcurrentDictionary<string, IRemoteSnapshotTransport> SiteATransports = new();

    private TestCluster _siteA = null!;
    private TestCluster _siteB = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var aBuilder = new TestClusterBuilder(initialSilosCount: 1);
        aBuilder.AddSiloBuilderConfigurator<SiteASiloConfigurator>();
        _siteA = aBuilder.Build();
        await _siteA.DeployAsync();

        var provider = new LatticeSnapshotProvider(
            _siteA.Client,
            new InMemoryWalCursorRegistry(),
            LatticeSnapshotProviderUnitTests.TestOptions());
        SiteATransports[SiteAClusterId] = new LatticeRemoteSnapshotService(
            provider,
            new StubReplicationContext(SiteAClusterId, LatticeMergeMode.LwwRegister),
            NullLogger<LatticeRemoteSnapshotService>.Instance)
        {
            ExportGate = new CrossTreeExportGate(_siteA.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services)
            {
                SiblingFilterForTesting = static _ => false,
            },
        };

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

    [Test]
    public async Task Imported_decision_rows_are_retained_until_the_stream_passes_the_exports_cut_then_retired()
    {
        var txid = await SettleAbortedSagaOnSiteAAsync(PassTree);
        var tails = await SiteATailsAsync(PassTree);
        Assert.That(tails.IsEmpty, Is.False, "precondition: the exported log holds the saga");

        await ImportAsync(PassTree);
        var retirement = Retirement(PassTree);
        var afterImport = await ReceiverStatusAsync(PassTree, txid);
        var heldWithNoPositions = await retirement.RetireAsync();

        var frontier = _siteB.Client.GetGrain<IReplicationTreeFrontierGrain>(PassTree);
        var epoch = await frontier.ObserveAsync(SiteAClusterId, null);
        await frontier.ObserveAsync(SiteAClusterId, Vouched(epoch, tails.PhysicalTreeId, JustBelow(tails.Tails)));
        var heldJustBelow = await retirement.RetireAsync();
        var statusJustBelow = await ReceiverStatusAsync(PassTree, txid);

        await frontier.ObserveAsync(SiteAClusterId, Vouched(epoch, tails.PhysicalTreeId, tails.Tails));
        var heldAtTails = await retirement.RetireAsync();

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(afterImport, Is.EqualTo(TxStatus.Aborted), "the drain recorded the exported decision");
            Assert.That(heldWithNoPositions, Is.EqualTo(1), "nothing has vouched the post-import stream yet");
            Assert.That(heldJustBelow, Is.EqualTo(1), "one partition short of the cut retains the row");
            Assert.That(statusJustBelow, Is.EqualTo(TxStatus.Aborted), "a retained row still settles a re-shipped pre-cut prepare");
            Assert.That(heldAtTails, Is.Zero, "at or past every tail the row is retired");
            Assert.That(await ReceiverStatusAsync(PassTree, txid), Is.EqualTo(TxStatus.Indeterminate), "the retired row was forgotten: its tombstone no longer reports the verdict");
        });
    }

    [Test]
    public async Task Positions_on_another_log_do_not_retire_imported_rows()
    {
        var txid = await SettleAbortedSagaOnSiteAAsync(OtherLogTree);
        var tails = await SiteATailsAsync(OtherLogTree);

        await ImportAsync(OtherLogTree);
        var frontier = _siteB.Client.GetGrain<IReplicationTreeFrontierGrain>(OtherLogTree);
        var epoch = await frontier.ObserveAsync(SiteAClusterId, null);
        await frontier.ObserveAsync(
            SiteAClusterId, Vouched(epoch, "another-log", [.. Enumerable.Repeat(long.MaxValue / 2, tails.Tails.Length)]));

        Assert.Multiple(async () =>
        {
            Assert.That(await Retirement(OtherLogTree).RetireAsync(), Is.EqualTo(1));
            Assert.That(await ReceiverStatusAsync(OtherLogTree, txid), Is.EqualTo(TxStatus.Aborted));
        });
    }

    [Test]
    public async Task Rows_from_a_source_that_captured_no_cut_are_retained_and_an_empty_cut_retires_at_once()
    {
        var retained = Guid.NewGuid();
        var retired = Guid.NewGuid();
        foreach (var txid in new[] { retained, retired })
        {
            await TxRegistryRouting.GetRegistry(_siteB.Client, DirectTree, txid).MarkAbortedAsync(txid);
        }

        var frontier = _siteB.Client.GetGrain<IReplicationTreeFrontierGrain>(DirectTree);
        await _siteB.Client.GetLatticeRegistry().RegisterAsync(
            DirectTree, new BPlusTree.State.TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        var epoch = await frontier.ObserveAsync("idr-legacy", null);
        await frontier.ObserveAsync("idr-legacy", Vouched(epoch, DirectTree, [.. Enumerable.Repeat(long.MaxValue / 2, 4)]));

        var retirement = Retirement(DirectTree);
        await retirement.RegisterAsync("idr-legacy", exportBoundary: null, [retained]);
        await retirement.RegisterAsync(
            "idr-empty",
            new CrossTreeSiblingBoundary { PhysicalTreeId = DirectTree, Tails = [0, 0, 0, 0], ExportEpoch = 0 },
            [retired]);

        Assert.Multiple(async () =>
        {
            Assert.That(await retirement.RetireAsync(), Is.EqualTo(1), "a source without a cut never vouches its rows away");
            Assert.That(await ReceiverStatusAsync(DirectTree, retained), Is.EqualTo(TxStatus.Aborted));
            Assert.That(await ReceiverStatusAsync(DirectTree, retired), Is.EqualTo(TxStatus.Indeterminate),
                "a log that held nothing at the cut has no pre-cut prepare to re-ship");
        });
    }

    private IImportedDecisionRetirementGrain Retirement(string tree) =>
        _siteB.Client.GetGrain<IImportedDecisionRetirementGrain>(tree);

    private Task<TxStatus> ReceiverStatusAsync(string tree, Guid txid) =>
        TxRegistryRouting.GetRegistry(_siteB.Client, tree, txid).GetStatusAsync(txid);

    private async Task<CrossTreeSiblingBoundary> SiteATailsAsync(string tree)
    {
        var gate = new CrossTreeExportGate(_siteA.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services);
        return await gate.CaptureExportBoundaryAsync(tree, CancellationToken.None);
    }

    /// <summary>Runs a two-key saga on site A's <paramref name="tree"/> to an abort, both terminals drained.</summary>
    private async Task<Guid> SettleAbortedSagaOnSiteAAsync(string tree)
    {
        var keyA = $"{tree}-a";
        var keyB = $"{tree}-b";
        while (LatticeSharding.GetShardIndex(keyA, LatticeConstants.DefaultShardCount)
            == LatticeSharding.GetShardIndex(keyB, LatticeConstants.DefaultShardCount))
        {
            keyB += "x";
        }

        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync($"{tree}-plain", [9]);
        var txid = Guid.NewGuid();
        var hlc = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks };
        var apply = _siteA.Client.GetGrain<IReplicationApplyGrain>(tree);
        await apply.ApplyPreparedSetAsync(
            keyA, [1], hlc, SagaOrigin, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 0);
        await apply.ApplyPreparedSetAsync(
            keyB, [2], hlc, SagaOrigin, sourceVectorClock: null,
            expiresAtTicks: 0, txid, atomicBatchSize: 2, atomicBatchIndex: 1);
        foreach (var key in new[] { keyA, keyB })
        {
            await apply.ApplyTxTerminalAsync(
                txid, committed: false, LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount),
                hlc with { Counter = 1 }, SagaOrigin);
        }

        return txid;
    }

    private async Task ImportAsync(string tree)
    {
        var coordinator = _siteB.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree);
        await coordinator.BootstrapAsync(SiteAClusterId, CancellationToken.None);
        var deadline = Environment.TickCount64 + 60_000;
        LatticeBootstrapState state;
        do
        {
            await Task.Delay(200);
            state = await coordinator.GetStateAsync(CancellationToken.None);
        }
        while (state != LatticeBootstrapState.LiveIncremental && state != LatticeBootstrapState.Failed
            && Environment.TickCount64 < deadline);

        Assert.That(state, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "precondition: the import completed");
    }

    private static ImmutableArray<long> JustBelow(ImmutableArray<long> tails)
    {
        var highest = tails.IndexOf(tails.Max());
        return tails.SetItem(highest, tails[highest] - 1);
    }

    private static ReplicationSourceFrontier Vouched(Guid lineage, string physical, ImmutableArray<long> positions) => new()
    {
        ReceiverLineage = lineage,
        TreeLowWatermark = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks },
        OriginLowWatermark = HybridLogicalClock.Zero,
        OriginGeneration = 1,
        AckedPositions = new ReplicationAckedPositions { PhysicalTreeId = physical, Positions = positions },
    };

    private sealed class SiteASiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = SiteAClusterId);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllResolver>();
        }
    }

    private sealed class SiteBSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            foreach (var tree in new[] { PassTree, OtherLogTree, DirectTree })
            {
                siloBuilder.ConfigureLattice(tree, o => o.TxDecisionRetention = TimeSpan.Zero);
            }

            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = SiteBClusterId);
            if (SiteATransports.TryGetValue(SiteAClusterId, out var transport))
            {
                siloBuilder.Services.AddSingleton(transport);
            }

            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllResolver>();
        }
    }

    private sealed class AllowAllResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
