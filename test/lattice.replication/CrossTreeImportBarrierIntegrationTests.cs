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
/// the terminal, and the imported shadow must not be published until the barrier
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
public partial class CrossTreeImportBarrierIntegrationTests
{
    private const string SiteAClusterId = "xtib-site-a";
    private const string SiteBClusterId = "xtib-site-b";

    /// <summary>A site-B tree configured with a cluster id of its own, so a barrier spanning it is refused.</summary>
    private const string ElsewhereTree = "xtib-ack-elsewhere";

    private static readonly ConcurrentDictionary<string, IRemoteSnapshotTransport> SiteATransports = new();

    /// <summary>Site B's replication topology, so a test can re-add site A at runtime.</summary>
    private static readonly FakeReplicationTopology SiteBTopology = new();

    /// <summary>The tree site B replicates statically, so its driver activation service subscribes to the topology.</summary>
    private const string ActivationAnchorTree = "xtib-activation-anchor";

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
        // Served under site A's real cross-tree export gate (#4684), so the
        // import carries the premise the receiver's barrier relies on.
        SiteATransports[SiteAClusterId] = new PausableTransport(new LatticeRemoteSnapshotService(
            _siteAProvider,
            new StubReplicationContext(SiteAClusterId, LatticeMergeMode.LwwRegister),
            NullLogger<LatticeRemoteSnapshotService>.Instance)
        {
            // These tests cover the barrier, not the sibling boundaries (R1),
            // which have their own fixture: no sibling holds a fence here.
            ExportGate = new CrossTreeExportGate(_siteA.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services)
            {
                SiblingFilterForTesting = static _ => false,
            },
        });

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

    /// <summary>
    /// A window longer than two ticks of the coordinator's 2 s phase timer
    /// (<c>CoordinatorGrain.PhaseTimerPeriod</c>), each of which re-checks the
    /// import's cross-tree hold before publishing the shadow copy.
    /// </summary>
    private static readonly TimeSpan HeldWindow = TimeSpan.FromSeconds(5);

    /// <summary>The fewest samples a <see cref="HeldWindow"/> must yield to count as sampled throughout.</summary>
    private const int MinimumHeldSamples = 8;

    /// <summary>
    /// Samples, every 250 ms for <paramref name="window"/>, whether reads of
    /// <paramref name="tree"/> remain on the original view and are not fenced.
    /// Returns the sample count and the number that observed a fence or another
    /// view.
    /// </summary>
    private async Task<(int Samples, int Unexpected)> SampleOriginalViewAsync(
        string tree,
        string key,
        byte[]? originalValue,
        TimeSpan window)
    {
        var samples = 0;
        var unexpected = 0;
        var end = Environment.TickCount64 + (long)window.TotalMilliseconds;
        while (Environment.TickCount64 < end)
        {
            var read = await ReadAsync(tree, key);
            var status = await Coordinator(tree).GetStatusAsync(CancellationToken.None);
            samples++;
            var isOriginal = originalValue is null
                ? read.Value is null
                : read.Value is not null && read.Value.AsSpan().SequenceEqual(originalValue);
            if (read.Fenced || status.ReadFenced || !isOriginal)
            {
                unexpected++;
            }

            await Task.Delay(250);
        }

        return (samples, unexpected);
    }

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
    public async Task An_imported_cross_tree_participant_keeps_the_original_view_until_its_sibling_terminal_arrives()
    {
        const string treeA = "xtib-fenced-a";
        const string treeB = "xtib-fenced-b";
        const string operationId = "xtib-fenced-op";
        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);

        // Tree A is imported while tree B's terminal is still on its own stream.
        await StartBootstrapAsync(treeA);
        var heldPhase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.IncrementalHandoff, LatticeBootstrapState.LiveIncremental);
        var siblingWhileHeld = await ReadAsync(treeB, "k");

        // The held shadow remains unpublished, and every phase tick re-checks
        // the barrier. Sample for more than two ticks to catch an early cutover.
        var (samples, unexpectedSamples) = await SampleOriginalViewAsync(treeA, "k", null, HeldWindow);
        var pendingHolds = await Coordinator(treeA).GetPendingCrossTreeHoldsForTestingAsync();

        await DeliverTreeBAsync(treeA, treeB, operationId);
        var released = await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental);
        var a = await ReadAsync(treeA, "k");
        var b = await ReadAsync(treeB, "k");

        Assert.Multiple(() =>
        {
            Assert.That(heldPhase, Is.Not.EqualTo(LatticeBootstrapState.Failed));
            Assert.That(siblingWhileHeld, Is.EqualTo((false, (byte[]?)null)), "precondition: tree B is pre-saga");
            Assert.That(samples, Is.GreaterThanOrEqualTo(MinimumHeldSamples), "precondition: the window was sampled throughout");
            Assert.That(pendingHolds, Has.Some.Contains($"barrier:{LatticeCrossTreeReceiverGrain.ComputeKey(SiteAClusterId, operationId)}"),
                "the diagnostic snapshot identifies the undecided barrier holding the import");
            Assert.That(unexpectedSamples, Is.Zero,
                "tree A must remain readable on its original pre-saga view until tree B's terminal decides the barrier");
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
            Assert.That(named[0].CrossTreeDecisionStamps?.Keys, Is.EquivalentTo(new[] { treeA, treeB }),
                "the decision row carries the full decision stamp vector (#4684)");
        });
    }

    [Test]
    public async Task An_import_that_names_no_row_of_an_operation_a_sibling_already_arrived_at_records_its_arrival()
    {
        // Issue #4684: the origin purged tree A's sub-saga before the export, so
        // the export carries tree A's outcome as a bare committed row and names
        // the operation nowhere. Tree B's terminal reached site B before the
        // export opened, so the barrier records tree A's arrival with tree B's
        // verdict, and neither tree is served split.
        const string treeA = "xtib-purged-a";
        const string treeB = "xtib-purged-b";
        const string operationId = "xtib-purged-op";
        await _siteA.Client.GetGrain<ILattice>(treeA).SetAsync("k", [1]);

        await DeliverTreeBAsync(treeA, treeB, operationId);
        await StartBootstrapAsync(treeA);
        var phase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental);

        var a = await ReadAsync(treeA, "k");
        var b = await ReadAsync(treeB, "k");
        var barrier = await Barrier(operationId).GetStatusAsync();
        Assert.Multiple(() =>
        {
            Assert.That(phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
            Assert.That(a, Is.EqualTo((false, (byte[]?)new byte[] { 1 })), "tree A is imported post-saga");
            Assert.That(b, Is.EqualTo((false, (byte[]?)new byte[] { 2 })),
                "tree B must flip with tree A: an import that names the operation nowhere is tree A's arrival");
            Assert.That(barrier.Decided, Is.True);
        });
    }

    /// <summary>
    /// Delivers tree B's sub-saga on site B through the real replication
    /// applier, its terminal carrying the operation's decision stamps as the
    /// shipper stamps it.
    /// </summary>
    private async Task DeliverStampedTreeBAsync(string treeA, string treeB, string operationId, IReadOnlyDictionary<string, long> stamps)
    {
        var txid = Guid.NewGuid();
        var stamp = HybridLogicalClock.Tick(new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks });
        await _siteB.Client.GetGrain<IReplicationApplyGrain>(treeB).ApplyPreparedSetAsync(
            "k", [2], stamp, SiteAClusterId,
            sourceVectorClock: null, expiresAtTicks: 0, txid, atomicBatchSize: 0, atomicBatchIndex: 0);
        var applier = _siteB.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services.GetRequiredService<IReplicationApplier>();
        var applied = await applier.ApplyAsync(Terminal(treeB, operationId, [treeA, treeB], txid, stamps));
        Assert.That(applied.Applied, Is.True, "precondition: tree B's terminal applies");
    }

    /// <summary>Site A's current export epoch of <paramref name="tree"/>.</summary>
    private Task<long> SiteAEpochAsync(string tree) =>
        _siteA.Client.GetGrain<IReplicationExportEpochGrain>(tree).GetAsync();

    [Test]
    public async Task A_stamped_barrier_that_opens_after_an_unnaming_import_from_after_the_decision_records_the_arrival()
    {
        // Issue #4684, the liveness half (the model's RImportFenceLifts): tree A
        // is imported first, from an export that opened after the operation's
        // decision and names it nowhere (its sub-saga was purged at the origin).
        // Tree B's stamped terminal then opens the barrier, which must record
        // tree A's arrival itself rather than wait for it for ever.
        const string treeA = "xtib-late-open-a";
        const string treeB = "xtib-late-open-b";
        const string operationId = "xtib-late-open-op";
        await _siteA.Client.GetGrain<ILattice>(treeA).SetAsync("k", [1]);
        var stamps = new Dictionary<string, long> { [treeA] = await SiteAEpochAsync(treeA), [treeB] = await SiteAEpochAsync(treeB) };

        await StartBootstrapAsync(treeA);
        var phase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental);
        await DeliverStampedTreeBAsync(treeA, treeB, operationId, stamps);

        var barrier = await Barrier(operationId).GetStatusAsync();
        Assert.Multiple(async () =>
        {
            Assert.That(phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
            Assert.That(barrier.Decided, Is.True, "the barrier records the unnaming import's arrival when it opens");
            Assert.That(await ReadAsync(treeA, "k"), Is.EqualTo((false, (byte[]?)new byte[] { 1 })));
            Assert.That(await ReadAsync(treeB, "k"), Is.EqualTo((false, (byte[]?)new byte[] { 2 })),
                "tree B flips with tree A");
        });
    }

    [Test]
    public async Task An_import_whose_export_opened_before_the_decision_leaves_the_tree_pending()
    {
        // Issue #4684, the opened-after guard: tree A's export opened before the
        // operation's decision (its export epoch is not past tree A's decision
        // stamp), so its bare rows can be pre-saga. Tree A stays pending in the
        // barrier however the import names the operation.
        const string treeA = "xtib-early-a";
        const string treeB = "xtib-early-b";
        const string operationId = "xtib-early-op";
        await _siteA.Client.GetGrain<ILattice>(treeA).SetAsync("k", [7]);

        await StartBootstrapAsync(treeA);
        var phase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental);

        // The decision is stamped after tree A's export opened.
        var stamps = new Dictionary<string, long> { [treeA] = await SiteAEpochAsync(treeA), [treeB] = await SiteAEpochAsync(treeB) };
        await DeliverStampedTreeBAsync(treeA, treeB, operationId, stamps);

        var barrier = await Barrier(operationId).GetStatusAsync();
        var b = await ReadAsync(treeB, "k");
        Assert.Multiple(() =>
        {
            Assert.That(phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
            Assert.That(barrier.Decided, Is.False, "an export that opened before the decision decides nothing");
            Assert.That(barrier.ArrivedTrees, Is.EqualTo(new[] { treeB }), "tree A stays pending in the barrier");
            Assert.That(b, Is.EqualTo((false, (byte[]?)null)), "tree B stays pre-saga");
        });
    }

    [Test]
    public async Task An_imported_tree_keeps_its_original_view_while_any_barrier_indexed_under_it_is_undecided()
    {
        // Issue #4684, fence condition 2 (the model's BarrierQuiet): a barrier
        // that waits for tree A prevents publication even when the import did
        // not arrive at it.
        const string treeA = "xtib-quiet-a";
        const string treeB = "xtib-quiet-b";
        const string operationId = "xtib-quiet-op";
        await _siteA.Client.GetGrain<ILattice>(treeA).SetAsync("k", [5]);

        // Stamped as decided after any export tree A can serve in this test, so
        // tree A's import never settles the operation.
        await DeliverStampedTreeBAsync(treeA, treeB, operationId, new Dictionary<string, long> { [treeA] = long.MaxValue - 1, [treeB] = 0 });
        await StartBootstrapAsync(treeA);
        var phase = await AwaitPhaseAsync(treeA, LatticeBootstrapState.IncrementalHandoff, LatticeBootstrapState.LiveIncremental);
        await Task.Delay(1500);

        var a = await ReadAsync(treeA, "k");
        var status = await Coordinator(treeA).GetStatusAsync(CancellationToken.None);
        Assert.Multiple(() =>
        {
            Assert.That(phase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff), "the bootstrap waits for the barrier");
            Assert.That(a, Is.EqualTo((false, (byte[]?)null)),
                "tree A remains readable on its original view while a barrier waiting for it is undecided");
            Assert.That(status.ReadFenced, Is.False,
                "a fresh shadow import does not fence the original tree");
        });
    }

    [Test]
    public async Task An_import_that_opened_after_the_decision_but_named_the_operation_leaves_the_tree_to_its_rows()
    {
        // Issue #4684: an import that carried a row of the operation settles the
        // tree through that row (a decision row arrives at the barrier; a
        // prepared row waits for the tree's terminal), never as a purged sub-saga.
        const string treeA = "xtib-named-a";
        const string treeB = "xtib-named-b";
        const string operationId = "xtib-named-op";
        var index = _siteB.Client.GetGrain<ICrossTreeBarrierIndexGrain>(treeA);
        await index.RecordImportAsync(SiteAClusterId, new CrossTreeImportRecord
        {
            ExportEpoch = long.MaxValue,
            NamedOperations = [operationId],
        });

        await DeliverStampedTreeBAsync(treeA, treeB, operationId, new Dictionary<string, long> { [treeA] = 0, [treeB] = 0 });

        var barrier = await Barrier(operationId).GetStatusAsync();
        Assert.That(barrier.ArrivedTrees, Is.EqualTo(new[] { treeB }),
            "an import that named the operation does not count as tree A's arrival");
    }

    [Test]
    public async Task A_barrier_registers_under_every_tree_it_waits_for_and_withdraws_once_decided()
    {
        const string treeA = "xtib-index-a";
        const string treeB = "xtib-index-b";
        const string operationId = "xtib-index-op";
        var key = LatticeCrossTreeReceiverGrain.ComputeKey(SiteAClusterId, operationId);
        await AuthorCrossTreeWriteAsync(treeA, treeB, operationId);

        await DeliverTreeBAsync(treeA, treeB, operationId);
        var whileOpen = await _siteB.Client.GetGrain<ICrossTreeBarrierIndexGrain>(treeA).GetAsync();
        await StartBootstrapAsync(treeA);
        await AwaitPhaseAsync(treeA, LatticeBootstrapState.LiveIncremental);
        var afterDecision = await _siteB.Client.GetGrain<ICrossTreeBarrierIndexGrain>(treeA).GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(whileOpen, Does.Contain(key), "an import of tree A must find the barrier that waits for it");
            Assert.That(afterDecision, Does.Not.Contain(key), "a decided barrier withdraws");
        });
    }

    [Test]
    public async Task The_receiver_acknowledges_a_cross_tree_terminal_only_after_the_barrier_recorded_it()
    {
        // Issue #4684: the origin purges a cross-tree decision once every peer
        // has acknowledged past the terminal, on the premise that an
        // acknowledged terminal reached the barrier. An applied result is
        // returned only after the barrier recorded the arrival, and a terminal
        // the barrier refuses is never acknowledged.
        const string treeA = "xtib-ack-a";
        const string treeB = "xtib-ack-b";
        const string operationId = "xtib-ack-op";
        var applier = _siteB.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services.GetRequiredService<IReplicationApplier>();

        var applied = await applier.ApplyAsync(Terminal(treeB, operationId, [treeA, treeB]));
        var barrier = await Barrier(operationId).GetStatusAsync();

        // A wait set whose trees disagree on cluster identity is refused at the
        // barrier: the notify fails, so the terminal must not be acknowledged.
        Exception? refusal = null;
        ApplyResult? refused = null;
        try
        {
            refused = await applier.ApplyAsync(Terminal(treeA, "xtib-ack-refused-op", [treeA, ElsewhereTree]));
        }
        catch (Exception ex)
        {
            refusal = ex;
        }

        Assert.Multiple(() =>
        {
            Assert.That(applied.Applied, Is.True);
            Assert.That(barrier.ArrivedTrees, Does.Contain(treeB), "an applied terminal has reached the barrier");
            Assert.That(refusal is not null || refused is { Applied: false, Deferred: true }, Is.True,
                "a terminal whose barrier notify failed must not be acknowledged");
        });
    }

    private static WalRecord Terminal(
        string tree, string operationId, IReadOnlyList<string> participants,
        Guid? txid = null, IReadOnlyDictionary<string, long>? stamps = null) => new()
    {
        TreeId = tree,
        Op = MutationKind.TxCommit,
        Key = ShardOf("k").ToString(System.Globalization.CultureInfo.InvariantCulture),
        ShardIndex = ShardOf("k"),
        Timestamp = HybridLogicalClock.Tick(new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks }),
        OriginClusterId = SiteAClusterId,
        TransactionId = txid ?? Guid.NewGuid(),
        CrossTreeOperationId = operationId,
        CrossTreeParticipants = participants,
        CrossTreeDecisionStamps = stamps,
    };

    private ILatticeCrossTreeReceiverGrain Barrier(string operationId) =>
        _siteB.Client.GetGrain<ILatticeCrossTreeReceiverGrain>(LatticeCrossTreeReceiverGrain.ComputeKey(SiteAClusterId, operationId));

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
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = SiteBClusterId;
                // One statically replicated tree, so the driver activation
                // service runs and subscribes to the topology: a test re-adds
                // site A at runtime through it. No test writes to this tree.
                opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
                {
                    [ActivationAnchorTree] = LatticeMergeMode.LwwRegister,
                };
            });
            siloBuilder.ConfigureLatticeReplication(ElsewhereTree, opts => opts.ClusterId = "xtib-site-elsewhere");
            siloBuilder.Services.AddSingleton<IReplicationTopology>(SiteBTopology);
            if (SiteATransports.TryGetValue(SiteAClusterId, out var transport))
            {
                siloBuilder.Services.AddSingleton(transport);
            }

            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    /// <summary>
    /// Delegates to site A's remote snapshot service, and can hold one tree's
    /// export stream after its first item: the source export has opened, and
    /// nothing has been applied at site B yet.
    /// </summary>
    private sealed class PausableTransport(IRemoteSnapshotItemTransport inner) : IRemoteSnapshotItemTransport
    {
        private static readonly ConcurrentDictionary<string, (TaskCompletionSource Reached, TaskCompletionSource Release)> Paused = new(StringComparer.Ordinal);

        public static void Pause(string tree, TaskCompletionSource reached, TaskCompletionSource release) => Paused[tree] = (reached, release);

        public static void Resume(string tree) => Paused.TryRemove(tree, out _);

        /// <summary>
        /// Trees whose export metadata reports it was not served under the
        /// cross-tree hold, as a source that predates the hold reports it.
        /// </summary>
        public static readonly ConcurrentDictionary<string, bool> Unhonoured = new(StringComparer.Ordinal);

        public async Task<RemoteSnapshotMetadata> GetMetadataAsync(
            string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken cancellationToken = default)
        {
            var metadata = await inner.GetMetadataAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken);
            return Unhonoured.ContainsKey(treeName) ? metadata with { CrossTreeHoldHonoured = false } : metadata;
        }

        public async IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
            string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var item in RequestSnapshotItemsAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken))
            {
                if (item.Entry is { } entry)
                {
                    yield return entry;
                }
            }
        }

        public async IAsyncEnumerable<RemoteSnapshotStreamItem> RequestSnapshotItemsAsync(
            string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var first = true;
            await foreach (var item in inner.RequestSnapshotItemsAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken))
            {
                if (first && Paused.TryGetValue(treeName, out var pause))
                {
                    pause.Reached.TrySetResult();
                    await pause.Release.Task.WaitAsync(cancellationToken);
                }

                first = false;
                yield return item;
            }
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
