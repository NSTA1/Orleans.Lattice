using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4684, R1: an import of one tree settles the cross-tree sub-sagas its
/// export carries, including one whose decision the origin purged before the
/// cross-tree hold existed, which arrives as bare committed rows. A sibling tree
/// whose terminal for that operation has not reached this cluster is still
/// pre-saga here, and no barrier exists to say so. So the export captures, at
/// its end, every sibling's write-ahead-log tails and export epoch, and the
/// importing cluster keeps publication held until each sibling it replicates has
/// passed that boundary: its shipper vouched acknowledged positions at or
/// past the tails (route 1), or a drain of the sibling from an export opened
/// after the capture ended here (route 2). A fresh bootstrap keeps the original
/// view readable while publication waits on that boundary. A sibling that never
/// passes is re-seeded automatically. Two real clusters: site A exports through
/// the real remote snapshot service under its real cross-tree gate; site B imports
/// through the real bootstrap coordinator and records shipped positions in its
/// real tree frontier.
/// </summary>
[TestFixture]
[Category("Integration")]
public class CrossTreeSiblingBoundaryIntegrationTests
{
    private const string SiteAClusterId = "xtsb-site-a";
    private const string SiteBClusterId = "xtsb-site-b";
    private const string UnreplicatedPrefix = "xtsb-unrep-";

    private static readonly ConcurrentDictionary<string, IRemoteSnapshotTransport> SiteATransports = new();

    // The siblings site A's exports list in the running test.
    private static readonly ConcurrentDictionary<string, bool> Siblings = new(StringComparer.Ordinal);

    private TestCluster _siteA = null!;
    private TestCluster _siteB = null!;
    private TimeSpan _savedReseedAfter;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _savedReseedAfter = LatticeBootstrapCoordinatorGrain.SiblingBoundaryReseedAfter;
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
                SiblingFilterForTesting = static tree => Siblings.ContainsKey(tree),
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
        LatticeBootstrapCoordinatorGrain.SiblingBoundaryReseedAfter = _savedReseedAfter;
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

    [SetUp]
    public void BeforeEach()
    {
        Siblings.Clear();
        LatticeBootstrapCoordinatorGrain.SiblingBoundaryReseedAfter = TimeSpan.FromHours(1);
    }

    private ILatticeBootstrapCoordinatorGrain Coordinator(string tree) =>
        _siteB.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree);

    private async Task<LatticeBootstrapState> ImportAsync(string tree, params LatticeBootstrapState[] phases)
    {
        await Coordinator(tree).BootstrapAsync(SiteAClusterId, CancellationToken.None);
        return await AwaitPhaseAsync(tree, TimeSpan.FromSeconds(60), phases);
    }

    private async Task<LatticeBootstrapState> AwaitPhaseAsync(string tree, TimeSpan timeout, params LatticeBootstrapState[] phases)
    {
        var deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
        LatticeBootstrapState state;
        do
        {
            await Task.Delay(200);
            state = await Coordinator(tree).GetStateAsync(CancellationToken.None);
        }
        while (!phases.Contains(state) && state != LatticeBootstrapState.Failed && Environment.TickCount64 < deadline);

        return state;
    }

    private async Task<bool> IsReadFencedAsync(string tree)
    {
        try
        {
            await _siteB.Client.GetGrain<ILattice>(tree).GetAsync("k");
            return false;
        }
        catch (LatticeTreeBootstrappingException)
        {
            return true;
        }
    }

    /// <summary>Writes the tree and a sibling at site A, and lists the sibling on the tree's export.</summary>
    private async Task WriteAsync(string tree, string sibling)
    {
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync("k", [1]);
        await _siteA.Client.GetGrain<ILattice>(sibling).SetAsync("k", [2]);
        Siblings[sibling] = true;
    }

    [Test]
    public async Task A_tree_remains_readable_while_its_sibling_waits_for_an_export_opened_after_capture()
    {
        const string tree = "xtsb-route2-t";
        const string sibling = "xtsb-route2-s";
        await WriteAsync(tree, sibling);

        var held = await ImportAsync(tree, LatticeBootstrapState.IncrementalHandoff, LatticeBootstrapState.LiveIncremental);
        await Task.Delay(1500);
        var readFencedWhileHeld = await IsReadFencedAsync(tree);
        var heldPhase = await Coordinator(tree).GetStateAsync(CancellationToken.None);
        var pendingHolds = await Coordinator(tree).GetPendingCrossTreeHoldsForTestingAsync();

        // The sibling's own export lists no sibling here, so it completes.
        var siblingPhase = await ImportAsync(sibling, LatticeBootstrapState.LiveIncremental);
        var released = await AwaitPhaseAsync(tree, TimeSpan.FromSeconds(60), LatticeBootstrapState.LiveIncremental);

        Assert.Multiple(async () =>
        {
            Assert.That(held, Is.Not.EqualTo(LatticeBootstrapState.Failed));
            Assert.That(heldPhase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff), "the import waits for the sibling");
            Assert.That(pendingHolds, Has.Some.Contains($"sibling:{sibling};"),
                "the diagnostic snapshot identifies the sibling boundary that is holding the import");
            Assert.That(readFencedWhileHeld, Is.False,
                "the original view remains readable while publication waits for the sibling boundary");
            Assert.That(siblingPhase, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
            Assert.That(released, Is.EqualTo(LatticeBootstrapState.LiveIncremental),
                "a drain of the sibling from an export opened after the capture passes the boundary");
            Assert.That(await IsReadFencedAsync(tree), Is.False);
        });
    }

    [Test]
    public async Task A_sibling_drained_only_from_an_export_opened_before_the_capture_does_not_pass_the_boundary()
    {
        // Route 2 counts a drain of the sibling only from an export numbered above
        // the captured epoch. The sibling here was drained first, from an export
        // that opened before the tree's export captured its boundary, and its
        // shipper vouches nothing: that drain cannot carry a sibling record
        // written between its export and the capture, so publication stays held
        // until the sibling is drained again from a later export.
        const string tree = "xtsb-stale-t";
        const string sibling = "xtsb-stale-s";
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync("k", [1]);
        await _siteA.Client.GetGrain<ILattice>(sibling).SetAsync("k", [2]);

        var siblingFirst = await ImportAsync(sibling, LatticeBootstrapState.LiveIncremental);
        var drainedBefore = await Coordinator(sibling).GetDrainedExportEpochAsync(SiteAClusterId);
        Assert.That(siblingFirst, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "precondition: the sibling was drained first");
        Assert.That(drainedBefore, Is.Not.Null, "precondition: the sibling has a drained export epoch");

        Siblings[sibling] = true;
        var held = await ImportAsync(tree, LatticeBootstrapState.IncrementalHandoff, LatticeBootstrapState.LiveIncremental);
        await Task.Delay(1500);
        var heldPhase = await Coordinator(tree).GetStateAsync(CancellationToken.None);
        var pendingHolds = await Coordinator(tree).GetPendingCrossTreeHoldsForTestingAsync();
        var readFencedWhileHeld = await IsReadFencedAsync(tree);

        var siblingAgain = await ImportAsync(sibling, LatticeBootstrapState.LiveIncremental);
        var released = await AwaitPhaseAsync(tree, TimeSpan.FromSeconds(60), LatticeBootstrapState.LiveIncremental);

        Assert.Multiple(async () =>
        {
            Assert.That(held, Is.Not.EqualTo(LatticeBootstrapState.Failed));
            Assert.That(heldPhase, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff),
                "a drain from an export opened before the capture does not pass the boundary");
            Assert.That(pendingHolds, Has.Some.Contains($"sibling:{sibling};"));
            Assert.That(readFencedWhileHeld, Is.False,
                "the original view remains readable while publication waits for the later export");
            Assert.That(siblingAgain, Is.EqualTo(LatticeBootstrapState.LiveIncremental));
            Assert.That(released, Is.EqualTo(LatticeBootstrapState.LiveIncremental),
                "a drain from an export opened after the capture does");
            Assert.That(await IsReadFencedAsync(tree), Is.False);
        });
    }

    [Test]
    public async Task A_sibling_passes_once_its_shipper_vouches_acknowledged_positions_past_the_captured_tails()
    {
        const string tree = "xtsb-route1-t";
        const string sibling = "xtsb-route1-s";
        await WriteAsync(tree, sibling);
        var physical = (await _siteA.Client.GetLatticeRegistry().GetEntryAsync(sibling))?.PhysicalTreeId ?? sibling;

        await ImportAsync(tree, LatticeBootstrapState.IncrementalHandoff, LatticeBootstrapState.LiveIncremental);

        // A sibling replicated here is a registered tree with a lineage, which
        // gives its frontier an epoch to take watermarks under.
        await _siteB.Client.GetLatticeRegistry().RegisterAsync(
            sibling, new BPlusTree.State.TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        var frontier = _siteB.Client.GetGrain<IReplicationTreeFrontierGrain>(sibling);
        var epoch = await frontier.ObserveAsync(SiteAClusterId, null);
        Assert.That(epoch, Is.Not.EqualTo(Guid.Empty), "precondition: the sibling's frontier accepts watermarks");

        // Positions on another log vouch nothing for this boundary.
        await frontier.ObserveAsync(SiteAClusterId, Vouched(epoch, "another-log"));
        await Task.Delay(1500);
        var phaseOnAnotherLog = await Coordinator(tree).GetStateAsync(CancellationToken.None);
        var readFencedOnAnotherLog = await IsReadFencedAsync(tree);

        await frontier.ObserveAsync(SiteAClusterId, Vouched(epoch, physical));
        var released = await AwaitPhaseAsync(tree, TimeSpan.FromSeconds(60), LatticeBootstrapState.LiveIncremental);

        Assert.Multiple(async () =>
        {
            Assert.That(phaseOnAnotherLog, Is.EqualTo(LatticeBootstrapState.IncrementalHandoff));
            Assert.That(readFencedOnAnotherLog, Is.False,
                "the original view remains readable while positions on another log do not pass the boundary");
            Assert.That(released, Is.EqualTo(LatticeBootstrapState.LiveIncremental),
                "acknowledged positions at or past every captured tail pass the boundary");
            Assert.That(await IsReadFencedAsync(tree), Is.False);
        });
    }

    [Test]
    public async Task A_sibling_import_whose_own_fence_is_held_still_passes_the_boundary_once_drained()
    {
        // Each tree is the other's sibling. The sibling's import waits on the
        // tree, but its drain has ended, and that is what passes the tree's
        // boundary: were a sibling counted only once its fence lifted, two
        // imports that wait on each other would never complete.
        const string tree = "xtsb-mutual-t";
        const string sibling = "xtsb-mutual-s";
        await WriteAsync(tree, sibling);
        Siblings[tree] = true;

        await ImportAsync(tree, LatticeBootstrapState.IncrementalHandoff, LatticeBootstrapState.LiveIncremental);
        await ImportAsync(sibling, LatticeBootstrapState.IncrementalHandoff, LatticeBootstrapState.LiveIncremental);
        var released = await AwaitPhaseAsync(tree, TimeSpan.FromSeconds(60), LatticeBootstrapState.LiveIncremental);

        Assert.Multiple(async () =>
        {
            Assert.That(released, Is.EqualTo(LatticeBootstrapState.LiveIncremental),
                "the sibling's drained import passes the tree's boundary while its own fence is held");
            Assert.That(await IsReadFencedAsync(tree), Is.False);
        });
    }

    [Test]
    public async Task Mutually_active_sibling_imports_do_not_queue_redundant_reseeds()
    {
        const string tree = "xtsb-mutual-reseed-t";
        const string sibling = "xtsb-mutual-reseed-s";
        await WriteAsync(tree, sibling);
        Siblings[tree] = true;
        Siblings[sibling] = true;
        LatticeBootstrapCoordinatorGrain.SiblingBoundaryReseedAfter = TimeSpan.FromSeconds(10);

        await Task.WhenAll(
            Coordinator(tree).BootstrapAsync(SiteAClusterId, CancellationToken.None),
            Coordinator(sibling).BootstrapAsync(SiteAClusterId, CancellationToken.None));

        var heldPhases = await Task.WhenAll(
            AwaitPhaseAsync(tree, TimeSpan.FromSeconds(20), LatticeBootstrapState.IncrementalHandoff),
            AwaitPhaseAsync(sibling, TimeSpan.FromSeconds(20), LatticeBootstrapState.IncrementalHandoff));
        var initialHolds = (await Coordinator(tree).GetPendingCrossTreeHoldsForTestingAsync())
            .Select(hold => $"{tree}: {hold}")
            .Concat((await Coordinator(sibling).GetPendingCrossTreeHoldsForTestingAsync())
                .Select(hold => $"{sibling}: {hold}"))
            .Where(static hold => hold.Contains("sibling:", StringComparison.Ordinal))
            .ToArray();

        await Coordinator(sibling).MarkSiblingReseedRequestedForTestingAsync(tree);
        await Coordinator(tree).ReleaseCrossTreeHoldForTestingAsync(ageSiblingBoundaries: true);
        var releaseAfterPriorRequest = await Coordinator(sibling).ReleaseCrossTreeHoldForTestingAsync(ageSiblingBoundaries: true);
        var refreshedHolds = await Coordinator(sibling).GetPendingCrossTreeHoldsForTestingAsync();

        var phases = await Task.WhenAll(
            AwaitPhaseAsync(tree, TimeSpan.FromMinutes(2), LatticeBootstrapState.LiveIncremental),
            AwaitPhaseAsync(sibling, TimeSpan.FromMinutes(2), LatticeBootstrapState.LiveIncremental));
        var holdDescriptions = (await Coordinator(tree).GetPendingCrossTreeHoldsForTestingAsync())
            .Select(hold => $"{tree}: {hold}")
            .Concat((await Coordinator(sibling).GetPendingCrossTreeHoldsForTestingAsync())
                .Select(hold => $"{sibling}: {hold}"))
            .Where(static hold => hold.Contains("sibling:", StringComparison.Ordinal))
            .ToArray();
        var statuses = await Task.WhenAll(
            Coordinator(tree).GetStatusAsync(CancellationToken.None),
            Coordinator(sibling).GetStatusAsync(CancellationToken.None));
        var drainedEpochs = await Task.WhenAll(
            Coordinator(tree).GetDrainedExportEpochAsync(SiteAClusterId),
            Coordinator(sibling).GetDrainedExportEpochAsync(SiteAClusterId));
        var statusDescriptions = new[]
        {
            $"{tree}: phase={statuses[0].Phase};source={statuses[0].SourceClusterId ?? "none"};readFenced={statuses[0].ReadFenced};entries={statuses[0].EntriesApplied};epoch={drainedEpochs[0]?.ToString() ?? "none"}",
            $"{sibling}: phase={statuses[1].Phase};source={statuses[1].SourceClusterId ?? "none"};readFenced={statuses[1].ReadFenced};entries={statuses[1].EntriesApplied};epoch={drainedEpochs[1]?.ToString() ?? "none"}",
        };

        Assert.Multiple(() =>
        {
            Assert.That(heldPhases, Is.All.EqualTo(LatticeBootstrapState.IncrementalHandoff),
                "both imports must be active at the handoff before the aged-boundary recovery is exercised");
            Assert.That(initialHolds, Has.Some.Contains($"{tree}: sibling:{sibling};"),
                "the first coordinator must be held at the second coordinator's captured export boundary");
            Assert.That(initialHolds, Has.Some.Contains($"{sibling}: sibling:{tree};"),
                "the second coordinator must be held at the first coordinator's captured export boundary");
            Assert.That(releaseAfterPriorRequest, Is.False);
            Assert.That(refreshedHolds, Has.Some.Contains($"sibling:{tree};").And.Contains("reseedRequested=True").And.Contains("refreshRequested=True"),
                "the lower-named coordinator must still refresh an active sibling after a bootstrap request was already issued");
            Assert.That(phases, Is.All.EqualTo(LatticeBootstrapState.LiveIncremental),
                $"both imports should pass their sibling boundaries; status: {string.Join(" | ", statusDescriptions)}; remaining holds: {string.Join(" | ", holdDescriptions)}");
        });
    }

    [Test]
    public async Task A_sibling_that_never_passes_is_re_seeded_automatically()
    {
        const string tree = "xtsb-reseed-t";
        const string sibling = "xtsb-reseed-s";
        await WriteAsync(tree, sibling);
        LatticeBootstrapCoordinatorGrain.SiblingBoundaryReseedAfter = TimeSpan.FromSeconds(1);

        var released = await ImportAsync(tree, LatticeBootstrapState.LiveIncremental);
        var siblingImported = await Coordinator(sibling).GetDrainedExportEpochAsync(SiteAClusterId);

        Assert.Multiple(async () =>
        {
            Assert.That(siblingImported, Is.Not.Null, "the coordinator re-seeded the sibling itself");
            Assert.That(released, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "the re-seed passes the boundary");
            Assert.That(await IsReadFencedAsync(tree), Is.False);
        });
    }

    [Test]
    public async Task A_sibling_not_replicated_here_holds_nothing()
    {
        const string tree = "xtsb-scope-t";
        const string sibling = UnreplicatedPrefix + "s";
        await WriteAsync(tree, sibling);

        var phase = await ImportAsync(tree, LatticeBootstrapState.LiveIncremental);

        Assert.That(phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental),
            "a sibling this cluster does not replicate never vouches here and is not waited for");
    }

    [Test]
    public async Task A_sibling_whose_log_held_nothing_holds_nothing()
    {
        const string tree = "xtsb-empty-t";
        const string sibling = "xtsb-empty-s";
        await _siteA.Client.GetGrain<ILattice>(tree).SetAsync("k", [1]);
        await _siteA.Client.GetLatticeRegistry().RegisterAsync(
            sibling, new BPlusTree.State.TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        Siblings[sibling] = true;

        var phase = await ImportAsync(tree, LatticeBootstrapState.LiveIncremental);

        Assert.That(phase, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "a sibling with nothing to acknowledge is passed");
    }

    private static ReplicationSourceFrontier Vouched(Guid lineage, string physical) => new()
    {
        ReceiverLineage = lineage,
        TreeLowWatermark = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks },
        OriginLowWatermark = HybridLogicalClock.Zero,
        OriginGeneration = 1,
        AckedPositions = new ReplicationAckedPositions
        {
            PhysicalTreeId = physical,
            Positions = [.. Enumerable.Repeat(long.MaxValue / 2, LatticeReplicationOptions.DefaultReplogPartitions)],
        },
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
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = SiteBClusterId;
                opts.AutoBootstrapOnFallOffLog = true;
            });
            if (SiteATransports.TryGetValue(SiteAClusterId, out var transport))
            {
                siloBuilder.Services.AddSingleton(transport);
            }

            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, ScopedResolver>();
        }
    }

    private sealed class AllowAllResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>Site B replicates every tree but those under <see cref="UnreplicatedPrefix"/>.</summary>
    private sealed class ScopedResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) =>
            treeId.StartsWith(UnreplicatedPrefix, StringComparison.Ordinal) ? null : LatticeMergeMode.LwwRegister;
    }
}
