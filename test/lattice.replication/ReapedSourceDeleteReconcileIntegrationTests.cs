using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
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
/// An in-place re-bootstrap must remove a key whose source tombstone was already
/// reaped (issue #4537). Tombstone compaction physically drops a delete after
/// <c>TombstoneGracePeriod</c>, so the #4504 tombstone pass cannot ship it and
/// the export simply omits the key. A receiver whose copy is aligned with the
/// source's lineage reconciles that absence: every source-origin key it held
/// before the export that the export did not carry is deleted at its captured
/// HLC. Two real clusters: site A is the bootstrap source, site B the receiver,
/// driven through the real bootstrap coordinator over the real remote snapshot
/// service.
/// </summary>
[TestFixture]
[Category("Integration")]
public partial class ReapedSourceDeleteReconcileIntegrationTests
{
    private const string SiteAClusterId = "rsdr-site-a";
    private const string SiteBClusterId = "rsdr-site-b";
    private const string SiteCClusterId = "rsdr-site-c";

    private static readonly ConcurrentDictionary<string, IRemoteSnapshotTransport> SiteATransports = new();

    /// <summary>
    /// Runs once when the receiver starts reading the next export's entries,
    /// after its pre-capture: lets a test land a write inside the drain.
    /// </summary>
    private static Func<Task>? _onDrainStarted;

    /// <summary>
    /// While set, replaces the source frontier an export carries at open (issue
    /// #4549), from the opening metadata, so a test controls the third origin's watermark.
    /// </summary>
    private static Func<RemoteSnapshotMetadata, SnapshotSourceFrontier?>? _openFrontier;

    /// <summary>
    /// While set, replaces the source frontier an export carries in its close
    /// trailer - the frontier a completed bootstrap installs on the receiver's
    /// tree frontier (issue #4586 part 2b) - from the one the source shipped.
    /// </summary>
    private static Func<SnapshotSourceFrontier?, SnapshotSourceFrontier?>? _closeFrontier;

    /// <summary>While set, the receiver's transport behaves as a sender that predates source generations.</summary>
    private static volatile bool _legacySender;

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
            LatticeSnapshotProviderUnitTests.TestOptions(SiteAClusterId));
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

    private static IReplicationApplier Applier(TestCluster cluster) =>
        cluster.Silos.OfType<InProcessSiloHandle>().First()
            .SiloHost.Services.GetRequiredService<IReplicationApplier>();

    private Task BootstrapSiteBAsync(string tree) =>
        DriveSiteBAsync(tree, c => c.BootstrapAsync(SiteAClusterId, CancellationToken.None));

    private async Task DriveSiteBAsync(string tree, Func<ILatticeBootstrapCoordinatorGrain, Task> kick)
    {
        var coordinator = _siteB.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree);
        await kick(coordinator);

        var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(60).TotalMilliseconds;
        LatticeBootstrapState state;
        do
        {
            await Task.Delay(250);
            state = await coordinator.GetStateAsync(CancellationToken.None);
        }
        while (state != LatticeBootstrapState.LiveIncremental
            && state != LatticeBootstrapState.Failed
            && Environment.TickCount64 < deadline);

        Assert.That(state, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "the bootstrap must complete");
    }

    /// <summary>Physically reaps every tombstone the source tree holds, as compaction would after its grace period.</summary>
    private async Task ReapSourceTombstonesAsync(string tree)
    {
        var registry = _siteA.Client.GetLatticeRegistry();
        var physicalTreeId = await registry.ResolveAsync(tree);
        var shardMap = await registry.GetShardMapAsync(tree)
            ?? ShardMap.GetOrCreateDefaultShared(
                LatticeConstants.DefaultVirtualShardCount,
                LatticeConstants.DefaultShardCount);
        foreach (var shardIndex in shardMap.GetPhysicalShardIndices())
        {
            var shard = _siteA.Client.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            var leafId = await shard.GetLeftmostLeafIdAsync();
            while (leafId is not null)
            {
                var leaf = _siteA.Client.GetGrain<IBPlusLeafGrain>(leafId.Value);
                await leaf.CompactTombstonesAsync(TimeSpan.Zero);
                leafId = await leaf.GetNextSiblingAsync();
            }
        }
    }

    private async Task<List<SnapshotEntry>> ExportSiteAAsync(string tree)
    {
        var exported = new List<SnapshotEntry>();
        var stream = await _siteAProvider.ExportAsync(tree, HybridLogicalClock.Zero);
        await foreach (var entry in stream.Entries)
        {
            exported.Add(entry);
        }

        return exported;
    }

    private static MeterListener ListenForOutcomes(string tree, ConcurrentBag<string> outcomes) =>
        MeterListening.StartForInstrument(LatticeReplicationMetrics.BootstrapReconcile, listener =>
            listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                string? treeTag = null;
                string? outcome = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeReplicationMetrics.TagTree) treeTag = tag.Value as string;
                    if (tag.Key == LatticeReplicationMetrics.TagOutcome) outcome = tag.Value as string;
                }

                if (treeTag == tree && outcome is not null)
                {
                    outcomes.Add(outcome);
                }
            }));

    [Test]
    public async Task Re_bootstrap_over_an_aligned_receiver_deletes_a_key_whose_source_tombstone_was_reaped()
    {
        const string tree = "rsdr-reaped-delete";
        const string kept = "kept";
        const string deleted = "deleted-and-reaped";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync(kept, new byte[] { 1 });
        await siteA.SetAsync(deleted, new byte[] { 2 });

        // First bootstrap into an empty receiver: the copy is aligned with the
        // source's lineage and every key carries the source as its origin.
        await BootstrapSiteBAsync(tree);
        Assert.That(await siteB.GetAsync(deleted), Is.EqualTo(new byte[] { 2 }), "precondition: the receiver holds the key");

        // The source deletes the key while the receiver is behind, then reaps the tombstone.
        await siteA.DeleteAsync(deleted);
        await ReapSourceTombstonesAsync(tree);
        var exported = await ExportSiteAAsync(tree);
        Assert.That(exported.Select(e => e.Key), Has.No.Member(deleted),
            "precondition: the reaped delete leaves no row in the export, so the tombstone pass cannot ship it");

        var outcomes = new ConcurrentBag<string>();
        using (ListenForOutcomes(tree, outcomes))
        {
            await BootstrapSiteBAsync(tree);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await siteB.GetAsync(deleted), Is.Null,
                "the source deleted the key and reaped its tombstone, so the re-bootstrapped receiver must not keep it");
            Assert.That(await siteB.GetAsync(kept), Is.EqualTo(new byte[] { 1 }), "a key the export carried is untouched");
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeReconciled));
        });
    }

    [Test]
    public async Task Re_bootstrap_keeps_a_newer_write_that_lands_on_a_captured_key_during_the_drain()
    {
        const string tree = "rsdr-newer-write-in-drain";
        const string key = "rewritten";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync(key, new byte[] { 1 });
        var sourceValueHlc = (await ExportSiteAAsync(tree)).Single(e => e.Key == key).Timestamp;
        await BootstrapSiteBAsync(tree);

        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);

        // The receiver has already pre-captured the key with the source as its
        // origin when a newer third-site write reaches it mid-drain. The
        // reconcile's delete is stamped at the captured HLC, so it must lose to
        // the newer write rather than erase it.
        ApplyResult? inDrain = null;
        _onDrainStarted = async () => inDrain = await Applier(_siteB).ApplyAsync(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = key,
            Value = new byte[] { 9 },
            Timestamp = HybridLogicalClock.Tick(sourceValueHlc),
            OriginClusterId = SiteCClusterId,
        });
        try
        {
            await BootstrapSiteBAsync(tree);
        }
        finally
        {
            _onDrainStarted = null;
        }

        Assert.That(inDrain, Is.Not.Null, "precondition: the write landed inside the drain");
        Assert.That(inDrain!.Value.Applied, Is.True, $"precondition: the in-drain write applied ({inDrain})");
        Assert.That(await siteB.GetAsync(key), Is.EqualTo(new byte[] { 9 }),
            "a write newer than the captured source value must survive the reconcile");
    }

    [Test]
    public async Task An_unstable_source_generation_owes_a_retry_that_re_drains_and_reconciles()
    {
        const string tree = "rsdr-owed-retry";
        const string kept = "kept";
        const string deleted = "deleted-and-reaped";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync(kept, new byte[] { 1 });
        await siteA.SetAsync(deleted, new byte[] { 2 });
        await BootstrapSiteBAsync(tree);

        await siteA.DeleteAsync(deleted);
        await ReapSourceTombstonesAsync(tree);

        // The source tree is soft-deleted and recovered between the export's
        // opening metadata and its entry stream: both ends read live, but the
        // soft-delete epoch moved, so the export cannot prove the absence.
        _onDrainStarted = async () =>
        {
            await siteA.DeleteTreeAsync();
            await siteA.RecoverTreeAsync();
        };
        var outcomes = new ConcurrentBag<string>();
        try
        {
            using (ListenForOutcomes(tree, outcomes))
            {
                await BootstrapSiteBAsync(tree);
            }
        }
        finally
        {
            _onDrainStarted = null;
        }

        Assert.Multiple(async () =>
        {
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeSkippedUnstable));
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeOwedRetry));
            Assert.That(await siteB.GetAsync(deleted), Is.EqualTo(new byte[] { 2 }),
                "an unstable export must not infer deletes");
        });

        // The owed retry is a full re-drain with a fresh pre-capture.
        outcomes = new ConcurrentBag<string>();
        using (ListenForOutcomes(tree, outcomes))
        {
            await DriveSiteBAsync(tree, c => c.RetryOwedReconcileAsync(SiteAClusterId));
        }

        Assert.Multiple(async () =>
        {
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeReconciled));
            Assert.That(await siteB.GetAsync(deleted), Is.Null, "the owed retry must reconcile the reaped delete");
            Assert.That(await siteB.GetAsync(kept), Is.EqualTo(new byte[] { 1 }));
        });

        // Settled: a further retry is a no-op and starts no bootstrap.
        outcomes = new ConcurrentBag<string>();
        using (ListenForOutcomes(tree, outcomes))
        {
            var coordinator = _siteB.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree);
            await coordinator.RetryOwedReconcileAsync(SiteAClusterId);
            await Task.Delay(500);
            Assert.That(await coordinator.GetStateAsync(CancellationToken.None), Is.EqualTo(LatticeBootstrapState.LiveIncremental));
        }

        Assert.That(outcomes, Is.Empty, "nothing is owed once the reconcile has run");
    }

    [Test]
    public async Task Re_bootstrap_over_a_never_aligned_receiver_skips_the_reconcile()
    {
        const string tree = "rsdr-never-aligned";
        const string key = "pre-existing";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync(key, new byte[] { 1 });

        // Before its first bootstrap the receiver holds a source-origin row the
        // source's export never carries, so its copy cannot be proven to derive
        // from the source's lineage.
        await Applier(_siteB).ApplyAsync(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "stray-source-row",
            Value = new byte[] { 5 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = SiteAClusterId,
        });
        await BootstrapSiteBAsync(tree);

        await siteA.DeleteAsync(key);
        await ReapSourceTombstonesAsync(tree);

        var outcomes = new ConcurrentBag<string>();
        using (ListenForOutcomes(tree, outcomes))
        {
            await BootstrapSiteBAsync(tree);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await siteB.GetAsync(key), Is.EqualTo(new byte[] { 1 }),
                "an unaligned receiver must not infer deletes from absence");
            Assert.That(await siteB.GetAsync("stray-source-row"), Is.EqualTo(new byte[] { 5 }));
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeSkippedNeverAligned));
            Assert.That(outcomes, Has.No.Member(LatticeReplicationMetrics.BootstrapReconcileOutcomeReconciled));
        });
    }

    [Test]
    public async Task A_receiver_holding_only_its_own_and_third_origin_rows_aligns_on_its_first_bootstrap()
    {
        const string tree = "rsdr-local-rows-align";
        const string deleted = "deleted-and-reaped";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync(deleted, new byte[] { 2 });

        // Rows the reconcile never touches do not stop the receiver aligning.
        await siteB.SetAsync("receiver-local", new byte[] { 7 });
        await Applier(_siteB).ApplyAsync(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "third-origin",
            Value = new byte[] { 3 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = SiteCClusterId,
        });
        var first = new ConcurrentBag<string>();
        using (ListenForOutcomes(tree, first))
        {
            await BootstrapSiteBAsync(tree);
        }

        Assert.That(first, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeReconciled),
            "holding no source-origin row, the receiver is aligned by its first import and reconciles in it");

        await siteA.DeleteAsync(deleted);
        await ReapSourceTombstonesAsync(tree);
        var outcomes = new ConcurrentBag<string>();
        using (ListenForOutcomes(tree, outcomes))
        {
            await BootstrapSiteBAsync(tree);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeReconciled));
            Assert.That(await siteB.GetAsync(deleted), Is.Null);
            Assert.That(await siteB.GetAsync("receiver-local"), Is.EqualTo(new byte[] { 7 }));
            Assert.That(await siteB.GetAsync("third-origin"), Is.EqualTo(new byte[] { 3 }));
        });
    }

    [Test]
    public async Task A_source_lineage_change_that_orphans_nothing_realigns_the_receiver()
    {
        const string tree = "rsdr-realign";
        const string copy = "rsdr-realign-copy";
        const string kept = "kept";
        const string deleted = "deleted-and-reaped";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync(kept, new byte[] { 1 });
        await siteA.SetAsync(deleted, new byte[] { 2 });
        await BootstrapSiteBAsync(tree);

        // The source rebinds the tree to a copy holding the same keys: a new lineage.
        var registry = _siteA.Client.GetLatticeRegistry();
        var before = (await registry.GetEntryAsync(tree))!.Lineage;
        var siteACopy = _siteA.Client.GetGrain<ILattice>(copy);
        await siteACopy.SetAsync(kept, new byte[] { 1 });
        await siteACopy.SetAsync(deleted, new byte[] { 2 });
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await registry.SetAliasAsync(tree, copy);
        }

        Assert.That((await registry.GetEntryAsync(tree))!.Lineage, Is.Not.EqualTo(before), "PRECONDITION: the lineage changed");

        var outcomes = new ConcurrentBag<string>();
        using (ListenForOutcomes(tree, outcomes))
        {
            await BootstrapSiteBAsync(tree);
        }

        Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeAligned),
            "every source-origin key the receiver held was carried, so it adopts the new lineage");

        await siteA.DeleteAsync(deleted);
        await ReapSourceTombstonesAsync(tree);
        outcomes = new ConcurrentBag<string>();
        using (ListenForOutcomes(tree, outcomes))
        {
            await BootstrapSiteBAsync(tree);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeReconciled));
            Assert.That(await siteB.GetAsync(deleted), Is.Null, "a realigned receiver reconciles the next reaped delete");
            Assert.That(await siteB.GetAsync(kept), Is.EqualTo(new byte[] { 1 }));
        });
    }

    [Test]
    public async Task An_unknown_source_generation_owes_a_retry_that_reconciles_once_the_sender_upgrades()
    {
        const string tree = "rsdr-unknown-owed";
        const string deleted = "deleted-and-reaped";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync(deleted, new byte[] { 2 });
        await BootstrapSiteBAsync(tree);
        await siteA.DeleteAsync(deleted);
        await ReapSourceTombstonesAsync(tree);

        var outcomes = new ConcurrentBag<string>();
        _legacySender = true;
        try
        {
            using (ListenForOutcomes(tree, outcomes))
            {
                await BootstrapSiteBAsync(tree);
            }
        }
        finally
        {
            _legacySender = false;
        }

        Assert.Multiple(async () =>
        {
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeSkippedUnknown));
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeOwedRetry),
                "an unknown generation is owed, not a permanent skip");
            Assert.That(await siteB.GetAsync(deleted), Is.EqualTo(new byte[] { 2 }));
        });

        outcomes = new ConcurrentBag<string>();
        using (ListenForOutcomes(tree, outcomes))
        {
            await DriveSiteBAsync(tree, c => c.RetryOwedReconcileAsync(SiteAClusterId));
        }

        Assert.Multiple(async () =>
        {
            Assert.That(outcomes, Does.Contain(LatticeReplicationMetrics.BootstrapReconcileOutcomeReconciled));
            Assert.That(await siteB.GetAsync(deleted), Is.Null, "the upgraded sender's retry reconciles the reaped delete");
        });
    }

    [Test]
    public async Task Re_bootstrap_keeps_a_third_origin_key_the_source_export_omits()
    {
        const string tree = "rsdr-third-origin";
        const string sourceKey = "from-source";
        const string thirdKey = "from-third-site";

        var siteA = _siteA.Client.GetGrain<ILattice>(tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(tree);
        await siteA.SetAsync(sourceKey, new byte[] { 1 });
        await BootstrapSiteBAsync(tree);

        await Applier(_siteB).ApplyAsync(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = thirdKey,
            Value = new byte[] { 3 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = SiteCClusterId,
        });

        await BootstrapSiteBAsync(tree);

        Assert.Multiple(async () =>
        {
            Assert.That(await siteB.GetAsync(thirdKey), Is.EqualTo(new byte[] { 3 }),
                "the source never applied the third-origin write, so its absence from the export proves nothing and the key must survive");
            Assert.That(await siteB.GetAsync(sourceKey), Is.EqualTo(new byte[] { 1 }));
        });
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
            siloBuilder.AddOutgoingGrainCallFilter<ApplyHoldFilter>();

            if (SiteATransports.TryGetValue(SiteAClusterId, out var transport))
            {
                siloBuilder.Services.AddSingleton<IRemoteSnapshotTransport>(
                    new DrainHookTransport((IRemoteSnapshotItemTransport)transport));
            }

            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    /// <summary>Delegates to the source's transport and runs <see cref="_onDrainStarted"/> before the first item.</summary>
    private sealed class DrainHookTransport(IRemoteSnapshotItemTransport inner) : IRemoteSnapshotItemTransport
    {
        public async Task<RemoteSnapshotMetadata> GetMetadataAsync(
            string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken cancellationToken = default)
        {
            var metadata = await inner.GetMetadataAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken);
            if (_openFrontier is { } frontierFor)
            {
                metadata = metadata with { SourceFrontier = frontierFor(metadata) };
            }

            return _legacySender ? metadata with { OpenGeneration = null } : metadata;
        }

        public IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
            string treeName, string sourceClusterId, HybridLogicalClock fromAsOfHlc, CancellationToken cancellationToken = default) =>
            inner.RequestSnapshotAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken);

        public async IAsyncEnumerable<RemoteSnapshotStreamItem> RequestSnapshotItemsAsync(
            string treeName,
            string sourceClusterId,
            HybridLogicalClock fromAsOfHlc,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (Interlocked.Exchange(ref _onDrainStarted, null) is { } hook)
            {
                await hook();
            }

            await foreach (var item in inner.RequestSnapshotItemsAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken))
            {
                if (_legacySender && item.CloseGeneration is not null)
                {
                    continue;
                }

                if (_closeFrontier is { } closeFrontierFor && item.CloseGeneration is not null)
                {
                    yield return item with { SourceFrontier = closeFrontierFor(item.SourceFrontier) };
                    continue;
                }

                yield return item;
            }
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
