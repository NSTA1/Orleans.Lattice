using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #5534: a bootstrap of an OR-Map tree (the shape of
/// <c>sys-replication-config</c>) installs no bootstrap drop floor and owes no
/// foreign-delete reconcile, so it must converge through OR-Map merge alone. Two
/// real clusters: site A is the source, exporting through the real snapshot
/// provider and remote snapshot service; site B is the receiver, driven through
/// the real bootstrap coordinator. The source removes a map key while its export
/// is paused after reading the key's old state; the removal reaches the receiver
/// as a live delivery before the stale export row is drained. Observed-remove
/// semantics must keep the key removed, and the bootstrap must complete and
/// answer a re-seed request rather than owing a reconcile retry forever.
/// </summary>
[TestFixture]
[Category("Integration")]
public class OrMapBootstrapConvergenceIntegrationTests
{
    private const string SiteAClusterId = "ormb-site-a";
    private const string SiteBClusterId = "ormb-site-b";
    private const string TreeName = "ormb-config";
    private const string MapKey = "config";

    private static readonly ConcurrentDictionary<string, IRemoteSnapshotTransport> SiteATransports = new();
    private static readonly CrdtShape Shape = CrdtShape.ForOrMap<string, PnCounter>();

    private TestCluster _siteA = null!;
    private TestCluster _siteB = null!;
    private GatedSnapshotProvider _gated = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var aBuilder = new TestClusterBuilder(initialSilosCount: 1);
        aBuilder.AddSiloBuilderConfigurator<SiteASiloConfigurator>();
        _siteA = aBuilder.Build();
        await _siteA.DeployAsync();

        _gated = new GatedSnapshotProvider(new LatticeSnapshotProvider(
            _siteA.Client,
            new InMemoryWalCursorRegistry(),
            LatticeSnapshotProviderUnitTests.TestOptions(SiteAClusterId)));
        SiteATransports[SiteAClusterId] = new LatticeRemoteSnapshotService(
            _gated,
            new StubReplicationContext(SiteAClusterId, LatticeMergeMode.OrMap),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        var bBuilder = new TestClusterBuilder(initialSilosCount: 1);
        bBuilder.AddSiloBuilderConfigurator<SiteBSiloConfigurator>();
        _siteB = bBuilder.Build();
        await _siteB.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        _gated?.Release();
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

    private static PnCounter Counter(long value)
    {
        var counter = new PnCounter();
        counter.Increment(SiteAClusterId, value);
        return counter;
    }

    private static IReplicationApplier Applier(TestCluster cluster) =>
        cluster.Silos.OfType<InProcessSiloHandle>().First()
            .SiloHost.Services.GetRequiredService<IReplicationApplier>();

    [Test]
    public async Task A_key_removed_on_the_source_during_the_export_stays_removed_and_the_bootstrap_completes()
    {
        var source = _siteA.Client.GetGrain<ILattice>(TreeName).OrMap<string, PnCounter>(MapKey);
        await source.SetAsync("kept", SiteAClusterId, Counter(1));
        await source.SetAsync("removed", SiteAClusterId, Counter(2));

        _gated.Arm(TreeName);
        var coordinator = _siteB.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(TreeName);
        await coordinator.BootstrapAsync(SiteAClusterId, CancellationToken.None);
        await _gated.ExportReadAsync(TimeSpan.FromSeconds(60));

        // The export has read the map with both keys. The source now removes one,
        // and the removal reaches the receiver live, ahead of the stale row.
        await source.RemoveAsync("removed");
        var postRemove = await _siteA.Client.GetGrain<ILattice>(TreeName).GetAsync(MapKey);
        Assert.That(postRemove, Is.Not.Null, "precondition: the source still holds the map");
        var delivered = await Applier(_siteB).ApplyAsync(new WalRecord
        {
            TreeId = TreeName,
            Op = MutationKind.Set,
            Key = MapKey,
            Value = postRemove,
            Timestamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 0 },
            OriginClusterId = SiteAClusterId,
            Mode = LatticeMergeMode.OrMap,
        });
        Assert.That(delivered.Deferred, Is.False, "an OR-Map tree's bootstrap installs no drop floor to defer a delivery");

        _gated.Release();
        var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(60).TotalMilliseconds;
        LatticeBootstrapState state;
        do
        {
            await Task.Delay(100);
            state = await coordinator.GetStateAsync(CancellationToken.None);
        }
        while (state != LatticeBootstrapState.LiveIncremental
            && state != LatticeBootstrapState.Failed
            && Environment.TickCount64 < deadline);

        var receiverBytes = await _siteB.Client.GetGrain<ILattice>(TreeName).GetAsync(MapKey);
        Assert.That(receiverBytes, Is.Not.Null, "the receiver must hold the map");
        var receiver = (OrMap<string, PnCounter>)Shape.DeserializeState(receiverBytes!);
        var completedEpoch = await coordinator.GetCompletedExportEpochAsync(SiteAClusterId);

        Assert.Multiple(() =>
        {
            Assert.That(state, Is.EqualTo(LatticeBootstrapState.LiveIncremental), "the bootstrap must complete");
            Assert.That(receiver.Keys(), Is.EquivalentTo(new[] { "kept" }),
                "the removal observed the stale row's dot, so the stale row must not resurrect the key");
            Assert.That(completedEpoch, Is.Not.Null.And.GreaterThan(0),
                "an OR-Map bootstrap owes no reconcile, so its export epoch answers a sender's re-seed request");
        });
    }

    /// <summary>
    /// Wraps the source's real snapshot provider and, for an armed tree, reads
    /// the whole export before yielding any of it, then pauses until released,
    /// so a test can change the source between the export's read and its drain.
    /// </summary>
    private sealed class GatedSnapshotProvider(ISnapshotProvider inner) : ISnapshotProvider
    {
        private readonly object _sync = new();
        private string? _tree;
        private TaskCompletionSource _read = New();
        private TaskCompletionSource _released = New();

        private static TaskCompletionSource New() => new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Arm(string tree)
        {
            lock (_sync)
            {
                _tree = tree;
                _read = New();
                _released = New();
            }
        }

        public void Release() => _released.TrySetResult();

        public async Task ExportReadAsync(TimeSpan timeout)
        {
            var reached = await Task.WhenAny(_read.Task, Task.Delay(timeout));
            Assert.That(reached, Is.SameAs(_read.Task), "PRECONDITION: the export must have read the source and paused");
        }

        public async Task<SnapshotStream> ExportAsync(
            string treeName, HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default)
        {
            var stream = await inner.ExportAsync(treeName, asOfHlc, cancellationToken);
            lock (_sync)
            {
                if (!string.Equals(treeName, _tree, StringComparison.Ordinal))
                {
                    return stream;
                }
            }

            SnapshotStream? outer = null;
            outer = new SnapshotStream(treeName, stream.AsOfHlc, stream.CausalStableFrontier, GateAsync(stream, () => outer!))
            {
                OpenGeneration = stream.OpenGeneration,
                OpenFrontier = stream.OpenFrontier,
                ExportEpoch = stream.ExportEpoch,
                CrossTreeParticipantsComplete = stream.CrossTreeParticipantsComplete,
            };
            return outer;
        }

        private async IAsyncEnumerable<SnapshotEntry> GateAsync(
            SnapshotStream stream,
            Func<SnapshotStream> outer,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var entries = new List<SnapshotEntry>();
            await foreach (var entry in stream.Entries.WithCancellation(cancellationToken))
            {
                entries.Add(entry);
            }

            var target = outer();
            target.CloseGeneration = stream.CloseGeneration;
            target.SourceFrontier = stream.SourceFrontier;
            _read.TrySetResult();
            await _released.Task.WaitAsync(cancellationToken);
            foreach (var entry in entries)
            {
                yield return entry;
            }
        }
    }

    private sealed class SiteASiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddOrMapShape<string, PnCounter>(TreeName);
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = SiteAClusterId);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, OrMapResolver>();
        }
    }

    private sealed class SiteBSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddOrMapShape<string, PnCounter>(TreeName);
            siloBuilder.AddLatticeReplication(opts => opts.ClusterId = SiteBClusterId);

            if (SiteATransports.TryGetValue(SiteAClusterId, out var transport))
            {
                siloBuilder.Services.AddSingleton(transport);
            }

            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, OrMapResolver>();
        }
    }

    private sealed class OrMapResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) =>
            string.Equals(treeId, TreeName, StringComparison.Ordinal) ? LatticeMergeMode.OrMap : LatticeMergeMode.LwwRegister;
    }
}
