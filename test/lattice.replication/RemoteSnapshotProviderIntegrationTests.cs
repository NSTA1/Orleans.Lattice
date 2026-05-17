using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end coverage of <see cref="RemoteSnapshotProvider"/> driving
/// cross-cluster bootstrap. Brings up two independent
/// <see cref="TestCluster"/> instances:
/// <list type="bullet">
///   <item>
///     <description>
///       <b>Site A (sender):</b> pre-populated with N live entries via
///       the public <see cref="ILattice"/> surface; uses the default
///       in-cluster <see cref="LatticeSnapshotProvider"/>.
///     </description>
///   </item>
///   <item>
///     <description>
///       <b>Site B (receiver):</b> fresh; configured with
///       <see cref="RemoteSnapshotProvider"/> as
///       <see cref="ISnapshotProvider"/> and a loopback
///       <see cref="IRemoteSnapshotTransport"/> that round-trips into
///       site A's <see cref="LatticeRemoteSnapshotService"/>.
///     </description>
///   </item>
/// </list>
/// After <see cref="ILatticeBootstrapCoordinator.BootstrapAsync"/> on
/// site B completes, the local tree on site B must observe every entry
/// that existed on site A at the snapshot cut-point. This validates
/// the receiver-side adapter wiring, the three-arg
/// <see cref="ISnapshotProvider.ExportAsync(string, string, HybridLogicalClock, CancellationToken)"/>
/// overload, and the round-trip against the real sender-side handler.
/// </summary>
[TestFixture]
[Category("Integration")]
public class RemoteSnapshotProviderIntegrationTests
{
    private const string SiteAClusterId = "rsp-site-a";
    private const string SiteBClusterId = "rsp-site-b";

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

        // Build a sender-side handler bound to site A's grain client.
        // This is the real LatticeRemoteSnapshotService; site B's
        // loopback transport routes inbound metadata/stream calls
        // through it just as a wire transport would.
        var siteAProvider = new LatticeSnapshotProvider(
            _siteA.Client,
            new InMemoryWalCursorRegistry(),
            LatticeSnapshotProviderUnitTests.TestOptions());
        var siteAHandler = new LatticeRemoteSnapshotService(
            siteAProvider,
            NullLogger<LatticeRemoteSnapshotService>.Instance);
        SiteATransports[SiteAClusterId] = new InProcessSnapshotTransport(siteAHandler);

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
    public async Task BootstrapAsync_drains_remote_snapshot_into_local_tree()
    {
        const string tree = "rsp-bootstrap";

        // Populate site A with a handful of live entries.
        var siteALattice = _siteA.Client.GetGrain<ILattice>(tree);
        await siteALattice.SetAsync("a", [0x01]);
        await siteALattice.SetAsync("b", [0x02]);
        await siteALattice.SetAsync("c", [0x03]);

        // Trigger bootstrap on site B. The state machine resolves
        // ISnapshotProvider as RemoteSnapshotProvider, which calls into
        // the loopback transport, which round-trips into site A's
        // LatticeRemoteSnapshotService and drains the snapshot.
        var coord = _siteB.Client.GetGrain<Orleans.Lattice.Replication.Grains.ILatticeBootstrapCoordinatorGrain>(tree);
        await coord.BootstrapAsync(SiteAClusterId, CancellationToken.None);

        // Poll for completion. The coordinator's work-pump runs on a
        // reminder/timer cadence so progress is observable through
        // GetStateAsync.
        var deadline = DateTimeOffset.UtcNow.AddSeconds(60);
        LatticeBootstrapState state;
        do
        {
            await Task.Delay(250);
            state = await coord.GetStateAsync(CancellationToken.None);
        }
        while (state != LatticeBootstrapState.LiveIncremental
            && state != LatticeBootstrapState.Failed
            && DateTimeOffset.UtcNow < deadline);

        Assert.That(state, Is.EqualTo(LatticeBootstrapState.LiveIncremental),
            "Bootstrap did not reach LiveIncremental within the timeout.");

        // Site B's local tree must now contain every entry from site A.
        var siteBLattice = _siteB.Client.GetGrain<ILattice>(tree);
        Assert.Multiple(async () =>
        {
            Assert.That(await siteBLattice.GetAsync("a"), Is.EqualTo(new byte[] { 0x01 }));
            Assert.That(await siteBLattice.GetAsync("b"), Is.EqualTo(new byte[] { 0x02 }));
            Assert.That(await siteBLattice.GetAsync("c"), Is.EqualTo(new byte[] { 0x03 }));
        });
    }

    /// <summary>
    /// In-process <see cref="IRemoteSnapshotTransport"/> that forwards
    /// every call directly to the supplied handler. Stands in for a
    /// wire-shaped (gRPC, HTTP) transport in unit-style integration
    /// tests so the receiver-side adapter is exercised against the
    /// real sender-side handler without standing up a real listener.
    /// </summary>
    private sealed class InProcessSnapshotTransport(IRemoteSnapshotTransport handler) : IRemoteSnapshotTransport
    {
        public Task<RemoteSnapshotMetadata> GetMetadataAsync(
            string treeName,
            string sourceClusterId,
            HybridLogicalClock fromAsOfHlc,
            CancellationToken cancellationToken = default)
            => handler.GetMetadataAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken);

        public async IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
            string treeName,
            string sourceClusterId,
            HybridLogicalClock fromAsOfHlc,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var entry in handler
                .RequestSnapshotAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken)
                .WithCancellation(cancellationToken)
                .ConfigureAwait(false))
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

            // Wire the loopback transport built in SetUp; this is what
            // RemoteSnapshotProvider drives instead of a real wire
            // binding.
            if (SiteATransports.TryGetValue(SiteAClusterId, out var transport))
            {
                siloBuilder.Services.AddSingleton<IRemoteSnapshotTransport>(transport);
            }

            // Replace ISnapshotProvider with the cross-cluster adapter.
            siloBuilder.AddRemoteSnapshotProvider();

            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
