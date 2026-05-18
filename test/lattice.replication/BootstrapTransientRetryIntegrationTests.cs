using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Lattice.Primitives;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end coverage of the receiver-side bootstrap drain's
/// bounded transient-retry behaviour: a flaky cross-cluster transport
/// that throws a classified-transient exception on the first attempt
/// must be auto-resumed by the coordinator without operator action,
/// and the resulting bootstrap must reach
/// <see cref="LatticeBootstrapState.LiveIncremental"/> with every
/// sender-side entry applied. Pairs with the unit-level
/// transient-retry tests in
/// <c>LatticeBootstrapCoordinatorGrainTests.TransientRetry</c> to
/// validate that the policy plumbing survives the real grain-pump,
/// reminder/timer cadence, and per-origin HWM dedupe paths.
/// </summary>
[TestFixture]
[Category("Integration")]
public class BootstrapTransientRetryIntegrationTests
{
    private const string SiteAClusterId = "btr-site-a";
    private const string SiteBClusterId = "btr-site-b";

    private static readonly ConcurrentDictionary<string, FlakyRemoteSnapshotTransport> SiteATransports = new();

    private TestCluster _siteA = null!;
    private TestCluster _siteB = null!;
    private FlakyRemoteSnapshotTransport _flakyTransport = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var aBuilder = new TestClusterBuilder(initialSilosCount: 1);
        aBuilder.AddSiloBuilderConfigurator<SiteASiloConfigurator>();
        _siteA = aBuilder.Build();
        await _siteA.DeployAsync();

        // Build the real sender-side handler bound to site A's grain
        // client, then wrap it in a flaky transport that fails the
        // first RequestSnapshotAsync call before passing the second
        // through. The transient-retry path in the receiver-side
        // bootstrap drain must absorb the first failure and succeed
        // on the second attempt.
        var siteAProvider = new LatticeSnapshotProvider(
            _siteA.Client,
            new InMemoryWalCursorRegistry(),
            LatticeSnapshotProviderUnitTests.TestOptions());
        var siteAHandler = new LatticeRemoteSnapshotService(
            siteAProvider,
            NullLogger<LatticeRemoteSnapshotService>.Instance);
        _flakyTransport = new FlakyRemoteSnapshotTransport(siteAHandler, failuresBeforeSuccess: 1);
        SiteATransports[SiteAClusterId] = _flakyTransport;

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
    public async Task BootstrapAsync_resumes_after_transient_transport_fault_and_reaches_LiveIncremental()
    {
        const string tree = "btr-bootstrap";

        // Populate site A with entries so the drain has work to do.
        var siteALattice = _siteA.Client.GetGrain<ILattice>(tree);
        await siteALattice.SetAsync("a", [0x01]);
        await siteALattice.SetAsync("b", [0x02]);
        await siteALattice.SetAsync("c", [0x03]);

        // Trigger bootstrap on site B. The first transport call throws
        // a classified-transient exception; the receiver's bounded
        // retry policy must absorb it, re-call ExportAsync, and the
        // second attempt drains the snapshot cleanly. We never call
        // ForceRequestSnapshotAsync - the test would fail to converge
        // without the auto-retry seam.
        var coord = _siteB.Client.GetGrain<Orleans.Lattice.Replication.Grains.ILatticeBootstrapCoordinatorGrain>(tree);
        await coord.BootstrapAsync(SiteAClusterId, CancellationToken.None);

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

        Assert.Multiple(async () =>
        {
            Assert.That(state, Is.EqualTo(LatticeBootstrapState.LiveIncremental),
                "Bootstrap must reach LiveIncremental despite the first transport call throwing a classified-transient fault.");
            Assert.That(_flakyTransport.AttemptCount, Is.GreaterThanOrEqualTo(2),
                "The transport must observe at least two RequestSnapshotAsync attempts (first fails, second succeeds).");

            var siteBLattice = _siteB.Client.GetGrain<ILattice>(tree);
            Assert.That(await siteBLattice.GetAsync("a"), Is.EqualTo(new byte[] { 0x01 }));
            Assert.That(await siteBLattice.GetAsync("b"), Is.EqualTo(new byte[] { 0x02 }));
            Assert.That(await siteBLattice.GetAsync("c"), Is.EqualTo(new byte[] { 0x03 }));
        });
    }

    /// <summary>
    /// Wraps an inner <see cref="IRemoteSnapshotTransport"/> and
    /// throws a classified-transient exception on the first N
    /// invocations of <see cref="RequestSnapshotAsync"/> before
    /// passing through to the inner. Mirrors the shape of a
    /// real cross-cluster gRPC channel reset: the first stream
    /// faults, the bounded retry policy on the receiver re-issues
    /// the call, and the channel has recovered by the time the
    /// second attempt runs. Thread-safe via
    /// <see cref="Interlocked.Increment"/>.
    /// </summary>
    private sealed class FlakyRemoteSnapshotTransport(
        IRemoteSnapshotTransport inner,
        int failuresBeforeSuccess) : IRemoteSnapshotTransport
    {
        private int _attemptCount;
        private readonly int _failuresBeforeSuccess = failuresBeforeSuccess;

        /// <summary>
        /// Number of <see cref="RequestSnapshotAsync"/> attempts the
        /// transport has observed so far. The retry-resume integration
        /// test asserts this is at least 2 (first attempt throws,
        /// second succeeds).
        /// </summary>
        public int AttemptCount => Volatile.Read(ref _attemptCount);

        public Task<RemoteSnapshotMetadata> GetMetadataAsync(
            string treeName,
            string sourceClusterId,
            HybridLogicalClock fromAsOfHlc,
            CancellationToken cancellationToken = default)
            => inner.GetMetadataAsync(treeName, sourceClusterId, fromAsOfHlc, cancellationToken);

        public async IAsyncEnumerable<SnapshotEntry> RequestSnapshotAsync(
            string treeName,
            string sourceClusterId,
            HybridLogicalClock fromAsOfHlc,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var attempt = Interlocked.Increment(ref _attemptCount);
            if (attempt <= _failuresBeforeSuccess)
            {
                // TimeoutException is recognised by the default
                // LatticeBootstrapTransientFaultClassifier as
                // transient, so the receiver's bounded retry policy
                // consumes one slot and re-opens the stream.
                throw new TimeoutException(
                    $"FlakyRemoteSnapshotTransport: synthetic transient failure on attempt #{attempt} (configured failuresBeforeSuccess={_failuresBeforeSuccess}).");
            }

            await foreach (var entry in inner
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
            siloBuilder.AddLatticeReplication(opts =>
            {
                opts.ClusterId = SiteBClusterId;

                // Configure the bootstrap retry policy with zero
                // backoff and a budget large enough to absorb the
                // synthetic transient failure injected by the flaky
                // transport. The default policy works too, but the
                // explicit configuration makes the test deterministic:
                // it never sleeps on a real wall-clock delay.
                opts.BootstrapTransientRetry = new BoundedExponentialRetryPolicyOptions
                {
                    MaxAttempts = 3,
                    InitialDelay = TimeSpan.Zero,
                    MaxDelay = TimeSpan.Zero,
                };
            });

            if (SiteATransports.TryGetValue(SiteAClusterId, out var transport))
            {
                siloBuilder.Services.AddSingleton<IRemoteSnapshotTransport>(transport);
            }

            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }
}
