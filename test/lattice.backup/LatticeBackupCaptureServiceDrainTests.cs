using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Wal;
using Orleans.Serialization;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeBackupCaptureService"/> that do not require a
/// live silo. Covers the cross-tree drain loop in DrainCrossTreeInFlightAsync:
/// <list type="bullet">
/// <item>Line 428 (while-true entry), line 442 (peakInFlight update), and
/// lines 452-455 (drain timeout throw) when the registry reports in-flight
/// sagas and the drain timeout is zero.</item>
/// </list>
/// </summary>
[TestFixture]
public sealed class LatticeBackupCaptureServiceDrainTests
{
    private ServiceProvider _services = null!;
    private Serializer _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private LatticeBackupCaptureService CreateService(
        IGrainFactory grainFactory,
        LatticeBackupOptions? backupOpts = null)
    {
        var opts = backupOpts ?? new LatticeBackupOptions();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var optionsResolver = new LatticeOptionsResolver(grainFactory, optionsMonitor);
        var gate = Substitute.For<ILatticeAccessGate>();
        var authorizer = new BackupAccessAuthorizer(gate);

        return new LatticeBackupCaptureService(
            grainFactory,
            Substitute.For<ILatticeBackupSink>(),
            Substitute.For<ILatticeBackupCatalogStore>(),
            authorizer,
            optionsMonitor,
            Options.Create(opts),
            Substitute.For<ILatticeMergeModeResolver>(),
            _serializer,
            Substitute.For<ICommitLogReader>(),
            Substitute.For<IWalSubscriber>(),
            optionsResolver,
            Substitute.For<IWalCursorRegistry>(),
            Options.Create(new ClusterOptions()),
            NullLogger<LatticeBackupCaptureService>.Instance);
    }

    [Test]
    public async Task CaptureSetAsync_throws_on_drain_timeout_when_sagas_stay_in_flight()
    {
        // Lines 428 (while entry), 442 (peakInFlight = totalInFlight), and
        // 452-455 (drain timeout throw):
        // With CrossTreeFenceDrainTimeout = TimeSpan.Zero the condition
        // (sw.Elapsed >= timeout) is satisfied on the very first iteration,
        // so a registry that always reports one in-flight saga causes the drain
        // to throw LatticeBackupCrossTreeFenceException before any tree capture
        // or authorization runs.
        var grainFactory = Substitute.For<IGrainFactory>();

        var registry = Substitute.For<ITxRegistryGrain>();
        registry.ObserveCrossTreeInFlightAsync()
            .Returns(Task.FromResult(new CrossTreeInFlightObservation(inFlightCount: 1, registrationEpoch: 0)));

        grainFactory.GetGrain<ITxRegistryGrain>(Arg.Any<string>()).Returns(registry);

        var opts = new LatticeBackupOptions
        {
            // A zero timeout guarantees the timeout branch fires on the first
            // iteration regardless of actual elapsed time.
            CrossTreeFenceDrainTimeout = TimeSpan.Zero,
            MaxCrossTreeFenceAttempts = 1,
            CrossTreeFencePollInterval = TimeSpan.FromMilliseconds(1),
        };

        var service = CreateService(grainFactory, opts);

        // CaptureSetAsync with CrossTreeConsistent=true and 2+ scopes enters
        // CaptureFencedSetAsync -> DrainCrossTreeInFlightAsync.
        var request = new LatticeBackupSetCaptureRequest(
            name: "test-set",
            scopes: new[]
            {
                BackupScopeSelector.WholeTree("orders"),
                BackupScopeSelector.WholeTree("products"),
            },
            crossTreeConsistent: true);

        Assert.That(
            async () => await service.CaptureSetAsync(request),
            Throws.TypeOf<LatticeBackupCrossTreeFenceException>()
                .With.Message.Contains("Timed out"));
    }
}
