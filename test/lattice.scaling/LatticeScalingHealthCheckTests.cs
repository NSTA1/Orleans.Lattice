using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Unit coverage for <see cref="LatticeScalingHealthCheck"/>. Instantiates the
/// check directly with a substituted <see cref="ILatticeScalingSignal"/> and an
/// injected <see cref="TimeProvider"/> so no host is required and every band of
/// the threshold projection is deterministic. Verifies the constructor guards,
/// each Healthy / Degraded / Unhealthy band across every signal, the disable
/// toggles, and the populated <c>data</c> dictionary.
/// </summary>
[TestFixture]
public sealed class LatticeScalingHealthCheckTests
{
    private const string DefaultName = LatticeScalingHealthCheckOptions.DefaultName;

    [Test]
    public void Constructor_throws_on_null_signal()
    {
        Assert.That(
            () => new LatticeScalingHealthCheck(
                null!,
                BuildOptionsMonitor(new LatticeScalingHealthCheckOptions()),
                NullLogger<LatticeScalingHealthCheck>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_options_monitor()
    {
        Assert.That(
            () => new LatticeScalingHealthCheck(
                Substitute.For<ILatticeScalingSignal>(),
                null!,
                NullLogger<LatticeScalingHealthCheck>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_logger()
    {
        Assert.That(
            () => new LatticeScalingHealthCheck(
                Substitute.For<ILatticeScalingSignal>(),
                BuildOptionsMonitor(new LatticeScalingHealthCheckOptions()),
                null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void CheckHealthAsync_throws_on_null_context()
    {
        var check = CreateCheck(Signal(), new LatticeScalingHealthCheckOptions());

        Assert.That(
            async () => await check.CheckHealthAsync(null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task CheckHealthAsync_returns_healthy_when_every_signal_below_bounds()
    {
        var signal = Signal(activation: 0.5d, resource: 0.4d, walDispatch: 0.6d);
        var check = CreateCheck(signal, new LatticeScalingHealthCheckOptions());

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
    }

    [Test]
    public async Task CheckHealthAsync_returns_degraded_when_worst_compute_exceeds_soft_bound()
    {
        // Resource is the worst dimension at 0.90, between 0.85 soft and 0.95 hard.
        var signal = Signal(activation: 0.1d, resource: 0.90d, walDispatch: 0.2d);
        var check = CreateCheck(signal, new LatticeScalingHealthCheckOptions());

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Degraded));
    }

    [Test]
    public async Task CheckHealthAsync_returns_unhealthy_when_worst_compute_exceeds_hard_bound()
    {
        var signal = Signal(activation: 0.1d, resource: 0.2d, walDispatch: 0.97d);
        var check = CreateCheck(signal, new LatticeScalingHealthCheckOptions());

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
    }

    [Test]
    public async Task CheckHealthAsync_null_compute_tier_disables_pressure_signal()
    {
        var signal = Signal(activation: 0.99d, resource: 0.99d, walDispatch: 0.99d);
        var options = new LatticeScalingHealthCheckOptions
        {
            ComputePressure = null,
            UnhealthyOnWalSaturated = false,
            DegradeOnWalThrottled = false,
            DegradeOnStorageOverThreshold = false,
        };
        var check = CreateCheck(signal, options);

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
    }

    [Test]
    public async Task CheckHealthAsync_returns_unhealthy_when_wal_saturated()
    {
        var signal = Signal(walSaturation: WalSaturationState.Saturated);
        var check = CreateCheck(signal, new LatticeScalingHealthCheckOptions());

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
            Assert.That(result.Data["walSaturation"], Is.EqualTo("Saturated"));
        });
    }

    [Test]
    public async Task CheckHealthAsync_returns_degraded_when_wal_throttled()
    {
        var signal = Signal(walSaturation: WalSaturationState.Throttled);
        var check = CreateCheck(signal, new LatticeScalingHealthCheckOptions());

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Degraded));
    }

    [Test]
    public async Task CheckHealthAsync_wal_saturated_ignored_when_toggle_disabled()
    {
        var signal = Signal(walSaturation: WalSaturationState.Saturated);
        var options = new LatticeScalingHealthCheckOptions { UnhealthyOnWalSaturated = false };
        var check = CreateCheck(signal, options);

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
    }

    [Test]
    public async Task CheckHealthAsync_returns_degraded_when_storage_over_threshold()
    {
        var signal = Signal(overThreshold: true, walRetainedBytes: 4096);
        var check = CreateCheck(signal, new LatticeScalingHealthCheckOptions());

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Degraded));
            Assert.That(result.Data["storageOverThreshold"], Is.True);
            Assert.That(result.Data["storageWalRetainedBytes"], Is.EqualTo(4096L));
        });
    }

    [Test]
    public async Task CheckHealthAsync_storage_over_threshold_never_escalates_past_degraded()
    {
        // Storage is advisory: even a maxed storage axis must not exceed
        // Degraded on its own.
        var signal = Signal(overThreshold: true, walRetainedBytes: long.MaxValue);
        var options = new LatticeScalingHealthCheckOptions
        {
            ComputePressure = null,
            UnhealthyOnWalSaturated = false,
            DegradeOnWalThrottled = false,
        };
        var check = CreateCheck(signal, options);

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Degraded));
    }

    [Test]
    public async Task CheckHealthAsync_storage_over_threshold_ignored_when_toggle_disabled()
    {
        var signal = Signal(overThreshold: true);
        var options = new LatticeScalingHealthCheckOptions { DegradeOnStorageOverThreshold = false };
        var check = CreateCheck(signal, options);

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
    }

    [Test]
    public async Task CheckHealthAsync_reports_worst_across_multiple_axes()
    {
        // Compute degraded (0.90) AND WAL saturated -> the worse of the two
        // (Unhealthy) wins.
        var signal = Signal(resource: 0.90d, walSaturation: WalSaturationState.Saturated, overThreshold: true);
        var check = CreateCheck(signal, new LatticeScalingHealthCheckOptions());

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
    }

    [Test]
    public async Task CheckHealthAsync_populates_data_dictionary()
    {
        var sampledAt = new DateTimeOffset(2024, 3, 4, 5, 6, 7, TimeSpan.Zero);
        var signal = Signal(
            scaleValue: 3.5d,
            recommendedReplicas: 4,
            reason: "compute axis dominates",
            activation: 0.3d,
            resource: 0.7d,
            walDispatch: 0.2d,
            sampledAt: sampledAt);
        var time = new FakeTimeProvider { UtcNow = new DateTimeOffset(2024, 3, 4, 5, 6, 8, TimeSpan.Zero) };
        var check = CreateCheck(signal, new LatticeScalingHealthCheckOptions(), time);

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Data["scaleValue"], Is.EqualTo(3.5d));
            Assert.That(result.Data["recommendedReplicas"], Is.EqualTo(4));
            Assert.That(result.Data["reason"], Is.EqualTo("compute axis dominates"));
            Assert.That(result.Data["computeActivation"], Is.EqualTo(0.3d));
            Assert.That(result.Data["computeResource"], Is.EqualTo(0.7d));
            Assert.That(result.Data["computeWalDispatch"], Is.EqualTo(0.2d));
            Assert.That(result.Data["computeWorst"], Is.EqualTo(0.7d));
            Assert.That(result.Data["walSaturation"], Is.EqualTo("Healthy"));
            Assert.That(result.Data["storageOverThreshold"], Is.False);
            Assert.That(result.Data["sampledAt"], Is.EqualTo(sampledAt));
            Assert.That(result.Data["checkedAt"], Is.EqualTo(time.UtcNow));
        });
    }

    [Test]
    public async Task CheckHealthAsync_uses_named_options_for_registration_name()
    {
        // A registration under a custom name must bind the options monitor's
        // Get(name) so per-name thresholds take effect.
        var signal = Signal(resource: 0.90d);
        var lenient = new LatticeScalingHealthCheckOptions
        {
            ComputePressure = new LatticeScalingHealthCheckOptions.DoubleTier(0.95d, 0.99d),
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeScalingHealthCheckOptions>>();
        monitor.Get("custom").Returns(lenient);
        var check = new LatticeScalingHealthCheck(signal, monitor, NullLogger<LatticeScalingHealthCheck>.Instance);

        var result = await check.CheckHealthAsync(Context("custom"), CancellationToken.None);

        // 0.90 is below the lenient 0.95 soft bound -> Healthy.
        Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
    }

    private static LatticeScalingHealthCheck CreateCheck(
        ILatticeScalingSignal signal,
        LatticeScalingHealthCheckOptions options,
        TimeProvider? time = null) =>
        new(
            signal,
            BuildOptionsMonitor(options),
            NullLogger<LatticeScalingHealthCheck>.Instance,
            time);

    private static ILatticeScalingSignal Signal(
        double scaleValue = 0d,
        int recommendedReplicas = 0,
        string reason = "test",
        double activation = 0d,
        double resource = 0d,
        double walDispatch = 0d,
        WalSaturationState walSaturation = WalSaturationState.Healthy,
        bool overThreshold = false,
        long walRetainedBytes = 0L,
        DateTimeOffset? sampledAt = null)
    {
        var snapshot = new ScalingSignal
        {
            ScaleValue = scaleValue,
            RecommendedReplicas = recommendedReplicas,
            Reason = reason,
            Compute = new ComputePressure
            {
                Activation = activation,
                Resource = resource,
                WalDispatch = walDispatch,
                WalSaturation = walSaturation,
            },
            Storage = new StoragePressure
            {
                OverThreshold = overThreshold,
                WalRetainedBytes = walRetainedBytes,
            },
            SampledAt = sampledAt ?? DateTimeOffset.UnixEpoch,
        };

        var facade = Substitute.For<ILatticeScalingSignal>();
        facade.GetScalingSignalAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(snapshot));
        return facade;
    }

    private static IOptionsMonitor<LatticeScalingHealthCheckOptions> BuildOptionsMonitor(
        LatticeScalingHealthCheckOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeScalingHealthCheckOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static HealthCheckContext Context(string name = DefaultName) => new()
    {
        Registration = new HealthCheckRegistration(
            name,
            Substitute.For<IHealthCheck>(),
            failureStatus: null,
            tags: null),
    };

    private sealed class FakeTimeProvider : TimeProvider
    {
        public DateTimeOffset UtcNow { get; set; }

        public override DateTimeOffset GetUtcNow() => UtcNow;
    }
}
