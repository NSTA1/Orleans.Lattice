using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Enrollment;

namespace Orleans.Lattice.GrainIndex.Tests.Enrollment;

/// <summary>
/// Covers <see cref="GrainIndexOutboxHostedService"/>: that it runs the drain on
/// a schedule without blocking start-up, honours the disable switch, and stops
/// cleanly.
/// </summary>
/// <remarks>
/// Nothing here sleeps. The store signals the moment it is scanned, so "the
/// background pass happened" is an awaited signal with a generous upper bound
/// rather than a guess at how long a tick takes.
/// </remarks>
[TestFixture]
public sealed class GrainIndexOutboxHostedServiceTests
{
    private static readonly TimeSpan SignalBudget = TimeSpan.FromSeconds(30);

    private static GrainIndexOutboxDrainer DrainerOver(RecordingEnrollmentStore store)
    {
        // The tree is built before the Returns(...) call: creating a substitute
        // inside one clobbers NSubstitute's record of the call being configured.
        var tree = EnrollmentTrees.Accepting();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>()).Returns(tree);

        var services = new ServiceCollection();
        services.AddOptions();
        var options = services.BuildServiceProvider()
            .GetRequiredService<IOptionsMonitor<GrainIndexOptions>>();

        return new GrainIndexOutboxDrainer(
            store,
            factory,
            options,
            NullLogger<GrainIndexOutboxDrainer>.Instance);
    }

    private static GrainIndexOutboxHostedService ServiceOver(
        RecordingEnrollmentStore store,
        Action<GrainIndexOutboxOptions>? configure = null)
    {
        var options = new GrainIndexOutboxOptions { RetryInterval = TimeSpan.FromMilliseconds(10) };
        configure?.Invoke(options);

        return new GrainIndexOutboxHostedService(
            DrainerOver(store),
            Options.Create(options),
            NullLogger<GrainIndexOutboxHostedService>.Instance);
    }

    [Test]
    public async Task Starting_the_service_does_not_block_on_a_drain_pass()
    {
        var store = new RecordingEnrollmentStore();
        using var service = ServiceOver(store);

        var start = service.StartAsync(CancellationToken.None);

        Assert.That(start.IsCompleted, Is.True,
            "Repair must not gate start-up: a silo whose outbox cannot be drained yet is still a "
            + "correct silo.");

        await start;
        await service.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task The_running_service_drains_the_outbox()
    {
        var store = new RecordingEnrollmentStore();
        using var service = ServiceOver(store);

        await service.StartAsync(CancellationToken.None);
        await store.ScanObserved.Task.WaitAsync(SignalBudget);
        await service.StopAsync(CancellationToken.None);

        Assert.That(store.Log, Does.Contain("scan"));
    }

    [Test]
    public async Task A_disabled_service_never_scans()
    {
        var store = new RecordingEnrollmentStore();
        using var service = ServiceOver(store, options => options.Enabled = false);

        await service.StartAsync(CancellationToken.None);
        await service.StopAsync(CancellationToken.None);

        Assert.That(store.Log, Is.Empty,
            "Disabling the drain must not disable the outbox: entries are still recorded, they are "
            + "simply applied by whoever the host chooses.");
    }

    [Test]
    public async Task Stopping_a_service_that_never_started_is_harmless()
    {
        using var service = ServiceOver(new RecordingEnrollmentStore());

        Assert.That(async () => await service.StopAsync(CancellationToken.None), Throws.Nothing);
        await Task.CompletedTask;
    }

    [Test]
    public async Task A_drain_pass_that_throws_does_not_take_the_silo_down()
    {
        var store = new RecordingEnrollmentStore { ReadFault = new InvalidOperationException("registry down") };
        using var service = ServiceOver(store);

        await service.StartAsync(CancellationToken.None);
        await store.ScanObserved.Task.WaitAsync(SignalBudget);

        Assert.That(async () => await service.StopAsync(CancellationToken.None), Throws.Nothing,
            "The outbox exists to survive exactly this kind of interruption, so a failing pass is "
            + "logged and retried rather than propagated.");
    }

    [Test]
    public async Task A_non_positive_retry_interval_falls_back_to_the_default()
    {
        var store = new RecordingEnrollmentStore();
        using var service = ServiceOver(store, options => options.RetryInterval = TimeSpan.Zero);

        Assert.That(async () => await service.StartAsync(CancellationToken.None), Throws.Nothing,
            "A zero interval would otherwise be rejected by the timer and crash the loop before "
            + "its first pass.");

        await service.StopAsync(CancellationToken.None);
    }

    [Test]
    public void A_null_dependency_is_rejected_at_construction()
    {
        var drainer = DrainerOver(new RecordingEnrollmentStore());
        var options = Options.Create(new GrainIndexOutboxOptions());

        Assert.Multiple(() =>
        {
            Assert.That(
                () => new GrainIndexOutboxHostedService(
                    null!, options, NullLogger<GrainIndexOutboxHostedService>.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexOutboxHostedService(
                    drainer, null!, NullLogger<GrainIndexOutboxHostedService>.Instance),
                Throws.ArgumentNullException);
            Assert.That(
                () => new GrainIndexOutboxHostedService(drainer, options, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Disposing_twice_is_harmless()
    {
        var service = ServiceOver(new RecordingEnrollmentStore());

        service.Dispose();
        Assert.That(service.Dispose, Throws.Nothing);
    }
}
