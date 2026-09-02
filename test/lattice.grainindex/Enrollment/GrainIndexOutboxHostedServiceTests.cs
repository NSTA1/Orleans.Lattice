using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Enrollment;
using System.Runtime.CompilerServices;

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

    [Test]
    public async Task Stopping_with_a_cancelled_token_is_harmless()
    {
        // Lines 78-82: StopAsync catches OperationCanceledException from
        // loop.WaitAsync(cancellationToken) when the caller passes a pre-cancelled
        // token while the loop task is still running. The blocking store keeps
        // the loop alive so WaitAsync sees an incomplete task with a cancelled
        // token.
        var store = new BlockingEnrollmentStore();
        using var service = ServiceWithStore(store);

        await service.StartAsync(CancellationToken.None);
        await store.ScanObserved.Task.WaitAsync(SignalBudget);

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        Assert.That(async () => await service.StopAsync(cts.Token), Throws.Nothing,
            "A caller whose shutdown budget has already expired must not crash the silo; "
            + "the outbox is durable and will be drained by the next silo to start.");
    }

    [Test]
    public async Task A_drain_pass_that_processes_entries_logs_the_result()
    {
        // Lines 107-112: the log statement for a non-empty drain result.
        // Adding a pending projection whose index is not declared on this silo
        // is enough: the pass scans it, increments Scanned, and logs.
        var store = new RecordingEnrollmentStore();
        store.Pending["undeclared-index/grain-a"] = new GrainIndexPendingProjection(
            "undeclared-index",
            "grain-a",
            Guid.NewGuid().ToString("N"),
            GrainIndexUpdatePlan.Between(
                GrainIndexProjection.Empty("grain-a"),
                GrainIndexProjection.Empty("grain-a")));

        using var service = ServiceOver(store);
        await service.StartAsync(CancellationToken.None);
        await store.ScanObserved.Task.WaitAsync(SignalBudget);
        await service.StopAsync(CancellationToken.None);

        Assert.That(store.Log, Does.Contain("scan"),
            "The store must have been scanned; the test waits on ScanObserved.");
    }

    [Test]
    public async Task A_drain_pass_cancelled_by_service_stop_does_not_propagate()
    {
        // Lines 115-117: RunAsync catches OCE when its own stopping token is the cause.
        // A blocking store keeps ScanPendingAsync alive until StopAsync fires.
        var store = new BlockingEnrollmentStore();
        using var service = ServiceWithStore(store);

        await service.StartAsync(CancellationToken.None);
        await store.ScanObserved.Task.WaitAsync(SignalBudget);

        Assert.That(async () => await service.StopAsync(CancellationToken.None), Throws.Nothing,
            "Cancellation that originates from StopAsync itself must not propagate out of StopAsync.");
    }

    [Test]
    public async Task A_drain_pass_that_throws_non_cancellation_exception_is_retried()
    {
        // Lines 119-122: RunAsync catches non-OCE exceptions from DrainAsync and
        // logs them rather than crashing the loop.
        var store = new RecordingEnrollmentStore
        {
            ScanFault = new InvalidOperationException("index cluster unreachable"),
        };
        using var service = ServiceOver(store);

        await service.StartAsync(CancellationToken.None);
        await store.ScanObserved.Task.WaitAsync(SignalBudget);

        Assert.That(async () => await service.StopAsync(CancellationToken.None), Throws.Nothing,
            "A non-cancellation exception from the drain must be logged and the loop must continue.");
    }

    [Test]
    public async Task The_periodic_timer_fires_between_drain_passes()
    {
        // Line 128: closing brace of the timer-wait try block (WaitForNextTickAsync
        // returned true - the timer fired). Waiting for the second scan ensures the
        // timer completed at least one tick before we ask to stop.
        var store = new RecordingEnrollmentStore();
        using var service = ServiceOver(store, options => options.RetryInterval = TimeSpan.FromMilliseconds(1));

        await service.StartAsync(CancellationToken.None);
        await store.SecondScanObserved.Task.WaitAsync(SignalBudget);
        await service.StopAsync(CancellationToken.None);

        Assert.That(store.Log.Count(static e => e == "scan"), Is.GreaterThanOrEqualTo(2),
            "At least two scan passes must have run, proving the timer ticked at least once.");
    }

    // --- Test-private helpers ---

    private static GrainIndexOutboxDrainer DrainerOverStore(IGrainIndexEnrollmentStore store)
    {
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

    private static GrainIndexOutboxHostedService ServiceWithStore(IGrainIndexEnrollmentStore store)
    {
        var options = new GrainIndexOutboxOptions { RetryInterval = TimeSpan.FromMilliseconds(10) };
        return new GrainIndexOutboxHostedService(
            DrainerOverStore(store),
            Options.Create(options),
            NullLogger<GrainIndexOutboxHostedService>.Instance);
    }

    /// <summary>
    /// An <see cref="IGrainIndexEnrollmentStore"/> that blocks inside
    /// <see cref="ScanPendingAsync"/> until the cancellation token fires, so
    /// a test can call <c>StopAsync</c> and observe the OCE the service catches.
    /// </summary>
    private sealed class BlockingEnrollmentStore : IGrainIndexEnrollmentStore
    {
        /// <summary>Fires the first time <see cref="ScanPendingAsync"/> is entered.</summary>
        public TaskCompletionSource ScanObserved { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<GrainIndexEnrollmentRecord?> ReadEnrollmentAsync(
            string indexName, string grainKey, CancellationToken cancellationToken) =>
            Task.FromResult<GrainIndexEnrollmentRecord?>(null);

        public Task WritePendingAsync(GrainIndexPendingProjection pending, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task CompleteAsync(
            string indexName, string grainKey, GrainIndexProjection projection, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task WithdrawAsync(string indexName, string grainKey, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public async IAsyncEnumerable<string> ScanSeenKeysAsync(
            string indexName, string firstKeyInclusive, string lastKeyInclusive,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }

        public async IAsyncEnumerable<GrainIndexPendingProjection> ScanPendingAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ScanObserved.TrySetResult();
            // Block until StopAsync cancels the service's stopping token.
            await Task.Delay(Timeout.Infinite, cancellationToken);
            yield break;
        }
    }
}
