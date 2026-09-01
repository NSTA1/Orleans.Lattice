using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="TenantResidencyWarmupHostedService"/>, the start-up
/// background service that warms the residency snapshot once. The happy path (the
/// registry is reachable immediately) completes on the first
/// <see cref="TenantResidencySnapshotMaintainer.EnsureWarmAsync"/> and needs no
/// delay, so it is driven with no timing dependency; the retry cadence itself is
/// not exercised by wall-clock here.
/// </summary>
[TestFixture]
public sealed class TenantResidencyWarmupHostedServiceTests
{
    private static async IAsyncEnumerable<TenantRecord> Empty(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask;
        yield break;
    }

    // An async-enumerable registry scan that signals entry, then stalls until the
    // cancellation token fires. Used to test the OCE-when-cancellation path.
    private static async IAsyncEnumerable<TenantRecord> StallUntilCancelled(
        TaskCompletionSource entered,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        entered.TrySetResult();
        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
        yield break;
    }

    // An async-enumerable registry scan that immediately throws a non-OCE. Used to
    // test the general-exception-catch path and the retry-delay-cancel path.
    private static async IAsyncEnumerable<TenantRecord> ThrowAsync(
        Exception ex,
        [EnumeratorCancellation] CancellationToken _ = default)
    {
        await Task.CompletedTask;
        throw ex;
#pragma warning disable CS0162
        yield break;
#pragma warning restore CS0162
    }

    private static TenantResidencySnapshotMaintainer Maintainer(ITenantRegistry registry) =>
        new(
            registry,
            Options.Create(new ClusterOptions { ClusterId = "region-a" }),
            Array.Empty<ITenantRegionStatusChangeListener>(),
            NullLogger<TenantResidencySnapshotMaintainer>.Instance);

    private static TenantResidencyWarmupHostedService Service(
        TenantResidencySnapshotMaintainer maintainer,
        TimeProvider? timeProvider = null) =>
        new(maintainer, timeProvider ?? TimeProvider.System, NullLogger<TenantResidencyWarmupHostedService>.Instance);

    /// <summary>
    /// A <see cref="TimeProvider"/> that signals a <see cref="TaskCompletionSource"/>
    /// the first time <see cref="CreateTimer"/> is invoked, so a test can wait for a
    /// <c>Task.Delay</c> to start before cancelling it.
    /// </summary>
    private sealed class TimerStartSignalingProvider(TaskCompletionSource timerStarted) : TimeProvider
    {
        private readonly TimeProvider _inner = System;

        public override long GetTimestamp() => _inner.GetTimestamp();
        public override long TimestampFrequency => _inner.TimestampFrequency;
        public override DateTimeOffset GetUtcNow() => _inner.GetUtcNow();

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            timerStarted.TrySetResult();
            return _inner.CreateTimer(callback, state, dueTime, period);
        }
    }

    [Test]
    public void Ctor_null_maintainer_throws() =>
        Assert.That(
            () => new TenantResidencyWarmupHostedService(
                null!, TimeProvider.System, NullLogger<TenantResidencyWarmupHostedService>.Instance),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_time_provider_throws()
    {
        var maintainer = Maintainer(Substitute.For<ITenantRegistry>());

        Assert.That(
            () => new TenantResidencyWarmupHostedService(
                maintainer, null!, NullLogger<TenantResidencyWarmupHostedService>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Ctor_null_logger_throws()
    {
        var maintainer = Maintainer(Substitute.For<ITenantRegistry>());

        Assert.That(
            () => new TenantResidencyWarmupHostedService(maintainer, TimeProvider.System, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task StartAsync_warms_the_maintainer_once()
    {
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => Empty());
        var maintainer = Maintainer(registry);
        var service = Service(maintainer);

        await service.StartAsync(CancellationToken.None);
        // Await the fire-and-forget warm loop to completion deterministically.
        await service.StopAsync(CancellationToken.None);

        Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1));
    }

    [Test]
    public async Task Warmup_property_is_populated_after_StartAsync()
    {
        // Covers line 52: the internal Warmup getter must return a non-null Task after
        // StartAsync so tests (and diagnostics) can observe the warm-up state.
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ => Empty());
        var maintainer = Maintainer(registry);
        var service = Service(maintainer);

        Assert.That(service.Warmup, Is.Null, "no warm-up before start");

        await service.StartAsync(CancellationToken.None);
        Assert.That(service.Warmup, Is.Not.Null, "the Warmup task is set after start");

        await service.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task WarmLoopAsync_catches_a_cancellation_from_a_stalled_warm_when_stopping()
    {
        // Covers lines 92-94: the maintainer's EnsureWarmAsync is stalled inside the
        // registry scan; StopAsync cancels _stopping, the OCE propagates, and the
        // WarmLoopAsync catch-when block returns cleanly without crashing.
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>())
            .Returns(info => StallUntilCancelled(entered, info.Arg<CancellationToken>()));
        var maintainer = Maintainer(registry);
        var service = Service(maintainer);

        await service.StartAsync(CancellationToken.None);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // StopAsync cancels _stopping; the warm-loop's EnsureWarmAsync propagates an
        // OperationCanceledException, which is caught by the when-filter at lines 92-94.
        Assert.That(
            async () => await service.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10)),
            Throws.Nothing,
            "StopAsync must complete cleanly even though the warmup was stalled");
    }

    [Test]
    public async Task WarmLoopAsync_catches_a_non_cancellation_exception_and_retries()
    {
        // Covers lines 96-101: the first EnsureWarmAsync call throws a non-OCE;
        // the loop catches it, logs, and retries after a short delay.
        // We await the Warmup task (not StopAsync) so we do not cancel the retry delay.
        var calls = 0;
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            var n = Interlocked.Increment(ref calls);
            return n == 1
                ? ThrowAsync(new InvalidOperationException("registry-offline"))
                : Empty();
        });
        var maintainer = Maintainer(registry);
        var service = Service(maintainer);

        await service.StartAsync(CancellationToken.None);

        // Wait for the Warmup task to complete on its own (the retry succeeds on
        // the second call after the 250 ms back-off delay in WarmLoopAsync).
        await service.Warmup!.WaitAsync(TimeSpan.FromSeconds(10));

        await service.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.GreaterThanOrEqualTo(2),
                "the warm loop must have retried after the first exception");
            Assert.That(maintainer.CurrentEpoch, Is.EqualTo(1),
                "the snapshot must be warm after the retry succeeded");
        });
    }

    [Test]
    public async Task WarmLoopAsync_exits_cleanly_when_cancelled_during_retry_delay()
    {
        // Covers lines 105-109: after catching a non-OCE the loop calls Task.Delay for
        // the retry back-off; if _stopping fires while the delay is running the
        // OperationCanceledException is caught and the loop returns cleanly.
        var timerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var timeProvider = new TimerStartSignalingProvider(timerStarted);

        // Registry always throws so the loop never warms up and always retries.
        var registry = Substitute.For<ITenantRegistry>();
        registry.ListAsync(Arg.Any<CancellationToken>())
            .Returns(_ => ThrowAsync(new InvalidOperationException("always-offline")));

        var maintainer = Maintainer(registry);
        var service = Service(maintainer, timeProvider);

        await service.StartAsync(CancellationToken.None);
        // Wait until the retry Task.Delay has started (CreateTimer was called).
        await timerStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Cancel the delay mid-flight; lines 105-109 catch the OCE and return.
        Assert.That(
            async () => await service.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10)),
            Throws.Nothing,
            "StopAsync must complete cleanly when the retry delay is interrupted");
    }

    [Test]
    public async Task StopAsync_before_start_is_a_no_op()
    {
        var maintainer = Maintainer(Substitute.For<ITenantRegistry>());
        var service = Service(maintainer);

        Assert.That(async () => await service.StopAsync(CancellationToken.None), Throws.Nothing);
        await Task.CompletedTask;
    }
}
