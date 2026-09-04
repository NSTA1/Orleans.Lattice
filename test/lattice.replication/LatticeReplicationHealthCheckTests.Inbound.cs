using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage for the bidirectional inbound tier on
/// <see cref="LatticeReplicationHealthCheck"/>.
/// </summary>
[TestFixture]
public class LatticeReplicationHealthCheckInboundTests
{
    private const string Name = LatticeReplicationHealthCheckOptions.DefaultName;

    [Test]
    public async Task Inbound_silence_under_threshold_does_not_degrade_health()
    {
        var clock = new FakeClock { Now = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero) };
        var stats = new TestableStats(clock);
        stats.RecordInboundSuccess("t", "p");
        clock.Now = clock.Now.AddSeconds(5);

        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = null,
            LastContactSeconds = null,
            ConsecutiveErrors = null,
            InboundDegradedAfter = TimeSpan.FromSeconds(10),
            InboundCriticalAfter = TimeSpan.FromSeconds(60),
        };

        var check = Create(stats, options);
        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
    }

    [Test]
    public async Task Inbound_silence_past_degraded_threshold_marks_degraded()
    {
        var clock = new FakeClock { Now = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero) };
        var stats = new TestableStats(clock);
        stats.RecordInboundSuccess("t", "p");
        clock.Now = clock.Now.AddSeconds(15);

        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = null,
            LastContactSeconds = null,
            ConsecutiveErrors = null,
            UnhealthyAfter = TimeSpan.Zero,  // disable sustained-degraded escalation
            InboundDegradedAfter = TimeSpan.FromSeconds(10),
            InboundCriticalAfter = TimeSpan.FromSeconds(60),
        };

        var check = Create(stats, options);
        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Degraded));
        // UnhealthyAfter=Zero disables the sustained-degraded escalation (the check
        // only escalates when UnhealthyAfter > Zero), so the inbound-silence signal
        // stands on its own verdict here.
    }

    [Test]
    public async Task Inbound_silence_past_critical_threshold_marks_unhealthy()
    {
        var clock = new FakeClock { Now = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero) };
        var stats = new TestableStats(clock);
        stats.RecordInboundSuccess("t", "p");
        clock.Now = clock.Now.AddSeconds(120);

        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = null,
            LastContactSeconds = null,
            ConsecutiveErrors = null,
            InboundDegradedAfter = TimeSpan.FromSeconds(10),
            InboundCriticalAfter = TimeSpan.FromSeconds(60),
        };

        var check = Create(stats, options);
        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
    }

    [Test]
    public async Task Inbound_signal_disabled_by_default_infinite_threshold()
    {
        var clock = new FakeClock { Now = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero) };
        var stats = new TestableStats(clock);
        stats.RecordInboundSuccess("t", "p");
        clock.Now = clock.Now.AddDays(1);  // arbitrarily large silence

        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = null,
            LastContactSeconds = null,
            ConsecutiveErrors = null,
        };

        var check = Create(stats, options);
        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy),
            "InboundDegradedAfter / InboundCriticalAfter default to InfiniteTimeSpan, disabling the signal");
    }

    private static LatticeReplicationHealthCheck Create(
        ReplicationPeerStats stats, LatticeReplicationHealthCheckOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationHealthCheckOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return new LatticeReplicationHealthCheck(
            stats, monitor, NullLogger<LatticeReplicationHealthCheck>.Instance);
    }

    private static HealthCheckContext Context() => new()
    {
        Registration = new HealthCheckRegistration(
            Name,
            _ => throw new NotSupportedException(),
            failureStatus: null,
            tags: null),
    };

    private sealed class FakeClock
    {
        public DateTimeOffset Now;
    }

    private sealed class TestableStats(FakeClock clock) : ReplicationPeerStats
    {
        protected override DateTimeOffset GetTimestamp() => clock.Now;
    }
}
