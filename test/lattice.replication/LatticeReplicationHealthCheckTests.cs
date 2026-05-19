using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class LatticeReplicationHealthCheckTests
{
    private const string DefaultName = LatticeReplicationHealthCheckOptions.DefaultName;

    [Test]
    public void Constructor_throws_on_null_peer_stats()
    {
        var options = BuildOptionsMonitor(new LatticeReplicationHealthCheckOptions());

        Assert.That(
            () => new LatticeReplicationHealthCheck(
                null!,
                options,
                NullLogger<LatticeReplicationHealthCheck>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_options_monitor()
    {
        Assert.That(
            () => new LatticeReplicationHealthCheck(
                new ReplicationPeerStats(),
                null!,
                NullLogger<LatticeReplicationHealthCheck>.Instance),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_throws_on_null_logger()
    {
        var options = BuildOptionsMonitor(new LatticeReplicationHealthCheckOptions());

        Assert.That(
            () => new LatticeReplicationHealthCheck(
                new ReplicationPeerStats(),
                options,
                null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void CheckHealthAsync_throws_on_null_context()
    {
        var check = CreateCheck(new ReplicationPeerStats(), new LatticeReplicationHealthCheckOptions());

        Assert.That(
            async () => await check.CheckHealthAsync(null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task CheckHealthAsync_returns_healthy_for_empty_snapshot()
    {
        var stats = new ReplicationPeerStats();
        var check = CreateCheck(stats, new LatticeReplicationHealthCheckOptions());

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
            Assert.That(result.Data["peers"], Is.EqualTo(0));
            Assert.That(result.Data.ContainsKey("degradedPeers"), Is.False);
            Assert.That(result.Data.ContainsKey("unhealthyPeers"), Is.False);
        });
    }

    [Test]
    public async Task CheckHealthAsync_returns_healthy_when_every_signal_below_degraded_bound()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordBacklog("tree", "peer", entriesBehind: 50, bytesBehind: 0);
        stats.RecordSuccess("tree", "peer");

        var check = CreateCheck(stats, new LatticeReplicationHealthCheckOptions());

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
    }

    [Test]
    public async Task CheckHealthAsync_returns_degraded_when_entries_behind_exceeds_soft_bound()
    {
        var stats = new ReplicationPeerStats();
        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = new LatticeReplicationHealthCheckOptions.LongTier(10, 100),
            LastContactSeconds = null,
            ConsecutiveErrors = null,
        };
        stats.RecordBacklog("tree", "peer", entriesBehind: 50, bytesBehind: 0);

        var check = CreateCheck(stats, options);

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Degraded));
            Assert.That(result.Data["degraded"], Is.EqualTo(1));
            Assert.That(result.Data["unhealthy"], Is.EqualTo(0));
            Assert.That((string[])result.Data["degradedPeers"], Has.Member("tree/peer"));
        });
    }

    [Test]
    public async Task CheckHealthAsync_returns_unhealthy_when_entries_behind_exceeds_hard_bound()
    {
        var stats = new ReplicationPeerStats();
        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = new LatticeReplicationHealthCheckOptions.LongTier(10, 100),
            LastContactSeconds = null,
            ConsecutiveErrors = null,
        };
        stats.RecordBacklog("tree", "peer", entriesBehind: 500, bytesBehind: 0);

        var check = CreateCheck(stats, options);

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
            Assert.That(result.Data["unhealthy"], Is.EqualTo(1));
            Assert.That((string[])result.Data["unhealthyPeers"], Has.Member("tree/peer"));
        });
    }

    [Test]
    public async Task CheckHealthAsync_returns_degraded_when_consecutive_errors_exceeds_soft_bound()
    {
        var stats = new ReplicationPeerStats();
        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = null,
            LastContactSeconds = null,
            ConsecutiveErrors = new LatticeReplicationHealthCheckOptions.LongTier(2, 10),
        };
        for (var i = 0; i < 5; i++)
        {
            stats.RecordError("tree", "peer");
        }

        var check = CreateCheck(stats, options);

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Degraded));
    }

    [Test]
    public async Task CheckHealthAsync_returns_unhealthy_when_consecutive_errors_exceeds_hard_bound()
    {
        var stats = new ReplicationPeerStats();
        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = null,
            LastContactSeconds = null,
            ConsecutiveErrors = new LatticeReplicationHealthCheckOptions.LongTier(2, 10),
        };
        for (var i = 0; i < 20; i++)
        {
            stats.RecordError("tree", "peer");
        }

        var check = CreateCheck(stats, options);

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
    }

    [Test]
    public async Task CheckHealthAsync_returns_degraded_when_last_contact_seconds_exceeds_soft_bound()
    {
        // The contact-age signal flows from RecordSuccess: a peer last
        // contacted at t=0 and probed at t=5 with a soft bound of 1 s
        // and hard bound of 60 s sits in the degraded tier.
        var clock = new FakeClock { Now = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero) };
        var stats = new TestableStats(clock);
        stats.RecordSuccess("tree", "peer");
        clock.Now = clock.Now.AddSeconds(5);

        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = null,
            LastContactSeconds = new LatticeReplicationHealthCheckOptions.DoubleTier(1d, 60d),
            ConsecutiveErrors = null,
        };

        var check = CreateCheck(stats, options);
        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Degraded));
    }

    [Test]
    public async Task CheckHealthAsync_returns_unhealthy_when_last_contact_seconds_exceeds_hard_bound()
    {
        var clock = new FakeClock { Now = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero) };
        var stats = new TestableStats(clock);
        stats.RecordSuccess("tree", "peer");
        clock.Now = clock.Now.AddSeconds(120);

        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = null,
            LastContactSeconds = new LatticeReplicationHealthCheckOptions.DoubleTier(1d, 60d),
            ConsecutiveErrors = null,
        };

        var check = CreateCheck(stats, options);
        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
            Assert.That(result.Data["unhealthy"], Is.EqualTo(1));
        });
    }

    [Test]
    public async Task CheckHealthAsync_excludes_NaN_last_contact_samples_from_classification()
    {
        // RecordError without RecordSuccess leaves LastContactSeconds NaN.
        var stats = new ReplicationPeerStats();
        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = null,
            LastContactSeconds = new LatticeReplicationHealthCheckOptions.DoubleTier(1d, 10d),
            ConsecutiveErrors = null,
        };
        stats.RecordError("tree", "peer");

        var check = CreateCheck(stats, options);

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.That(result.Status, Is.EqualTo(HealthStatus.Healthy));
    }

    [Test]
    public async Task CheckHealthAsync_escalates_sustained_degraded_to_unhealthy_after_grace_window()
    {
        var clock = new FakeClock { Now = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero) };
        var stats = new TestableStats(clock);
        stats.RecordBacklog("tree", "peer", entriesBehind: 50, bytesBehind: 0);
        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = new LatticeReplicationHealthCheckOptions.LongTier(10, 100_000),
            LastContactSeconds = null,
            ConsecutiveErrors = null,
            UnhealthyAfter = TimeSpan.FromSeconds(30),
        };
        var time = new FakeTimeProvider { UtcNow = clock.Now };
        var check = CreateCheck(stats, options, time);

        var first = await check.CheckHealthAsync(Context(), CancellationToken.None);
        Assert.That(first.Status, Is.EqualTo(HealthStatus.Degraded), "first probe = degraded, grace window starts");

        time.UtcNow = clock.Now.AddSeconds(15);
        var midway = await check.CheckHealthAsync(Context(), CancellationToken.None);
        Assert.That(midway.Status, Is.EqualTo(HealthStatus.Degraded), "halfway through window = still degraded");

        time.UtcNow = clock.Now.AddSeconds(45);
        var escalated = await check.CheckHealthAsync(Context(), CancellationToken.None);
        Assert.That(escalated.Status, Is.EqualTo(HealthStatus.Unhealthy), "past grace window = escalated");
    }

    [Test]
    public async Task CheckHealthAsync_resets_grace_window_when_peer_recovers()
    {
        var clock = new FakeClock { Now = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero) };
        var stats = new TestableStats(clock);
        stats.RecordBacklog("tree", "peer", entriesBehind: 50, bytesBehind: 0);
        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = new LatticeReplicationHealthCheckOptions.LongTier(10, 100_000),
            LastContactSeconds = null,
            ConsecutiveErrors = null,
            UnhealthyAfter = TimeSpan.FromSeconds(30),
        };
        var time = new FakeTimeProvider { UtcNow = clock.Now };
        var check = CreateCheck(stats, options, time);

        var first = await check.CheckHealthAsync(Context(), CancellationToken.None);
        Assert.That(first.Status, Is.EqualTo(HealthStatus.Degraded));

        // Recover: backlog falls back to zero. The grace window must clear.
        stats.RecordBacklog("tree", "peer", entriesBehind: 0, bytesBehind: 0);
        time.UtcNow = clock.Now.AddSeconds(20);
        var healthy = await check.CheckHealthAsync(Context(), CancellationToken.None);
        Assert.That(healthy.Status, Is.EqualTo(HealthStatus.Healthy));

        // Re-degrade after a long gap. The grace window must NOT count the
        // earlier excursion - if it did the next probe would already be
        // unhealthy.
        stats.RecordBacklog("tree", "peer", entriesBehind: 50, bytesBehind: 0);
        time.UtcNow = clock.Now.AddSeconds(60);
        var reDegraded = await check.CheckHealthAsync(Context(), CancellationToken.None);
        Assert.That(reDegraded.Status, Is.EqualTo(HealthStatus.Degraded));
    }

    [Test]
    public async Task CheckHealthAsync_with_zero_grace_window_escalates_immediately()
    {
        var stats = new ReplicationPeerStats();
        stats.RecordBacklog("tree", "peer", entriesBehind: 50, bytesBehind: 0);
        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = new LatticeReplicationHealthCheckOptions.LongTier(10, 100_000),
            LastContactSeconds = null,
            ConsecutiveErrors = null,
            UnhealthyAfter = TimeSpan.Zero,
        };

        var check = CreateCheck(stats, options);

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        // UnhealthyAfter == Zero disables the grace window so degraded never
        // promotes through the time gate (the only escalation path remaining
        // is the hard bound).
        Assert.That(result.Status, Is.EqualTo(HealthStatus.Degraded));
    }

    [Test]
    public async Task CheckHealthAsync_reports_worst_per_peer_classification()
    {
        var stats = new ReplicationPeerStats();
        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = new LatticeReplicationHealthCheckOptions.LongTier(10, 100),
            LastContactSeconds = null,
            ConsecutiveErrors = new LatticeReplicationHealthCheckOptions.LongTier(2, 10),
        };
        // Peer A: healthy.
        stats.RecordBacklog("tree", "peer-a", entriesBehind: 0, bytesBehind: 0);
        // Peer B: degraded by entries.
        stats.RecordBacklog("tree", "peer-b", entriesBehind: 50, bytesBehind: 0);
        // Peer C: unhealthy by hard error count.
        for (var i = 0; i < 50; i++)
        {
            stats.RecordError("tree", "peer-c");
        }

        var check = CreateCheck(stats, options);

        var result = await check.CheckHealthAsync(Context(), CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Status, Is.EqualTo(HealthStatus.Unhealthy));
            Assert.That(result.Data["peers"], Is.EqualTo(3));
            Assert.That(result.Data["degraded"], Is.EqualTo(1));
            Assert.That(result.Data["unhealthy"], Is.EqualTo(1));
        });
    }

    [Test]
    public async Task CheckHealthAsync_clears_degraded_since_when_peer_drops_from_telemetry()
    {
        // The peer is degraded on the first probe. We then advance the
        // clock past UnhealthyAfter while the peer no longer appears in
        // telemetry (the stats source is reset). The second probe must
        // observe an empty snapshot and report Healthy. The point of the
        // test is to confirm the implementation does not "remember"
        // stale degraded-since records for peers that have left the
        // topology - if it did, a subsequent re-appearance would
        // incorrectly inherit the prior grace window.
        var clock = new FakeClock { Now = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero) };
        var stats = new TestableStats(clock);
        stats.RecordBacklog("tree", "peer", entriesBehind: 50, bytesBehind: 0);
        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = new LatticeReplicationHealthCheckOptions.LongTier(10, 100),
            LastContactSeconds = null,
            ConsecutiveErrors = null,
            UnhealthyAfter = TimeSpan.FromSeconds(30),
        };
        var time = new FakeTimeProvider { UtcNow = clock.Now };
        var check = CreateCheck(stats, options, time);

        var firstResult = await check.CheckHealthAsync(Context(), CancellationToken.None);
        Assert.That(firstResult.Status, Is.EqualTo(HealthStatus.Degraded));

        // Drop the recorded peer's backlog to zero so the peer falls back
        // under every soft bound on the next probe - this is the closest
        // analogue available to "peer disappeared from telemetry" without
        // mutating private state.
        stats.RecordBacklog("tree", "peer", entriesBehind: 0, bytesBehind: 0);
        time.UtcNow = clock.Now.AddMinutes(5);

        var secondResult = await check.CheckHealthAsync(Context(), CancellationToken.None);
        Assert.That(secondResult.Status, Is.EqualTo(HealthStatus.Healthy));

        // Re-degrade after a long gap. The grace window must start fresh.
        stats.RecordBacklog("tree", "peer", entriesBehind: 50, bytesBehind: 0);
        time.UtcNow = clock.Now.AddMinutes(10);
        var reDegraded = await check.CheckHealthAsync(Context(), CancellationToken.None);
        Assert.That(reDegraded.Status, Is.EqualTo(HealthStatus.Degraded));
    }

    [Test]
    public async Task CheckHealthAsync_hard_unhealthy_clears_prior_degraded_since_record()
    {
        // Documented side-effect: when a peer transitions Degraded -> hard
        // Unhealthy (i.e. exceeds the hard bound rather than ageing out
        // through UnhealthyAfter), its first-degraded-at record is
        // discarded so a subsequent recovery starts a fresh grace
        // window. Without this, a peer that recovers from a hard
        // outage and then drifts back into degraded would be immediately
        // re-escalated using a stale timestamp.
        var clock = new FakeClock { Now = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero) };
        var stats = new TestableStats(clock);
        stats.RecordBacklog("tree", "peer", entriesBehind: 50, bytesBehind: 0);
        var options = new LatticeReplicationHealthCheckOptions
        {
            EntriesBehind = new LatticeReplicationHealthCheckOptions.LongTier(10, 100),
            LastContactSeconds = null,
            ConsecutiveErrors = null,
            UnhealthyAfter = TimeSpan.FromSeconds(30),
        };
        var time = new FakeTimeProvider { UtcNow = clock.Now };
        var check = CreateCheck(stats, options, time);

        // T0: soft-degraded; grace window starts.
        var degraded = await check.CheckHealthAsync(Context(), CancellationToken.None);
        Assert.That(degraded.Status, Is.EqualTo(HealthStatus.Degraded));

        // T+5s: backlog jumps past the hard bound -> immediate Unhealthy.
        // This must drop the prior degraded-since record.
        stats.RecordBacklog("tree", "peer", entriesBehind: 500, bytesBehind: 0);
        time.UtcNow = clock.Now.AddSeconds(5);
        var hardUnhealthy = await check.CheckHealthAsync(Context(), CancellationToken.None);
        Assert.That(hardUnhealthy.Status, Is.EqualTo(HealthStatus.Unhealthy));

        // T+10s: full recovery.
        stats.RecordBacklog("tree", "peer", entriesBehind: 0, bytesBehind: 0);
        time.UtcNow = clock.Now.AddSeconds(10);
        var recovered = await check.CheckHealthAsync(Context(), CancellationToken.None);
        Assert.That(recovered.Status, Is.EqualTo(HealthStatus.Healthy));

        // T+20s: drift back to soft-degraded. If the hard-Unhealthy step
        // had failed to clear the original record (at T0), this probe
        // would already be Unhealthy because 20s > 30s grace window from
        // the discarded original timestamp. The expected behaviour is a
        // fresh Degraded with a fresh window.
        stats.RecordBacklog("tree", "peer", entriesBehind: 50, bytesBehind: 0);
        time.UtcNow = clock.Now.AddSeconds(20);
        var freshDegraded = await check.CheckHealthAsync(Context(), CancellationToken.None);
        Assert.That(freshDegraded.Status, Is.EqualTo(HealthStatus.Degraded));
    }

    [Test]
    public void Options_default_constants_have_sensible_relative_ordering()
    {
        // The Default* constants are part of the public API surface;
        // accidentally inverting one of the tiers (or zeroing a tier's
        // hard bound) would silently break every host that takes the
        // defaults without rebinding.
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeReplicationHealthCheckOptions.DefaultEntriesBehind.Degraded,
                Is.LessThan(LatticeReplicationHealthCheckOptions.DefaultEntriesBehind.Unhealthy));
            Assert.That(
                LatticeReplicationHealthCheckOptions.DefaultEntriesBehind.Degraded,
                Is.GreaterThanOrEqualTo(0L));

            Assert.That(
                LatticeReplicationHealthCheckOptions.DefaultLastContactSeconds.Degraded,
                Is.LessThan(LatticeReplicationHealthCheckOptions.DefaultLastContactSeconds.Unhealthy));
            Assert.That(
                LatticeReplicationHealthCheckOptions.DefaultLastContactSeconds.Degraded,
                Is.GreaterThanOrEqualTo(0d));

            Assert.That(
                LatticeReplicationHealthCheckOptions.DefaultConsecutiveErrors.Degraded,
                Is.LessThan(LatticeReplicationHealthCheckOptions.DefaultConsecutiveErrors.Unhealthy));
            Assert.That(
                LatticeReplicationHealthCheckOptions.DefaultConsecutiveErrors.Degraded,
                Is.GreaterThanOrEqualTo(0L));

            Assert.That(
                LatticeReplicationHealthCheckOptions.DefaultUnhealthyAfter,
                Is.GreaterThan(TimeSpan.Zero));

            Assert.That(
                LatticeReplicationHealthCheckOptions.DefaultName,
                Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public void Options_default_constructor_applies_default_constants()
    {
        var options = new LatticeReplicationHealthCheckOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.EntriesBehind, Is.EqualTo(LatticeReplicationHealthCheckOptions.DefaultEntriesBehind));
            Assert.That(options.LastContactSeconds, Is.EqualTo(LatticeReplicationHealthCheckOptions.DefaultLastContactSeconds));
            Assert.That(options.ConsecutiveErrors, Is.EqualTo(LatticeReplicationHealthCheckOptions.DefaultConsecutiveErrors));
            Assert.That(options.UnhealthyAfter, Is.EqualTo(LatticeReplicationHealthCheckOptions.DefaultUnhealthyAfter));
        });
    }

    [Test]
    public void AddLatticeReplicationHealthCheck_registers_check_under_default_name()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ReplicationPeerStats>();
        services.AddSingleton(BuildOptionsMonitor(new LatticeReplicationHealthCheckOptions()));
        services.AddLogging();
        services.AddHealthChecks().AddLatticeReplicationHealthCheck();

        var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value;

        Assert.That(
            registry.Registrations.Select(r => r.Name),
            Has.Member(LatticeReplicationHealthCheckOptions.DefaultName));
    }

    [Test]
    public void AddLatticeReplicationHealthCheck_honours_caller_supplied_name()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ReplicationPeerStats>();
        services.AddSingleton(BuildOptionsMonitor(new LatticeReplicationHealthCheckOptions()));
        services.AddLogging();
        services.AddHealthChecks().AddLatticeReplicationHealthCheck("custom-name", tags: new[] { "ready" });

        var provider = services.BuildServiceProvider();
        var registry = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value;

        var registration = registry.Registrations.Single(r => r.Name == "custom-name");
        Assert.That(registration.Tags, Has.Member("ready"));
    }

    [Test]
    public void AddLatticeReplicationHealthCheck_throws_on_null_builder()
    {
        Assert.That(
            () => LatticeReplicationServiceCollectionExtensions.AddLatticeReplicationHealthCheck(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddLatticeReplicationHealthCheck_registers_health_check_as_singleton()
    {
        // Regression guard: the default IHealthChecksBuilder.AddCheck<T>(...)
        // contract is to register T as transient, which would reset the
        // per-peer "first-degraded-at" map on every probe and silently
        // disable LatticeReplicationHealthCheckOptions.UnhealthyAfter
        // escalation. The extension must explicitly promote the check to
        // a singleton on the underlying ServiceCollection so the
        // HealthCheckService resolves the same instance across calls.
        var services = new ServiceCollection();
        services.AddSingleton<ReplicationPeerStats>();
        services.AddSingleton(BuildOptionsMonitor(new LatticeReplicationHealthCheckOptions()));
        services.AddLogging();
        services.AddHealthChecks().AddLatticeReplicationHealthCheck();

        var registration = services.Single(d => d.ServiceType == typeof(LatticeReplicationHealthCheck));

        Assert.That(registration.Lifetime, Is.EqualTo(ServiceLifetime.Singleton));

        // Cross-check the resolved instance identity across two scopes to
        // catch any future regression where the lifetime is correct but
        // the resolution path returns distinct instances.
        var provider = services.BuildServiceProvider();
        using var scopeA = provider.CreateScope();
        using var scopeB = provider.CreateScope();
        var fromA = scopeA.ServiceProvider.GetRequiredService<LatticeReplicationHealthCheck>();
        var fromB = scopeB.ServiceProvider.GetRequiredService<LatticeReplicationHealthCheck>();
        Assert.That(fromA, Is.SameAs(fromB));
    }

    private static LatticeReplicationHealthCheck CreateCheck(
        ReplicationPeerStats stats,
        LatticeReplicationHealthCheckOptions options,
        TimeProvider? time = null) =>
        new(
            stats,
            BuildOptionsMonitor(options),
            NullLogger<LatticeReplicationHealthCheck>.Instance,
            time);

    private static IOptionsMonitor<LatticeReplicationHealthCheckOptions> BuildOptionsMonitor(
        LatticeReplicationHealthCheckOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationHealthCheckOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static HealthCheckContext Context(string name = DefaultName) => new()
    {
        Registration = new HealthCheckRegistration(
            name,
            sp => new LatticeReplicationHealthCheck(
                new ReplicationPeerStats(),
                BuildOptionsMonitor(new LatticeReplicationHealthCheckOptions()),
                NullLogger<LatticeReplicationHealthCheck>.Instance),
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

    private sealed class FakeTimeProvider : TimeProvider
    {
        public DateTimeOffset UtcNow { get; set; }
        public override DateTimeOffset GetUtcNow() => UtcNow;
    }
}
