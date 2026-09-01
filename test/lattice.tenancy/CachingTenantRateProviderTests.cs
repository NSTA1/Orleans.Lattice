using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using static Orleans.Lattice.Tenancy.Tests.RateLimiterTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="CachingTenantRateProvider"/>, the time-to-live cache
/// that decouples the frequent, purely local budget re-apportionment from the
/// expensive whole-tree scan of the durable tenant registry.
/// </summary>
[TestFixture]
public sealed class CachingTenantRateProviderTests
{
    private static IOptionsMonitor<LatticeTenantRateLimiterOptions> Options(TimeSpan ttl)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeTenantRateLimiterOptions>>();
        monitor.CurrentValue.Returns(new LatticeTenantRateLimiterOptions { RateSnapshotTtl = ttl });
        return monitor;
    }

    private static CachingTenantRateProvider Create(
        ITenantRateProvider inner, TimeProvider clock, TimeSpan ttl)
        => new(inner, clock, Options(ttl), NullLogger<CachingTenantRateProvider>.Instance);

    private static async Task<List<TenantRateSpec>> DrainAsync(ITenantRateProvider provider)
    {
        var specs = new List<TenantRateSpec>();
        await foreach (var spec in provider.GetConfiguredRatesAsync())
        {
            specs.Add(spec);
        }

        return specs;
    }

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        var inner = new CountingRateProvider();
        var clock = new ManualTimeProvider();
        var options = Options(TimeSpan.FromMinutes(2));
        var logger = NullLogger<CachingTenantRateProvider>.Instance;

        Assert.Multiple(() =>
        {
            Assert.That(() => new CachingTenantRateProvider(null!, clock, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new CachingTenantRateProvider(inner, null!, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new CachingTenantRateProvider(inner, clock, null!, logger), Throws.ArgumentNullException);
            Assert.That(() => new CachingTenantRateProvider(inner, clock, options, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task A_second_read_inside_the_ttl_does_not_rescan_the_registry()
    {
        // The whole point: the lease cycle used to run a 64-shard whole-tree scan on
        // every tick, so a scan slower than the tick ran back to back at a 100% duty
        // cycle and the registry never drained.
        var inner = new CountingRateProvider(Spec("acme", 1000));
        var clock = new ManualTimeProvider();
        var provider = Create(inner, clock, TimeSpan.FromMinutes(2));

        var first = await DrainAsync(provider);
        var second = await DrainAsync(provider);

        Assert.Multiple(() =>
        {
            Assert.That(inner.Scans, Is.EqualTo(1));
            Assert.That(first.Select(s => s.Tenant), Is.EqualTo(second.Select(s => s.Tenant)));
        });
    }

    [Test]
    public async Task A_read_past_the_ttl_refreshes_the_snapshot()
    {
        var inner = new CountingRateProvider(Spec("acme", 1000));
        var clock = new ManualTimeProvider();
        var provider = Create(inner, clock, TimeSpan.FromSeconds(30));

        _ = await DrainAsync(provider);
        clock.AdvanceSeconds(31);
        _ = await DrainAsync(provider);

        Assert.That(inner.Scans, Is.EqualTo(2));
    }

    [Test]
    public async Task A_refreshed_snapshot_reflects_a_configuration_change()
    {
        var inner = new CountingRateProvider(Spec("acme", 1000));
        var clock = new ManualTimeProvider();
        var provider = Create(inner, clock, TimeSpan.FromSeconds(30));

        _ = await DrainAsync(provider);
        inner.Specs = [Spec("acme", 2000), Spec("globex", 500)];
        clock.AdvanceSeconds(31);

        var refreshed = await DrainAsync(provider);

        Assert.That(refreshed.Select(s => s.OpsPerSecond), Is.EqualTo(new[] { 2000L, 500L }));
    }

    [Test]
    public async Task A_refresh_failure_serves_the_previous_snapshot()
    {
        // Propagating instead would let one slow registry read prune every tenant's
        // bucket through the coordinator's retain-only step - a worse failure than
        // briefly apportioning from slightly stale rates.
        var inner = new CountingRateProvider(Spec("acme", 1000));
        var clock = new ManualTimeProvider();
        var provider = Create(inner, clock, TimeSpan.FromSeconds(30));

        _ = await DrainAsync(provider);
        inner.Fault = new TimeoutException("registry scan stalled");
        clock.AdvanceSeconds(31);

        var stale = await DrainAsync(provider);

        Assert.Multiple(() =>
        {
            Assert.That(stale.Select(s => s.OpsPerSecond), Is.EqualTo(new[] { 1000L }));
            Assert.That(inner.Scans, Is.EqualTo(2), "the refresh was attempted");
        });
    }

    [Test]
    public async Task A_served_stale_snapshot_is_re_attempted_on_the_next_read()
    {
        var inner = new CountingRateProvider(Spec("acme", 1000));
        var clock = new ManualTimeProvider();
        var provider = Create(inner, clock, TimeSpan.FromSeconds(30));

        _ = await DrainAsync(provider);
        inner.Fault = new TimeoutException("registry scan stalled");
        clock.AdvanceSeconds(31);
        _ = await DrainAsync(provider);

        inner.Fault = null;
        inner.Specs = [Spec("acme", 4000)];
        var recovered = await DrainAsync(provider);

        Assert.That(recovered.Select(s => s.OpsPerSecond), Is.EqualTo(new[] { 4000L }));
    }

    [Test]
    public void A_first_read_failure_propagates_so_the_loop_backs_off()
    {
        var inner = new CountingRateProvider { Fault = new TimeoutException("registry scan stalled") };
        var provider = Create(inner, new ManualTimeProvider(), TimeSpan.FromMinutes(2));

        Assert.That(async () => await DrainAsync(provider), Throws.TypeOf<TimeoutException>());
    }

    [Test]
    public async Task A_non_positive_ttl_falls_back_to_the_default()
    {
        var inner = new CountingRateProvider(Spec("acme", 1000));
        var clock = new ManualTimeProvider();
        var provider = Create(inner, clock, TimeSpan.Zero);

        _ = await DrainAsync(provider);
        clock.AdvanceSeconds((long)LatticeTenantRateLimiterOptions.DefaultRateSnapshotTtl.TotalSeconds - 1);
        _ = await DrainAsync(provider);
        Assert.That(inner.Scans, Is.EqualTo(1), "still inside the default TTL");

        clock.AdvanceSeconds(2);
        _ = await DrainAsync(provider);
        Assert.That(inner.Scans, Is.EqualTo(2), "past the default TTL");
    }

    [Test]
    public async Task An_empty_registry_is_cached_like_any_other_snapshot()
    {
        var inner = new CountingRateProvider();
        var provider = Create(inner, new ManualTimeProvider(), TimeSpan.FromMinutes(2));

        var first = await DrainAsync(provider);
        var second = await DrainAsync(provider);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.Empty);
            Assert.That(second, Is.Empty);
            Assert.That(inner.Scans, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_concurrent_caller_that_arrives_while_a_refresh_is_in_flight_is_served_from_the_same_refresh()
    {
        // Covers the "already fresh" guard inside the semaphore: the second waiter
        // enters the gate AFTER the first caller has finished the scan and updated
        // the snapshot, finds it fresh, and returns early without scanning again.
        var scanStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var scanGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var inner = new GatedRateProvider(scanStarted, scanGate, Spec("acme", 100));
        var clock = new ManualTimeProvider();
        var provider = Create(inner, clock, TimeSpan.FromSeconds(60));

        // Prime the cache so that both concurrent callers below will all see a
        // stale snapshot (first scan happens synchronously here).
        _ = await DrainAsync(provider);
        clock.AdvanceSeconds(61);

        // Start the first refresh; it will stall at the scan gate.
        var first = DrainAsync(provider);
        await scanStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Second caller arrives while the first scan is still in progress.
        var second = DrainAsync(provider);

        // Let the in-flight scan complete.
        scanGate.SetResult();

        var r1 = await first.WaitAsync(TimeSpan.FromSeconds(10));
        var r2 = await second.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Multiple(() =>
        {
            Assert.That(inner.Scans, Is.EqualTo(2), "exactly one real scan for the concurrent pair");
            Assert.That(r1.Select(s => s.Tenant), Is.EquivalentTo(r2.Select(s => s.Tenant)));
        });
    }

    private static TenantRateSpec Spec(string tenant, long opsPerSecond)
        => new(TenantId.Parse(tenant), opsPerSecond, BurstPercent: 0);

    /// <summary>
    /// An inner provider that counts how many times it was actually enumerated, so a
    /// cache hit is observable, and that can be made to fault mid-scan.
    /// </summary>
    private sealed class CountingRateProvider(params TenantRateSpec[] specs) : ITenantRateProvider
    {
        /// <summary>The specs served by the next scan.</summary>
        public TenantRateSpec[] Specs { get; set; } = specs;

        /// <summary>When set, the next scan throws it instead of yielding.</summary>
        public Exception? Fault { get; set; }

        /// <summary>The number of times the provider was enumerated.</summary>
        public int Scans { get; private set; }

#pragma warning disable CS1998 // synchronous fake enumerator
        public async IAsyncEnumerable<TenantRateSpec> GetConfiguredRatesAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            Scans++;
            if (Fault is { } fault)
            {
                throw fault;
            }

            foreach (var spec in Specs)
            {
                yield return spec;
            }
        }
#pragma warning restore CS1998
    }

    /// <summary>
    /// A provider that signals when the SECOND scan starts and then waits on a gate
    /// task before yielding results, enabling deterministic concurrent-caller tests.
    /// The first scan (priming the cache) completes immediately.
    /// </summary>
    private sealed class GatedRateProvider(
        TaskCompletionSource started,
        TaskCompletionSource gate,
        params TenantRateSpec[] specs) : ITenantRateProvider
    {
        /// <summary>Total number of scans started.</summary>
        public int Scans { get; private set; }

        public async IAsyncEnumerable<TenantRateSpec> GetConfiguredRatesAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var n = ++Scans;
            if (n >= 2)
            {
                // Only gate the second and later scans; the prime scan returns immediately.
                started.TrySetResult();
                await gate.Task.ConfigureAwait(false);
            }

            foreach (var spec in specs)
            {
                yield return spec;
            }
        }
    }
}
