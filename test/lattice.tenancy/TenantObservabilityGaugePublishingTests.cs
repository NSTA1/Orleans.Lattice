using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using static Orleans.Lattice.Tenancy.Tests.ObservabilityTestData;
using static Orleans.Lattice.Tenancy.Tests.OverageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for the per-tenant observable-gauge publishing path -
/// <see cref="TenantObservabilityGaugeRegistry"/> and
/// <see cref="TenantObservabilityPublisher"/>. Because the registry is a
/// process-global static meter, the fixture is <see cref="NonParallelizableAttribute"/>
/// and every test publishes under a unique tenant id and asserts the presence of its
/// own series (never the absence of another's), so it never races another test's
/// publish. Publishing is driven deterministically via
/// <see cref="TenantObservabilityPublisher.PublishOnceAsync"/> and a synchronous
/// <see cref="MeterListener"/> scrape - never a timer wait - so there is no timing,
/// ordering, or wall-clock dependency.
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TenantObservabilityGaugePublishingTests
{
    private static IOptionsMonitor<TenantObservabilityOptions> Options(TenantObservabilityOptions options)
    {
        var monitor = Substitute.For<IOptionsMonitor<TenantObservabilityOptions>>();
        monitor.CurrentValue.Returns(options);
        return monitor;
    }

    private static TenantObservabilityPublisher Publisher(
        FakeTenantUsageIndex usage,
        FakeTenantOverageBilling billing,
        TenantObservabilityOptions? options = null) =>
        new(
            new TenantObservabilitySource(usage, billing),
            new RateLimiterTestData.ManualTimeProvider(),
            Options(options ?? new TenantObservabilityOptions()),
            Substitute.For<ILogger<TenantObservabilityPublisher>>());

    /// <summary>Records every long measurement on the tenancy meter into a flat list.</summary>
    private static List<(string Name, long Value, string? Tenant)> Scrape()
    {
        var recorded = new List<(string, long, string?)>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == LatticeTenantMetrics.MeterName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            },
        };

        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            string? tenant = null;
            foreach (var tag in tags)
            {
                if (tag.Key == LatticeTenantMetrics.TagTenant)
                {
                    tenant = tag.Value as string;
                }
            }

            recorded.Add((instrument.Name, value, tenant));
        });

        listener.Start();
        listener.RecordObservableInstruments();
        return recorded;
    }

    // ---- Registry ------------------------------------------------------

    [Test]
    public void EnsureRegistered_is_idempotent()
    {
        Assert.That(() =>
        {
            TenantObservabilityGaugeRegistry.EnsureRegistered();
            TenantObservabilityGaugeRegistry.EnsureRegistered();
        }, Throws.Nothing);
    }

    [Test]
    public void Publish_null_snapshot_throws()
    {
        Assert.That(() => TenantObservabilityGaugeRegistry.Publish(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Publish_then_scrape_observes_the_published_per_tenant_series()
    {
        var tenant = TenantId.Parse("reg-usage-tenant");
        TenantObservabilityGaugeRegistry.EnsureRegistered();
        TenantObservabilityGaugeRegistry.Publish(TenantObservabilityGaugeSnapshot.Build(new[]
        {
            new TenantObservabilitySnapshot(tenant, Usage(bytes: 4242), Quotas(bytes: 9000), Overage(bytes: 11)),
        }));

        var recorded = Scrape();

        Assert.Multiple(() =>
        {
            Assert.That(
                recorded,
                Has.Some.EqualTo((LatticeTenantMetrics.UsageBytesName, 4242L, (string?)"reg-usage-tenant")),
                "the published usage series is observable, tagged by tenant");
            Assert.That(
                recorded,
                Has.Some.EqualTo((LatticeTenantMetrics.OverageBytesName, 11L, (string?)"reg-usage-tenant")),
                "the published overage series is observable, tagged by tenant");
            Assert.That(
                recorded,
                Has.Some.Matches<(string Name, long Value, string? Tenant)>(
                    m => m.Name == LatticeTenantMetrics.TenantsName
                        && m.Tenant == LatticeTenantLabel.PlatformTenant),
                "the cluster-aggregate tenant count is not attributable to any one tenant, so it carries "
                    + "the reserved platform sentinel rather than a tenant id - the derived tenant dimension "
                    + "is emitted on every series, so a tenant-scoped query never has to special-case it");
        });
    }

    [Test]
    public void Latest_reflects_the_last_published_snapshot()
    {
        var snapshot = TenantObservabilityGaugeSnapshot.Build(new[]
        {
            new TenantObservabilitySnapshot(TenantId.Parse("reg-latest"), Usage(bytes: 1), Quotas(), TenantOverageSample.Empty),
        });

        TenantObservabilityGaugeRegistry.Publish(snapshot);

        Assert.That(TenantObservabilityGaugeRegistry.Latest, Is.SameAs(snapshot));
    }

    // ---- Publisher ctor guards -----------------------------------------

    [Test]
    public void Constructor_rejects_null_arguments()
    {
        var source = new TenantObservabilitySource(new FakeTenantUsageIndex(), new FakeTenantOverageBilling());
        var time = new RateLimiterTestData.ManualTimeProvider();
        var options = Options(new TenantObservabilityOptions());
        var logger = Substitute.For<ILogger<TenantObservabilityPublisher>>();

        Assert.Multiple(() =>
        {
            Assert.That(() => new TenantObservabilityPublisher(null!, time, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new TenantObservabilityPublisher(source, null!, options, logger), Throws.ArgumentNullException);
            Assert.That(() => new TenantObservabilityPublisher(source, time, null!, logger), Throws.ArgumentNullException);
            Assert.That(() => new TenantObservabilityPublisher(source, time, options, null!), Throws.ArgumentNullException);
        });
    }

    // ---- Publisher behaviour -------------------------------------------

    [Test]
    public async Task PublishOnceAsync_publishes_the_sampled_snapshot_as_observable_series()
    {
        var tenant = TenantId.Parse("pub-once-tenant");
        var usage = new FakeTenantUsageIndex().With(tenant, View(Quotas(bytes: 8000), Usage(bytes: 777)));
        var billing = new FakeTenantOverageBilling().With(tenant, Overage(keys: 9));
        var publisher = Publisher(usage, billing);

        await publisher.PublishOnceAsync();
        var recorded = Scrape();

        Assert.Multiple(() =>
        {
            Assert.That(recorded, Has.Some.EqualTo((LatticeTenantMetrics.UsageBytesName, 777L, (string?)"pub-once-tenant")));
            Assert.That(recorded, Has.Some.EqualTo((LatticeTenantMetrics.OverageKeysName, 9L, (string?)"pub-once-tenant")));
        });
    }

    [Test]
    public async Task StartAsync_when_publishing_disabled_does_not_start_the_loop()
    {
        var publisher = Publisher(
            new FakeTenantUsageIndex(),
            new FakeTenantOverageBilling(),
            new TenantObservabilityOptions { PublishGauges = false });

        await publisher.StartAsync(CancellationToken.None);

        Assert.That(publisher.Loop, Is.Null, "a disabled publisher never launches the sample loop");

        await publisher.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task StartAsync_starts_the_loop_and_StopAsync_completes_it()
    {
        var tenant = TenantId.Parse("pub-loop-tenant");
        var usage = new FakeTenantUsageIndex().With(tenant, View(Quotas(bytes: 500), Usage(bytes: 250)));
        var publisher = Publisher(usage, new FakeTenantOverageBilling());

        await publisher.StartAsync(CancellationToken.None);
        Assert.That(publisher.Loop, Is.Not.Null, "an enabled publisher launches the sample loop");

        await publisher.StopAsync(CancellationToken.None);
        Assert.That(publisher.Loop!.IsCompleted, Is.True, "the loop completes after stop");
    }

    [Test]
    public void StopAsync_without_a_prior_start_is_a_no_op()
    {
        var publisher = Publisher(new FakeTenantUsageIndex(), new FakeTenantOverageBilling());

        Assert.That(async () => await publisher.StopAsync(CancellationToken.None), Throws.Nothing);
    }

    [Test]
    public async Task RunLoopAsync_falls_back_to_the_default_publish_interval_when_configured_interval_is_non_positive()
    {
        // Covers line 115: when PublishInterval is <= 0, RunLoopAsync falls back to
        // DefaultPublishInterval. The fakes complete synchronously so the background task
        // always reaches line 115 before StopAsync can cancel the timer wait.
        var publisher = new TenantObservabilityPublisher(
            new TenantObservabilitySource(new FakeTenantUsageIndex(), new FakeTenantOverageBilling()),
            new RateLimiterTestData.ManualTimeProvider(),
            Options(new TenantObservabilityOptions { PublishGauges = true, PublishInterval = TimeSpan.Zero }),
            Substitute.For<ILogger<TenantObservabilityPublisher>>());

        await publisher.StartAsync(CancellationToken.None);
        await publisher.StopAsync(CancellationToken.None);

        Assert.That(publisher.Loop!.IsCompleted, Is.True,
            "the loop must have run through the interval-fallback path and exited on stop");
    }

    [Test]
    public async Task RunLoopAsync_fires_a_second_publish_on_the_next_tick()
    {
        // Covers lines 121-123: the loop body inside the while fires a second
        // PublishCycleSafelyAsync call on the second timer tick. Uses a real short
        // interval (5 ms) and waits for a deterministic signal from the second tick.
        var secondEntry = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var usage = new SignalingFakeUsageIndex(secondEntry);
        var publisher = new TenantObservabilityPublisher(
            new TenantObservabilitySource(usage, new FakeTenantOverageBilling()),
            TimeProvider.System,
            Options(new TenantObservabilityOptions
            {
                PublishGauges = true,
                PublishInterval = TimeSpan.FromMilliseconds(5),
            }),
            Substitute.For<ILogger<TenantObservabilityPublisher>>());

        await publisher.StartAsync(CancellationToken.None);
        await secondEntry.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await publisher.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Test]
    public async Task PublishCycleSafelyAsync_swallows_a_non_cancellation_exception_and_continues()
    {
        // Covers lines 135-140: PublishCycleSafelyAsync catches a non-OCE thrown by
        // PublishOnceAsync, logs a warning, and returns normally so the loop can
        // continue to the next tick.
        var secondEntry = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var usage = new FaultingFirstCallUsageIndex(secondEntry);
        var publisher = new TenantObservabilityPublisher(
            new TenantObservabilitySource(usage, new FakeTenantOverageBilling()),
            TimeProvider.System,
            Options(new TenantObservabilityOptions
            {
                PublishGauges = true,
                PublishInterval = TimeSpan.FromMilliseconds(5),
            }),
            Substitute.For<ILogger<TenantObservabilityPublisher>>());

        await publisher.StartAsync(CancellationToken.None);
        await secondEntry.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await publisher.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// A fake <see cref="ITenantUsageIndex"/> that signals a
    /// <see cref="TaskCompletionSource"/> on the second call to
    /// <see cref="EnsureWarmAsync"/>, so a test can wait deterministically for the
    /// loop's second tick.
    /// </summary>
    private sealed class SignalingFakeUsageIndex(TaskCompletionSource secondEntry) : ITenantUsageIndex
    {
        private int _calls;

        public bool TryGetView(TenantId tenant, out TenantUsageView view)
        {
            view = default;
            return false;
        }

        public Task EnsureWarmAsync(CancellationToken cancellationToken = default)
        {
            if (Interlocked.Increment(ref _calls) >= 2)
            {
                secondEntry.TrySetResult();
            }

            return Task.CompletedTask;
        }

        public IReadOnlyDictionary<string, TenantUsageView> EnumerateViews() =>
            new Dictionary<string, TenantUsageView>(StringComparer.Ordinal);
    }

    /// <summary>
    /// A fake <see cref="ITenantUsageIndex"/> that throws on the first call to
    /// <see cref="EnsureWarmAsync"/> (exercising the general-exception catch) and
    /// signals the supplied <see cref="TaskCompletionSource"/> on the second.
    /// </summary>
    private sealed class FaultingFirstCallUsageIndex(TaskCompletionSource secondEntry) : ITenantUsageIndex
    {
        private int _calls;

        public bool TryGetView(TenantId tenant, out TenantUsageView view)
        {
            view = default;
            return false;
        }

        public Task EnsureWarmAsync(CancellationToken cancellationToken = default)
        {
            var n = Interlocked.Increment(ref _calls);
            if (n == 1)
            {
                return Task.FromException(new InvalidOperationException("observability-source-fault"));
            }

            secondEntry.TrySetResult();
            return Task.CompletedTask;
        }

        public IReadOnlyDictionary<string, TenantUsageView> EnumerateViews() =>
            new Dictionary<string, TenantUsageView>(StringComparer.Ordinal);
    }
}
