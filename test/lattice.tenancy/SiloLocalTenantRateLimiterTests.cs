using static Orleans.Lattice.Tenancy.Tests.RateLimiterTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Unit tests for <see cref="SiloLocalTenantRateLimiter"/>.</summary>
public sealed class SiloLocalTenantRateLimiterTests
{
    private static SiloLocalTenantRateLimiter CreateLimiter(out ManualTimeProvider clock)
    {
        clock = new ManualTimeProvider();
        return new SiloLocalTenantRateLimiter(clock);
    }

    [Test]
    public void Constructor_rejects_a_null_time_provider()
    {
        Assert.That(() => new SiloLocalTenantRateLimiter(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void TryAcquire_admits_the_uninitialised_no_tenant_value()
    {
        var limiter = CreateLimiter(out _);

        Assert.That(limiter.TryAcquire(default), Is.True);
    }

    [Test]
    public void TryAcquire_admits_a_tenant_with_no_configured_bucket()
    {
        var limiter = CreateLimiter(out _);

        Assert.That(limiter.TryAcquire(TenantId.Parse("acme")), Is.True);
        Assert.That(limiter.BucketCount, Is.EqualTo(0));
    }

    [Test]
    public void TryAcquire_throttles_once_a_bucket_is_configured_and_exhausted()
    {
        var limiter = CreateLimiter(out _);
        var tenant = TenantId.Parse("acme");
        var emission = Frequency / 5; // 5 ops/sec, no burst

        limiter.Configure(tenant, emission, burstToleranceTicks: 0);

        Assert.That(limiter.TryAcquire(tenant), Is.True, "first op admitted");
        Assert.That(limiter.TryAcquire(tenant), Is.False, "second op throttled");
    }

    [Test]
    public void TryAcquire_admits_again_after_logical_time_advances()
    {
        var limiter = CreateLimiter(out var clock);
        var tenant = TenantId.Parse("acme");
        var emission = Frequency / 5;

        limiter.Configure(tenant, emission, burstToleranceTicks: 0);
        Assert.That(limiter.TryAcquire(tenant), Is.True);
        Assert.That(limiter.TryAcquire(tenant), Is.False);

        clock.Advance(emission);
        Assert.That(limiter.TryAcquire(tenant), Is.True);
    }

    [Test]
    public void Configure_ignores_the_uninitialised_tenant()
    {
        var limiter = CreateLimiter(out _);

        limiter.Configure(default, 100, 0);

        Assert.That(limiter.BucketCount, Is.EqualTo(0));
    }

    [Test]
    public void Configure_creates_one_bucket_per_tenant()
    {
        var limiter = CreateLimiter(out _);

        limiter.Configure(TenantId.Parse("acme"), 100, 0);
        limiter.Configure(TenantId.Parse("globex"), 200, 0);

        Assert.That(limiter.BucketCount, Is.EqualTo(2));
    }

    [Test]
    public void Configure_preserves_bucket_state_when_the_parameters_are_unchanged()
    {
        var limiter = CreateLimiter(out _);
        var tenant = TenantId.Parse("acme");
        var emission = Frequency / 5;

        limiter.Configure(tenant, emission, burstToleranceTicks: 0);
        Assert.That(limiter.TryAcquire(tenant), Is.True); // consume the token

        // Re-configuring with identical parameters must not reset the bucket.
        limiter.Configure(tenant, emission, burstToleranceTicks: 0);
        Assert.That(limiter.TryAcquire(tenant), Is.False, "state preserved: still throttled");
    }

    [Test]
    public void Configure_replaces_the_bucket_when_the_parameters_change()
    {
        var limiter = CreateLimiter(out _);
        var tenant = TenantId.Parse("acme");

        limiter.Configure(tenant, Frequency / 5, burstToleranceTicks: 0);
        Assert.That(limiter.TryAcquire(tenant), Is.True);
        Assert.That(limiter.TryAcquire(tenant), Is.False);

        // A different rate installs a fresh bucket with a full allowance again.
        limiter.Configure(tenant, Frequency / 10, burstToleranceTicks: 0);
        Assert.That(limiter.TryAcquire(tenant), Is.True);
    }

    [Test]
    public void ReadAndResetDemand_returns_zero_for_an_unknown_or_default_tenant()
    {
        var limiter = CreateLimiter(out _);

        Assert.That(limiter.ReadAndResetDemand(default), Is.EqualTo(0));
        Assert.That(limiter.ReadAndResetDemand(TenantId.Parse("acme")), Is.EqualTo(0));
    }

    [Test]
    public void ReadAndResetDemand_reads_and_resets_a_configured_bucket()
    {
        var limiter = CreateLimiter(out _);
        var tenant = TenantId.Parse("acme");

        limiter.Configure(tenant, Frequency / 100, 20 * (Frequency / 100));
        limiter.TryAcquire(tenant);
        limiter.TryAcquire(tenant);

        Assert.That(limiter.ReadAndResetDemand(tenant), Is.EqualTo(2));
        Assert.That(limiter.ReadAndResetDemand(tenant), Is.EqualTo(0));
    }

    [Test]
    public void RetainOnly_rejects_a_null_set()
    {
        var limiter = CreateLimiter(out _);

        Assert.That(() => limiter.RetainOnly(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void RetainOnly_removes_buckets_not_in_the_configured_set()
    {
        var limiter = CreateLimiter(out _);
        limiter.Configure(TenantId.Parse("acme"), 100, 0);
        limiter.Configure(TenantId.Parse("globex"), 100, 0);
        limiter.Configure(TenantId.Parse("initech"), 100, 0);

        limiter.RetainOnly(new HashSet<string>(StringComparer.Ordinal) { "acme", "initech" });

        Assert.Multiple(() =>
        {
            Assert.That(limiter.BucketCount, Is.EqualTo(2));
            Assert.That(limiter.TryAcquire(TenantId.Parse("globex")), Is.True, "pruned tenant is inert again");
        });
    }

    [Test]
    public void RetainOnly_with_an_empty_set_removes_every_bucket()
    {
        var limiter = CreateLimiter(out _);
        limiter.Configure(TenantId.Parse("acme"), 100, 0);
        limiter.Configure(TenantId.Parse("globex"), 100, 0);

        limiter.RetainOnly(new HashSet<string>(StringComparer.Ordinal));

        Assert.That(limiter.BucketCount, Is.EqualTo(0));
    }

    [Test]
    public void RetainOnly_keeps_every_bucket_when_all_are_configured()
    {
        var limiter = CreateLimiter(out _);
        limiter.Configure(TenantId.Parse("acme"), 100, 0);
        limiter.Configure(TenantId.Parse("globex"), 100, 0);

        limiter.RetainOnly(new HashSet<string>(StringComparer.Ordinal) { "acme", "globex" });

        Assert.That(limiter.BucketCount, Is.EqualTo(2));
    }
}
