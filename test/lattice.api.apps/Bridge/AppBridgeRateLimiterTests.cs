using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps.Tests.Bridge;

/// <summary>
/// The per-<c>(caller, tenant, app)</c> fixed-window rate limit, both on its own and as the bridge applies it.
/// Time is advanced by hand, so no test depends on the clock.
/// </summary>
[TestFixture]
public sealed class AppBridgeRateLimiterTests
{
    private static readonly AppSlug Crm = AppSlug.Parse("crm");
    private static readonly AppSlug Billing = AppSlug.Parse("billing");

    private static AppBridgeRateLimiter Limiter(BridgeHarness.ManualTime time, int permits = 2) =>
        new(new LatticeAppBridgeOptions { RateLimitPermitLimit = permits, RateLimitWindow = TimeSpan.FromSeconds(10) }, time);

    [Test]
    public void A_partition_is_refused_once_its_window_is_spent_and_admitted_again_in_the_next()
    {
        var time = new BridgeHarness.ManualTime();
        var limiter = Limiter(time);

        Assert.That(limiter.TryAcquire("alice", TenantId.Default, Crm), Is.True);
        Assert.That(limiter.TryAcquire("alice", TenantId.Default, Crm), Is.True);
        Assert.That(limiter.TryAcquire("alice", TenantId.Default, Crm), Is.False);

        time.Advance(TimeSpan.FromSeconds(9));
        Assert.That(limiter.TryAcquire("alice", TenantId.Default, Crm), Is.False);

        time.Advance(TimeSpan.FromSeconds(1));
        Assert.That(limiter.TryAcquire("alice", TenantId.Default, Crm), Is.True);
    }

    [Test]
    public void Partitions_are_independent_per_caller_tenant_and_app()
    {
        var time = new BridgeHarness.ManualTime();
        var limiter = Limiter(time, permits: 1);

        Assert.That(limiter.TryAcquire("alice", TenantId.Default, Crm), Is.True);
        Assert.That(limiter.TryAcquire("alice", TenantId.Default, Crm), Is.False);
        Assert.That(limiter.TryAcquire("bob", TenantId.Default, Crm), Is.True);
        Assert.That(limiter.TryAcquire("alice", TenantId.Parse("acme"), Crm), Is.True);
        Assert.That(limiter.TryAcquire("alice", TenantId.Default, Billing), Is.True);
        Assert.That(limiter.PartitionCount, Is.EqualTo(4));
    }

    [Test]
    public void A_full_table_prunes_expired_partitions_and_otherwise_fails_closed()
    {
        var time = new BridgeHarness.ManualTime();
        var limiter = Limiter(time);
        for (var i = 0; i < AppBridgeRateLimiter.MaxPartitions; i++)
        {
            Assert.That(limiter.TryAcquire($"s{i}", TenantId.Default, Crm), Is.True);
        }

        Assert.That(limiter.TryAcquire("newcomer", TenantId.Default, Crm), Is.False, "no expired partition to prune");
        Assert.That(limiter.TryAcquire("s0", TenantId.Default, Crm), Is.True, "a tracked partition is unaffected");

        time.Advance(TimeSpan.FromSeconds(10));
        Assert.That(limiter.TryAcquire("newcomer", TenantId.Default, Crm), Is.True, "expired partitions are pruned");
        Assert.That(limiter.PartitionCount, Is.EqualTo(1));
    }

    [TestCase(0)]
    [TestCase(-5)]
    public void A_permit_limit_below_one_is_rejected(int permits) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new AppBridgeRateLimiter(new LatticeAppBridgeOptions { RateLimitPermitLimit = permits }));

    [TestCase(0)]
    [TestCase(-1)]
    public void A_non_positive_window_is_rejected(int seconds) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new AppBridgeRateLimiter(new LatticeAppBridgeOptions { RateLimitWindow = TimeSpan.FromSeconds(seconds) }));

    [Test]
    public void Null_options_are_rejected() =>
        Assert.Throws<ArgumentNullException>(() => new AppBridgeRateLimiter(null!));

    [Test]
    public void The_system_clock_is_used_when_no_time_source_is_supplied() =>
        Assert.That(new AppBridgeRateLimiter(new LatticeAppBridgeOptions()).TryAcquire("alice", TenantId.Default, Crm), Is.True);

    [Test]
    public async Task The_bridge_refuses_a_rate_limited_caller_before_any_authorization_or_data_access()
    {
        var harness = new BridgeHarness().Installed("alice", BridgeHarness.Editors);
        harness.Options.RateLimitPermitLimit = 1;
        harness.Options.RateLimitWindow = TimeSpan.FromSeconds(1);
        var bridge = harness.Bridge;

        await bridge.SetAsync(BridgeHarness.Target(), "k", new byte[] { 1 });
        var dialled = harness.Dialled.Count;
        BridgeAssert.Fails(AppBridgeFailure.Unavailable, () => bridge.GetAsync(BridgeHarness.Target(), "k"));
        Assert.That(harness.Dialled, Has.Count.EqualTo(dialled));

        harness.Time.Advance(TimeSpan.FromSeconds(1));
        Assert.That(await bridge.GetAsync(BridgeHarness.Target(), "k"), Is.Not.Null);
    }

    [Test]
    public void The_bridge_rate_limits_a_caller_who_is_then_denied()
    {
        // Denied requests spend permits too, so probing cannot run faster than the limit.
        var harness = new BridgeHarness().Installed("mallory");
        harness.Options.RateLimitPermitLimit = 1;
        var bridge = harness.Bridge;

        BridgeAssert.Fails(AppBridgeFailure.Denied, () => bridge.GetAsync(BridgeHarness.Target(), "k"));
        BridgeAssert.Fails(AppBridgeFailure.Unavailable, () => bridge.GetAsync(BridgeHarness.Target(), "k"));
    }
}
