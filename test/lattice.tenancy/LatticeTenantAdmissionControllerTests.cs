using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantAdmissionController"/>: the real
/// <see cref="ITenantAdmissionController"/> the tenancy package contributes. Covers
/// the constructor guards, that it is always active, the fail-open behaviour for a
/// cold or unknown tenant, and that it admits or refuses against the aggregate the
/// enforcement scope selects - the global fold under GlobalConverged, the local
/// slot under PerCluster - throwing <see cref="LatticeQuotaExceededException"/>
/// carrying the tenant id on a breach.
/// </summary>
[TestFixture]
public sealed class LatticeTenantAdmissionControllerTests
{
    private const string Tree = "orders";
    private static readonly TenantId Acme = TenantId.Parse("acme");

    private static LatticeTenantAdmissionController Create(
        FakeTenantUsageIndex index,
        TenantEnforcementScope scope,
        ITenantRateLimiter? rateLimiter = null) =>
        new(index, new FixedScopeResolver(scope), rateLimiter ?? new AdmitAllRateLimiter());

    /// <summary>
    /// A rate limiter that admits every operation, so a quota test exercises the
    /// footprint dimensions without the rate budget interfering. The rate-limit
    /// path has its own dedicated tests.
    /// </summary>
    private sealed class AdmitAllRateLimiter : ITenantRateLimiter
    {
        public bool TryAcquire(TenantId tenant) => true;
    }

    /// <summary>A rate limiter that refuses every operation, pinning the rate-budget branch.</summary>
    private sealed class RefuseAllRateLimiter : ITenantRateLimiter
    {
        public bool TryAcquire(TenantId tenant) => false;
    }

    private static FakeTenantUsageIndex IndexWith(TenantQuotas quotas, LocalUsageSample global, LocalUsageSample local)
    {
        var index = new FakeTenantUsageIndex();
        index.Views["acme"] = new TenantUsageView(quotas, global, local);
        return index;
    }

    [Test]
    public void Constructor_null_index_throws()
    {
        Assert.That(
            () => new LatticeTenantAdmissionController(
                null!, new FixedScopeResolver(TenantEnforcementScope.GlobalConverged), new AdmitAllRateLimiter()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_scope_resolver_throws()
    {
        Assert.That(
            () => new LatticeTenantAdmissionController(new FakeTenantUsageIndex(), null!, new AdmitAllRateLimiter()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_rate_limiter_throws()
    {
        Assert.That(
            () => new LatticeTenantAdmissionController(
                new FakeTenantUsageIndex(),
                new FixedScopeResolver(TenantEnforcementScope.GlobalConverged),
                null!),
            Throws.ArgumentNullException);
    }

    // ---- Rate budget (issue #1688) --------------------------------------
    //
    // MaxOpsPerSecond was inert: ITenantRateLimiter lives in the tenancy package
    // so the core write seam could not reach it, and nothing else consulted it.
    // Folding it into the admission controller - which core already reaches
    // through the ITenantAdmissionController null seam, short-circuited on
    // IsActive - enforces the dimension without core taking a new dependency.

    [Test]
    public void A_refused_rate_budget_throws_the_ops_per_second_dimension()
    {
        var controller = Create(
            new FakeTenantUsageIndex(), TenantEnforcementScope.GlobalConverged, new RefuseAllRateLimiter());

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(
            async () => await controller.IsAdmittedAsync(Acme, Tree));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Dimension, Is.EqualTo(LatticeQuotaExceededException.OpsPerSecondDimension));
            Assert.That(ex.TenantId, Is.EqualTo("acme"));
            Assert.That(ex.TreeId, Is.EqualTo(Tree));
            Assert.That(ex.Message, Does.Contain("retry"), "a rate breach is a transient back-off signal");
        });
    }

    [Test]
    public void The_rate_budget_is_applied_before_the_footprint_quotas()
    {
        // An unknown tenant would otherwise fail open on the footprint branch, so a
        // refusal here proves the rate budget is consulted first.
        var controller = Create(
            new FakeTenantUsageIndex(), TenantEnforcementScope.GlobalConverged, new RefuseAllRateLimiter());

        Assert.ThrowsAsync<LatticeQuotaExceededException>(
            async () => await controller.IsAdmittedAsync(Acme, Tree));
    }

    [Test]
    public async Task An_admitted_rate_budget_falls_through_to_the_footprint_quotas()
    {
        var index = IndexWith(new TenantQuotas { MaxBytes = 1_000 }, global: Sample(bytes: 100), local: Sample(bytes: 100));
        var controller = Create(index, TenantEnforcementScope.GlobalConverged);

        Assert.That(await controller.IsAdmittedAsync(Acme, Tree), Is.True);
    }

    [Test]
    public void IsActive_is_true()
    {
        Assert.That(Create(new FakeTenantUsageIndex(), TenantEnforcementScope.GlobalConverged).IsActive, Is.True);
    }

    [Test]
    public void IsAdmittedAsync_null_tree_throws()
    {
        var controller = Create(new FakeTenantUsageIndex(), TenantEnforcementScope.GlobalConverged);

        Assert.That(async () => await controller.IsAdmittedAsync(Acme, null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task IsAdmittedAsync_admits_a_tenant_with_no_view_failing_open()
    {
        var controller = Create(new FakeTenantUsageIndex(), TenantEnforcementScope.GlobalConverged);

        Assert.That(await controller.IsAdmittedAsync(Acme, Tree), Is.True);
    }

    [Test]
    public async Task GlobalConverged_admits_when_the_global_fold_is_within_quota()
    {
        var index = IndexWith(new TenantQuotas { MaxBytes = 1_000 }, global: Sample(bytes: 900), local: Sample(bytes: 900));
        var controller = Create(index, TenantEnforcementScope.GlobalConverged);

        Assert.That(await controller.IsAdmittedAsync(Acme, Tree), Is.True);
    }

    [Test]
    public void GlobalConverged_refuses_when_the_global_fold_exceeds_quota()
    {
        // The local slot alone is within quota, but the cross-cluster fold is over:
        // GlobalConverged admits against the fold, so this refuses.
        var index = IndexWith(new TenantQuotas { MaxBytes = 1_000 }, global: Sample(bytes: 1_500), local: Sample(bytes: 400));
        var controller = Create(index, TenantEnforcementScope.GlobalConverged);

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(
            async () => await controller.IsAdmittedAsync(Acme, Tree));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TenantId, Is.EqualTo("acme"));
            Assert.That(ex.Dimension, Is.EqualTo(LatticeQuotaExceededException.BytesDimension));
            Assert.That(ex.Current, Is.EqualTo(1_500), "the global fold is the admitted quantity");
        });
    }

    [Test]
    public async Task PerCluster_admits_when_the_local_slot_is_within_quota_even_though_the_global_fold_is_over()
    {
        // The global fold is over quota, but PerCluster admits against the local
        // slot only, which is within quota - so this admits.
        var index = IndexWith(new TenantQuotas { MaxBytes = 1_000 }, global: Sample(bytes: 5_000), local: Sample(bytes: 400));
        var controller = Create(index, TenantEnforcementScope.PerCluster);

        Assert.That(await controller.IsAdmittedAsync(Acme, Tree), Is.True);
    }

    [Test]
    public void PerCluster_refuses_when_the_local_slot_exceeds_quota()
    {
        var index = IndexWith(new TenantQuotas { MaxBytes = 1_000 }, global: Sample(bytes: 400), local: Sample(bytes: 1_500));
        var controller = Create(index, TenantEnforcementScope.PerCluster);

        var ex = Assert.ThrowsAsync<LatticeQuotaExceededException>(
            async () => await controller.IsAdmittedAsync(Acme, Tree));

        Assert.That(ex!.Current, Is.EqualTo(1_500), "the local slot is the admitted quantity under PerCluster");
    }

    [Test]
    public async Task An_unbounded_tenant_is_admitted_regardless_of_usage()
    {
        var index = IndexWith(TenantQuotas.Unbounded, global: Sample(long.MaxValue, long.MaxValue, long.MaxValue, long.MaxValue), local: LocalUsageSample.Empty);
        var controller = Create(index, TenantEnforcementScope.GlobalConverged);

        Assert.That(await controller.IsAdmittedAsync(Acme, Tree), Is.True);
    }
}
