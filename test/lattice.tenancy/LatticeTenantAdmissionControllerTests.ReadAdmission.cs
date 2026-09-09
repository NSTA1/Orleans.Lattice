using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantAdmissionController.IsReadAdmitted"/>:
/// the read-side half of tenant admission.
/// </summary>
/// <remarks>
/// <para>
/// The read path applies the sustained request-rate ceiling
/// (<c>MaxOpsPerSecond</c>) and deliberately nothing else. Before it existed the
/// limiter was consulted only from the write-mutation sites, so a dimension whose
/// own quota documents a "cluster-wide ops/sec ceiling" silently governed writes
/// alone and a tenant could issue unbounded reads - whole-keyspace counts and
/// scans included - with no budget and no fairness against its neighbours.
/// </para>
/// <para>
/// The deliberate omission is as load-bearing as the enforcement, so both halves
/// are pinned here. The footprint dimensions bound stored volume, which a read
/// does not increase; refusing reads on a storage breach would trap an over-quota
/// tenant, because it could no longer read its own data back in order to delete
/// it and get under the cap. Storage pressure is answered by refusing writes.
/// </para>
/// </remarks>
public sealed partial class LatticeTenantAdmissionControllerTests
{
    /// <summary>
    /// A rate limiter that admits a fixed number of acquisitions and refuses every
    /// one after that, so a test can observe the exact operation the budget is
    /// spent on. It also records every tenant it was asked about, which is what
    /// proves the read path consults the limiter at all rather than short-circuiting.
    /// </summary>
    private sealed class BudgetedRateLimiter(int budget) : ITenantRateLimiter
    {
        private int _remaining = budget;

        public List<TenantId> Asked { get; } = [];

        public bool TryAcquire(TenantId tenant)
        {
            Asked.Add(tenant);
            if (_remaining <= 0)
            {
                return false;
            }

            _remaining--;
            return true;
        }
    }

    [Test]
    public void IsReadAdmitted_null_tree_id_throws()
    {
        var controller = Create(new FakeTenantUsageIndex(), TenantEnforcementScope.GlobalConverged);

        Assert.That(() => controller.IsReadAdmitted(Acme, null!), Throws.ArgumentNullException);
    }

    [Test]
    public void IsReadAdmitted_admits_when_the_rate_budget_has_capacity()
    {
        var controller = Create(new FakeTenantUsageIndex(), TenantEnforcementScope.GlobalConverged);

        Assert.That(controller.IsReadAdmitted(Acme, Tree), Is.True);
    }

    /// <summary>
    /// The headline regression for the read path: a refused rate budget throws the
    /// ops/sec dimension rather than returning false, so the write-admission seam's
    /// refusal path stays reserved for a genuine access denial.
    /// </summary>
    [Test]
    public void IsReadAdmitted_refused_rate_budget_throws_the_ops_per_second_dimension()
    {
        var controller = Create(
            new FakeTenantUsageIndex(), TenantEnforcementScope.GlobalConverged, new RefuseAllRateLimiter());

        var ex = Assert.Throws<LatticeQuotaExceededException>(() => controller.IsReadAdmitted(Acme, Tree));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Dimension, Is.EqualTo(LatticeQuotaExceededException.OpsPerSecondDimension));
            Assert.That(ex.TenantId, Is.EqualTo("acme"));
            Assert.That(ex.TreeId, Is.EqualTo(Tree));
            Assert.That(ex.Current, Is.EqualTo(0), "a rate breach reports no instantaneous quantity");
            Assert.That(ex.Limit, Is.EqualTo(0));
            Assert.That(
                ex.Message,
                Does.Contain("reading from tree"),
                "the message must name the read path, so an operator can tell a throttled read from a throttled write");
            Assert.That(ex.Message, Does.Contain("retry"), "a rate breach is a transient back-off signal");
        });
    }

    /// <summary>
    /// The uninitialised "no tenant" value carries a null <c>Value</c>; the refusal
    /// must still construct rather than throwing a <see cref="NullReferenceException"/>
    /// while building the exception that reports the breach.
    /// </summary>
    [Test]
    public void IsReadAdmitted_refusal_for_the_default_tenant_reports_an_empty_tenant_id()
    {
        var controller = Create(
            new FakeTenantUsageIndex(), TenantEnforcementScope.GlobalConverged, new RefuseAllRateLimiter());

        var ex = Assert.Throws<LatticeQuotaExceededException>(() => controller.IsReadAdmitted(default, Tree));

        Assert.That(ex!.TenantId, Is.EqualTo(string.Empty));
    }

    /// <summary>
    /// The read path spends exactly one token per call, so a budget of one admits
    /// the first read and refuses the second. This is what makes the ceiling a
    /// genuine sustained-rate budget over reads rather than a one-off check.
    /// </summary>
    [Test]
    public void IsReadAdmitted_charges_one_token_per_read()
    {
        var limiter = new BudgetedRateLimiter(budget: 1);
        var controller = Create(new FakeTenantUsageIndex(), TenantEnforcementScope.GlobalConverged, limiter);

        Assert.Multiple(() =>
        {
            Assert.That(controller.IsReadAdmitted(Acme, Tree), Is.True, "the first read is within budget");
            Assert.That(
                () => controller.IsReadAdmitted(Acme, Tree),
                Throws.TypeOf<LatticeQuotaExceededException>(),
                "the second read has exhausted the budget");
        });

        Assert.That(limiter.Asked, Has.Count.EqualTo(2), "every read consults the limiter");
        Assert.That(limiter.Asked, Is.All.EqualTo(Acme), "the calling tenant is the one charged");
    }

    /// <summary>
    /// The deliberate omission, pinned. A tenant whose stored footprint is far over
    /// every bounded dimension is still admitted to read: refusing here would trap
    /// it, because it could no longer read its own data back in order to delete it
    /// and get under the cap.
    /// </summary>
    [Test]
    public void IsReadAdmitted_admits_a_tenant_whose_footprint_is_over_quota()
    {
        var index = IndexWith(
            new TenantQuotas { MaxBytes = 1_000 },
            global: Sample(bytes: 5_000_000),
            local: Sample(bytes: 5_000_000));
        var controller = Create(index, TenantEnforcementScope.GlobalConverged);

        Assert.Multiple(() =>
        {
            Assert.That(
                controller.IsReadAdmitted(Acme, Tree),
                Is.True,
                "storage pressure is answered by refusing writes, not by trapping the tenant out of its own data");

            // The control: the same tenant, same index, same controller - the write
            // path does refuse. Without this the test above would also pass if
            // footprint admission were broken outright.
            Assert.That(
                async () => await controller.IsAdmittedAsync(Acme, Tree),
                Throws.TypeOf<LatticeQuotaExceededException>(),
                "the write path must still refuse the same over-quota tenant");
        });
    }

    /// <summary>
    /// A cold tenant with no compiled view is admitted to read, matching the
    /// fail-open behaviour of the write path.
    /// </summary>
    [Test]
    public void IsReadAdmitted_admits_a_tenant_with_no_view()
    {
        var controller = Create(new FakeTenantUsageIndex(), TenantEnforcementScope.GlobalConverged);

        Assert.That(controller.IsReadAdmitted(TenantId.Parse("never-seen"), Tree), Is.True);
    }

    /// <summary>
    /// The read budget is shared with the write budget - both spend the same
    /// limiter - so reads and writes contend for one ceiling rather than each
    /// getting a private one.
    /// </summary>
    [Test]
    public void IsReadAdmitted_shares_the_rate_budget_with_the_write_path()
    {
        var limiter = new BudgetedRateLimiter(budget: 1);
        var controller = Create(new FakeTenantUsageIndex(), TenantEnforcementScope.GlobalConverged, limiter);

        Assert.That(controller.IsReadAdmitted(Acme, Tree), Is.True, "the read spends the only token");

        Assert.That(
            async () => await controller.IsAdmittedAsync(Acme, Tree),
            Throws.TypeOf<LatticeQuotaExceededException>(),
            "the write then finds the shared budget exhausted");
    }
}
