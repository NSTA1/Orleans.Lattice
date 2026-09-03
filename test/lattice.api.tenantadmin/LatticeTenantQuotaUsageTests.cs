using Orleans.Lattice;
using Orleans.Lattice.Tenancy;
using static Orleans.Lattice.Api.TenantAdmin.Tests.TenantAdminTestSupport;

namespace Orleans.Lattice.Api.TenantAdmin.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantQuotaUsage"/>, the read-only
/// usage-against-quota facade. Covers the full authorization matrix (platform
/// operator, tenant admin of the tenant, tenant admin of a <em>different</em>
/// tenant, anonymous), the fail-closed indistinguishability of a missing tenant
/// from an unauthorized one, the unbounded / bounded / capped-at-zero dimension
/// modelling, the enforcement-scope qualifier, and the input guards. Every usage
/// figure comes from a fixed hand-authored sample rather than a live sampler, so
/// nothing here depends on timing, ordering, or the wall clock.
/// </summary>
[TestFixture]
public sealed class LatticeTenantQuotaUsageTests
{
    private const string OperatorSubject = "platform-operator";
    private const string AcmeAdminSubject = "acme-admin";

    private static readonly TenantId Acme = TenantId.Parse("acme");
    private static readonly TenantId Beta = TenantId.Parse("beta");

    private static HybridLogicalClock Stamp(long ticks) => new() { WallClockTicks = ticks };

    private static LocalUsageSample Usage(
        long bytes = 0, long keys = 0, long memoryBytes = 0, long treeCount = 0) =>
        new() { Bytes = bytes, Keys = keys, MemoryBytes = memoryBytes, TreeCount = treeCount };

    private static TenantOverageSample Overage(
        long bytes = 0, long keys = 0, long memoryBytes = 0, long treeCount = 0) =>
        new() { Bytes = bytes, Keys = keys, MemoryBytes = memoryBytes, TreeCount = treeCount };

    private static TenantRecord Record(
        TenantId tenant, TenantQuotas quotas, params string[] adminSubjects)
    {
        var record = TenantRecord.Create(
            tenant, TenantStatus.Active, quotas, TenantPlacement.Shared, Stamp(1), "seed");
        var stamp = 2L;
        foreach (var subject in adminSubjects)
        {
            record.AddAdminSubject(subject, Stamp(stamp++), "seed");
        }

        return record;
    }

    /// <summary>
    /// Builds the facade under a gate that admits exactly one platform-operator
    /// subject on the reserved policy tree, with <paramref name="callerSubject"/>
    /// as the resolved caller.
    /// </summary>
    private static LatticeTenantQuotaUsage Facade(
        FakeTenantRegistry registry,
        FakeTenantUsageReader usageReader,
        string? callerSubject)
    {
        var authorizer = new TenantRegionResidencyAuthorizer(
            new AdminSubjectGate(OperatorSubject),
            registry,
            callerSubject is null
                ? new FixedMembershipContext(LatticeSubject.Anonymous)
                : new FixedMembershipContext(new LatticeSubject(callerSubject)));
        return new LatticeTenantQuotaUsage(authorizer, usageReader);
    }

    // ---- ctor guards -----------------------------------------------------

    [Test]
    public void Ctor_null_authorizer_throws() =>
        Assert.That(
            () => new LatticeTenantQuotaUsage(null!, new FakeTenantUsageReader()),
            Throws.ArgumentNullException);

    [Test]
    public void Ctor_null_usage_reader_throws()
    {
        var authorizer = new TenantRegionResidencyAuthorizer(new FixedGate(true), new FakeTenantRegistry());

        Assert.That(
            () => new LatticeTenantQuotaUsage(authorizer, null!),
            Throws.ArgumentNullException);
    }

    // ---- input guards ----------------------------------------------------

    [Test]
    public void GetQuotaUsageAsync_null_tenant_id_throws()
    {
        var facade = Facade(new FakeTenantRegistry(), new FakeTenantUsageReader(), OperatorSubject);

        Assert.That(() => facade.GetQuotaUsageAsync(null!), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetQuotaUsageAsync_empty_tenant_id_throws()
    {
        var facade = Facade(new FakeTenantRegistry(), new FakeTenantUsageReader(), OperatorSubject);

        Assert.That(() => facade.GetQuotaUsageAsync(string.Empty), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void GetQuotaUsageAsync_malformed_tenant_id_throws_before_touching_the_reader()
    {
        var reader = new FakeTenantUsageReader();
        var facade = Facade(new FakeTenantRegistry(), reader, OperatorSubject);

        Assert.Multiple(() =>
        {
            Assert.That(() => facade.GetQuotaUsageAsync("Not A Tenant!"), Throws.InstanceOf<ArgumentException>());
            Assert.That(reader.Reads, Is.Empty);
        });
    }

    // ---- authorization matrix -------------------------------------------

    [Test]
    public async Task GetQuotaUsageAsync_platform_operator_reads_any_tenant()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, TenantQuotas.Unbounded));
        var reader = new FakeTenantUsageReader().With(Acme, Usage(bytes: 42), TenantQuotas.Unbounded);

        var report = await Facade(registry, reader, OperatorSubject).GetQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.TenantId, Is.EqualTo("acme"));
            Assert.That(report.Bytes.Usage, Is.EqualTo(42));
        });
    }

    [Test]
    public async Task GetQuotaUsageAsync_tenant_admin_reads_its_own_tenant()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, TenantQuotas.Unbounded, AcmeAdminSubject));
        var reader = new FakeTenantUsageReader().With(Acme, Usage(keys: 9), TenantQuotas.Unbounded);

        var report = await Facade(registry, reader, AcmeAdminSubject).GetQuotaUsageAsync("acme");

        Assert.That(report.Keys.Usage, Is.EqualTo(9));
    }

    [Test]
    public void GetQuotaUsageAsync_tenant_admin_of_another_tenant_is_refused()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, TenantQuotas.Unbounded, AcmeAdminSubject));
        registry.Seed(Record(Beta, TenantQuotas.Unbounded, "beta-admin"));
        var reader = new FakeTenantUsageReader().With(Beta, Usage(bytes: 1), TenantQuotas.Unbounded);

        Assert.That(
            () => Facade(registry, reader, AcmeAdminSubject).GetQuotaUsageAsync("beta"),
            Throws.InstanceOf<TenantNotFoundException>());
    }

    [Test]
    public void GetQuotaUsageAsync_anonymous_caller_is_refused()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, TenantQuotas.Unbounded, AcmeAdminSubject));

        Assert.That(
            () => Facade(registry, new FakeTenantUsageReader(), callerSubject: null).GetQuotaUsageAsync("acme"),
            Throws.InstanceOf<TenantNotFoundException>());
    }

    [Test]
    public void GetQuotaUsageAsync_non_admin_subject_is_refused()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, TenantQuotas.Unbounded, AcmeAdminSubject));

        Assert.That(
            () => Facade(registry, new FakeTenantUsageReader(), "some-random-subject").GetQuotaUsageAsync("acme"),
            Throws.InstanceOf<TenantNotFoundException>());
    }

    [Test]
    public void GetQuotaUsageAsync_missing_tenant_is_not_found_for_an_operator()
    {
        var facade = Facade(new FakeTenantRegistry(), new FakeTenantUsageReader(), OperatorSubject);

        Assert.That(() => facade.GetQuotaUsageAsync("ghost"), Throws.InstanceOf<TenantNotFoundException>());
    }

    /// <summary>
    /// The load-bearing leak test: for a non-operator caller, an existing tenant it
    /// may not read and a tenant that does not exist at all must be reported
    /// identically, so the read can never be used as an existence oracle.
    /// </summary>
    [Test]
    public void GetQuotaUsageAsync_unauthorized_and_missing_tenants_are_indistinguishable()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, TenantQuotas.Unbounded, AcmeAdminSubject));
        registry.Seed(Record(Beta, TenantQuotas.Unbounded, "beta-admin"));
        var facade = Facade(registry, new FakeTenantUsageReader(), AcmeAdminSubject);

        var unauthorized = Assert.ThrowsAsync<TenantNotFoundException>(
            () => facade.GetQuotaUsageAsync("beta"));
        var missing = Assert.ThrowsAsync<TenantNotFoundException>(
            () => facade.GetQuotaUsageAsync("ghost"));

        Assert.Multiple(() =>
        {
            Assert.That(
                unauthorized!.GetType(),
                Is.EqualTo(missing!.GetType()),
                "an unauthorized tenant and an absent one must raise the same type");
            Assert.That(
                unauthorized.Message.Replace("beta", "ghost", StringComparison.Ordinal),
                Is.EqualTo(missing.Message),
                "the two messages must differ only in the tenant id the caller already supplied");
        });
    }

    [Test]
    public void GetQuotaUsageAsync_refusal_does_not_read_usage()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Beta, TenantQuotas.Unbounded, "beta-admin"));
        var reader = new FakeTenantUsageReader().With(Beta, Usage(bytes: 999), TenantQuotas.Unbounded);
        var facade = Facade(registry, reader, AcmeAdminSubject);

        Assert.That(() => facade.GetQuotaUsageAsync("beta"), Throws.InstanceOf<TenantNotFoundException>());
        Assert.That(reader.Reads, Is.Empty, "a refused read must never touch the usage index");
    }

    // ---- unbounded vs bounded vs capped-at-zero --------------------------

    [Test]
    public async Task GetQuotaUsageAsync_unbounded_dimension_reports_no_ceiling_not_a_zero_ceiling()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, TenantQuotas.Unbounded));
        var reader = new FakeTenantUsageReader().With(Acme, Usage(bytes: 4_100_000_000), TenantQuotas.Unbounded);

        var report = await Facade(registry, reader, OperatorSubject).GetQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.Bytes.Limit, Is.Null, "unbounded means no ceiling at all");
            Assert.That(report.Bytes.BurstLimit, Is.Null, "an unbounded dimension has no burst ceiling either");
            Assert.That(report.Bytes.IsBounded, Is.False);
            Assert.That(report.Bytes.Usage, Is.EqualTo(4_100_000_000));
            Assert.That(report.Bytes.Overage, Is.Zero, "an unbounded dimension can never be in overage");
            Assert.That(report.Quotas.IsUnbounded, Is.True);
        });
    }

    [Test]
    public async Task GetQuotaUsageAsync_zero_ceiling_is_bounded_and_in_overage()
    {
        var quotas = new TenantQuotas { MaxBytes = 0 };
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, quotas));
        var reader = new FakeTenantUsageReader().With(Acme, Usage(bytes: 5), quotas);

        var report = await Facade(registry, reader, OperatorSubject).GetQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.Bytes.Limit, Is.EqualTo(0), "a ceiling of zero is a ceiling, not an absent one");
            Assert.That(report.Bytes.IsBounded, Is.True, "capped-at-zero must never read as unbounded");
            Assert.That(report.Bytes.BurstLimit, Is.EqualTo(0));
            Assert.That(report.Bytes.Overage, Is.EqualTo(5), "every byte is over a zero cap");
        });
    }

    [Test]
    public async Task GetQuotaUsageAsync_bounded_dimensions_round_trip_every_figure()
    {
        var quotas = new TenantQuotas
        {
            MaxBytes = 10_000,
            MaxKeys = 500,
            MaxMemoryBytes = 2_000,
            MaxTreeCount = 8,
            MaxOpsPerSecond = 250,
            BurstPercent = 20,
        };
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, quotas));
        var reader = new FakeTenantUsageReader().With(
            Acme,
            Usage(bytes: 4_100, keys: 600, memoryBytes: 1_500, treeCount: 3),
            quotas,
            Overage(bytes: 11, keys: 22));

        var report = await Facade(registry, reader, OperatorSubject).GetQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.Bytes.Usage, Is.EqualTo(4_100));
            Assert.That(report.Bytes.Limit, Is.EqualTo(10_000));
            Assert.That(report.Bytes.BurstLimit, Is.EqualTo(12_000), "20% burst above a 10000 ceiling");
            Assert.That(report.Bytes.Overage, Is.Zero);
            Assert.That(report.Bytes.MeteredOverage, Is.EqualTo(11));

            Assert.That(report.Keys.Usage, Is.EqualTo(600));
            Assert.That(report.Keys.Limit, Is.EqualTo(500));
            Assert.That(report.Keys.Overage, Is.EqualTo(100), "overage is measured above the steady-state cap, not the burst cap");
            Assert.That(report.Keys.MeteredOverage, Is.EqualTo(22));

            Assert.That(report.MemoryBytes.Usage, Is.EqualTo(1_500));
            Assert.That(report.MemoryBytes.Limit, Is.EqualTo(2_000));
            Assert.That(report.TreeCount.Usage, Is.EqualTo(3));
            Assert.That(report.TreeCount.Limit, Is.EqualTo(8));

            Assert.That(report.BurstPercent, Is.EqualTo(20));
            Assert.That(report.HasUsage, Is.True);
            Assert.That(report.IsDefault, Is.False);
        });
    }

    [Test]
    public async Task GetQuotaUsageAsync_ops_per_second_reports_its_ceiling_but_no_usage()
    {
        var quotas = new TenantQuotas { MaxOpsPerSecond = 250, BurstPercent = 10 };
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, quotas));
        var reader = new FakeTenantUsageReader().With(Acme, Usage(bytes: 1), quotas);

        var report = await Facade(registry, reader, OperatorSubject).GetQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.OpsPerSecond.Limit, Is.EqualTo(250));
            Assert.That(
                report.OpsPerSecond.BurstLimit,
                Is.EqualTo(275),
                "the burst ceiling matches the engine evaluator exactly, which multiplies before dividing "
                + "through a 128-bit intermediate (250 + 250 * 10 / 100 = 275) so a ceiling that is not a "
                + "multiple of 100 keeps its full burst allowance rather than losing it to a floored divide");
            Assert.That(report.OpsPerSecond.IsBounded, Is.True);
            Assert.That(
                report.OpsPerSecond.Usage,
                Is.Null,
                "the engine samples no operation rate, so an unmeasured usage must not be faked as zero");
            Assert.That(report.OpsPerSecond.IsMeasured, Is.False);
        });
    }

    [Test]
    public async Task GetQuotaUsageAsync_reports_the_burst_ceiling_the_evaluator_admits_at()
    {
        // Regression for a report/enforcement divergence: the mapping computed the
        // burst ceiling as ceiling / 100 * burstPercent (divide first), which
        // floors the allowance to zero for any ceiling below 100 and understates
        // any ceiling that is not a multiple of 100. The evaluator multiplies
        // first (ceiling + ceiling * burstPercent / 100), so a 50% burst over a
        // ceiling of 10 admits up to 15 while the report claimed 10, and over a
        // ceiling of 250 admits 375 while the report claimed 350. The report must
        // name the same ceiling the evaluator enforces.
        var quotas = new TenantQuotas { MaxTreeCount = 10, MaxBytes = 250, BurstPercent = 50 };
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, quotas));
        var reader = new FakeTenantUsageReader().With(Acme, Usage(bytes: 1, treeCount: 1), quotas);

        var report = await Facade(registry, reader, OperatorSubject).GetQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(
                report.TreeCount.BurstLimit,
                Is.EqualTo(15),
                "10 + 50% is 15; a sub-100 ceiling must not floor its burst to zero (the old divide-first gave 10)");
            Assert.That(
                report.Bytes.BurstLimit,
                Is.EqualTo(375),
                "250 + 50% is 375; the old divide-first floored 250 / 100 to 2 and gave 350");
        });
    }

    [Test]
    public async Task GetQuotaUsageAsync_zero_usage_is_measured_and_distinct_from_unmeasured()
    {
        var quotas = new TenantQuotas { MaxBytes = 100 };
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, quotas));
        var reader = new FakeTenantUsageReader().With(Acme, Usage(), quotas);

        var report = await Facade(registry, reader, OperatorSubject).GetQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.Bytes.Usage, Is.EqualTo(0));
            Assert.That(report.Bytes.IsMeasured, Is.True, "a real reading of zero is measured");
            Assert.That(report.OpsPerSecond.IsMeasured, Is.False);
        });
    }

    // ---- parity with the engine snapshot ---------------------------------

    [Test]
    public async Task GetQuotaUsageAsync_overage_and_burst_match_the_engine_snapshot_for_the_same_input()
    {
        var quotas = new TenantQuotas
        {
            MaxBytes = 1_000,
            MaxKeys = 100,
            MaxMemoryBytes = 4_000,
            MaxTreeCount = 2,
            BurstPercent = 15,
        };
        var usage = Usage(bytes: 1_400, keys: 40, memoryBytes: 9_000, treeCount: 5);
        var metered = Overage(bytes: 7, keys: 0, memoryBytes: 13, treeCount: 1);

        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, quotas));
        var reader = new FakeTenantUsageReader().With(Acme, usage, quotas, metered);

        var report = await Facade(registry, reader, OperatorSubject).GetQuotaUsageAsync("acme");

        // The independently-constructed engine projection for the same input.
        var snapshot = new TenantObservabilitySnapshot(Acme, usage, quotas, metered);
        var live = snapshot.InstantaneousOverage;

        Assert.Multiple(() =>
        {
            Assert.That(report.Bytes.Overage, Is.EqualTo(live.Bytes));
            Assert.That(report.Keys.Overage, Is.EqualTo(live.Keys));
            Assert.That(report.MemoryBytes.Overage, Is.EqualTo(live.MemoryBytes));
            Assert.That(report.TreeCount.Overage, Is.EqualTo(live.TreeCount));

            Assert.That(report.Bytes.MeteredOverage, Is.EqualTo(snapshot.MeteredOverage.Bytes));
            Assert.That(report.Keys.MeteredOverage, Is.EqualTo(snapshot.MeteredOverage.Keys));
            Assert.That(report.MemoryBytes.MeteredOverage, Is.EqualTo(snapshot.MeteredOverage.MemoryBytes));
            Assert.That(report.TreeCount.MeteredOverage, Is.EqualTo(snapshot.MeteredOverage.TreeCount));

            Assert.That(report.BurstPercent, Is.EqualTo(snapshot.Quotas.BurstPercent));
            Assert.That(report.Quotas, Is.EqualTo(TenantQuotasMapping.ToDescriptor(snapshot.Quotas)));
        });
    }

    // ---- enforcement scope ----------------------------------------------

    [Test]
    public async Task GetQuotaUsageAsync_reports_the_global_converged_scope()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, TenantQuotas.Unbounded));
        var reader = new FakeTenantUsageReader(TenantEnforcementScope.GlobalConverged)
            .With(Acme, Usage(bytes: 1), TenantQuotas.Unbounded);

        var report = await Facade(registry, reader, OperatorSubject).GetQuotaUsageAsync("acme");

        Assert.That(report.EnforcementScope, Is.EqualTo(TenantQuotaEnforcementScope.GlobalConverged));
    }

    [Test]
    public async Task GetQuotaUsageAsync_reports_the_per_cluster_scope()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, TenantQuotas.Unbounded));
        var reader = new FakeTenantUsageReader(TenantEnforcementScope.PerCluster)
            .With(Acme, Usage(bytes: 1), TenantQuotas.Unbounded);

        var report = await Facade(registry, reader, OperatorSubject).GetQuotaUsageAsync("acme");

        Assert.That(
            report.EnforcementScope,
            Is.EqualTo(TenantQuotaEnforcementScope.PerCluster),
            "a local reading must be labelled local so the UI cannot present it as a cross-cluster sum");
    }

    // ---- no warm usage view ---------------------------------------------

    [Test]
    public async Task GetQuotaUsageAsync_without_a_usage_view_reports_ceilings_and_unmeasured_usage()
    {
        var quotas = new TenantQuotas { MaxBytes = 1_000, MaxKeys = 50, BurstPercent = 10 };
        var registry = new FakeTenantRegistry();
        registry.Seed(Record(Acme, quotas));
        var reader = new FakeTenantUsageReader(TenantEnforcementScope.PerCluster);

        var report = await Facade(registry, reader, OperatorSubject).GetQuotaUsageAsync("acme");

        Assert.Multiple(() =>
        {
            Assert.That(report.HasUsage, Is.False);
            Assert.That(report.Bytes.Limit, Is.EqualTo(1_000), "the declared ceiling is still authoritative");
            Assert.That(report.Bytes.BurstLimit, Is.EqualTo(1_100));
            Assert.That(report.Bytes.Usage, Is.Null, "an unsampled tenant reports no usage rather than zero");
            Assert.That(report.Keys.IsMeasured, Is.False);
            Assert.That(report.MemoryBytes.Limit, Is.Null);
            Assert.That(report.BurstPercent, Is.EqualTo(10));
            Assert.That(
                report.EnforcementScope,
                Is.EqualTo(TenantQuotaEnforcementScope.PerCluster),
                "the scope is resolved directly, so an unmeasured report is never mislabelled global");
        });
    }

    [Test]
    public async Task GetQuotaUsageAsync_reports_the_reserved_default_tenant_as_default_and_unbounded()
    {
        var registry = new FakeTenantRegistry();
        registry.Seed(TenantRecord.CreateDefault(Stamp(1), "seed"));
        var reader = new FakeTenantUsageReader()
            .With(TenantId.Default, Usage(bytes: 12, keys: 3), TenantQuotas.Unbounded);

        var report = await Facade(registry, reader, OperatorSubject)
            .GetQuotaUsageAsync(TenantId.DefaultId);

        Assert.Multiple(() =>
        {
            Assert.That(report.IsDefault, Is.True);
            Assert.That(report.Quotas.IsUnbounded, Is.True);
            Assert.That(report.Bytes.Limit, Is.Null);
            Assert.That(report.Keys.Limit, Is.Null);
            Assert.That(report.MemoryBytes.Limit, Is.Null);
            Assert.That(report.TreeCount.Limit, Is.Null);
            Assert.That(report.OpsPerSecond.Limit, Is.Null);
            Assert.That(report.Bytes.Usage, Is.EqualTo(12), "an unbounded tenant still reports what it is using");
        });
    }
}
