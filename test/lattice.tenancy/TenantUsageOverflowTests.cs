namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>Guards quota accounting against wrapped cross-tree and cross-cluster totals.</summary>
[TestFixture]
public sealed class TenantUsageOverflowTests
{
    [Test]
    public void Add_overflow_saturates_each_dimension_and_preserves_associativity()
    {
        var large = new LocalUsageSample
        {
            Bytes = long.MaxValue, Keys = long.MaxValue,
            MemoryBytes = long.MaxValue, TreeCount = long.MaxValue,
        };
        var one = new LocalUsageSample { Bytes = 1, Keys = 1, MemoryBytes = 1, TreeCount = 1 };
        Assert.Multiple(() =>
        {
            Assert.That(large.Add(one), Is.EqualTo(large));
            Assert.That(one.Add(large), Is.EqualTo(large));
            Assert.That(large.Add(one).Add(one), Is.EqualTo(large.Add(one.Add(one))));
        });
    }

    [Test]
    public void RollUp_overflow_saturates_instead_of_publishing_negative_usage()
    {
        var rolled = LocalUsageSample.RollUp(
            [new TreeUsageSample(long.MaxValue, long.MaxValue, long.MaxValue), new TreeUsageSample(1, 1, 1)]);
        Assert.Multiple(() =>
        {
            Assert.That(rolled.Bytes, Is.EqualTo(long.MaxValue));
            Assert.That(rolled.Keys, Is.EqualTo(long.MaxValue));
            Assert.That(rolled.MemoryBytes, Is.EqualTo(long.MaxValue));
            Assert.That(rolled.TreeCount, Is.EqualTo(2));
        });
    }

    [Test]
    public void Fold_overflow_cannot_reopen_a_tenants_footprint_admission()
    {
        var tenant = TenantId.Parse("acme");
        var usage = TenantUsageRecord.Create(tenant);
        usage.SetLocalSample("a", new LocalUsageSample { Bytes = long.MaxValue }, HybridLogicalClock.Zero, "a");
        usage.SetLocalSample("b", new LocalUsageSample { Bytes = 1 }, HybridLogicalClock.Zero, "b");
        var view = CompiledTenantUsage.Compile(
            [TenantRecord.Create(tenant, TenantStatus.Active, new TenantQuotas { MaxBytes = 10 },
                TenantPlacement.Shared, HybridLogicalClock.Zero, "a")], [usage], "a");
        Assert.That(view.TryGetView(tenant, out var compiled), Is.True);
        Assert.That(
            () => TenantQuotaEvaluator.Admit(tenant, compiled.Quotas,
                compiled.UsageFor(TenantEnforcementScope.GlobalConverged), "orders"),
            Throws.TypeOf<LatticeQuotaExceededException>());
    }
}
