using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

public sealed partial class LatticeTenantAdmissionControllerTests
{
    [Test]
    public async Task IsAdmittedAsync_concurrent_cold_admissions_refuse_after_slots_converge()
    {
        var index = new FakeTenantUsageIndex();
        var global = Create(index, TenantEnforcementScope.GlobalConverged);
        var local = Create(index, TenantEnforcementScope.PerCluster);
        var quotas = new TenantQuotas { MaxBytes = 1 };
        var record = TenantRecord.Create(Acme, TenantStatus.Active, quotas,
            TenantPlacement.Shared, TestClocks.Clock(1), "a");
        var a = TenantUsageRecord.Create(Acme);
        var b = TenantUsageRecord.Create(Acme);

        Assert.That(await global.IsAdmittedAsync(Acme, Tree), Is.True);
        Assert.That(await global.IsAdmittedAsync(Acme, Tree), Is.True);
        a.SetLocalSample("a", Sample(bytes: 1), TestClocks.Clock(2), "a");
        b.SetLocalSample("b", Sample(bytes: 1), TestClocks.Clock(2), "b");
        var converged = TenantUsageRecord.Merge(a, b);
        var compiled = CompiledTenantUsage.Compile([record], [converged], "a");
        Assert.That(compiled.TryGetView(Acme, out var view), Is.True);
        index.Views["acme"] = view;

        Assert.That(await local.IsAdmittedAsync(Acme, Tree), Is.True,
            "PerCluster reads its one slot even though the global sum exceeds the cap");
        Assert.That(async () => await global.IsAdmittedAsync(Acme, Tree),
            Throws.TypeOf<LatticeQuotaExceededException>());
        a.SetLocalSample("a", Sample(bytes: 2), TestClocks.Clock(3), "a");
        a.MergeFrom(converged);
        compiled = CompiledTenantUsage.Compile([record], [a], "a");
        Assert.That(compiled.TryGetView(Acme, out view), Is.True);
        index.Views["acme"] = view;
        Assert.That(async () => await local.IsAdmittedAsync(Acme, Tree),
            Throws.TypeOf<LatticeQuotaExceededException>());
    }

}
