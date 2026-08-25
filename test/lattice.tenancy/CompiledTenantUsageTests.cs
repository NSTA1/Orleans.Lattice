using static Orleans.Lattice.Tenancy.Tests.TestClocks;
using static Orleans.Lattice.Tenancy.Tests.UsageTestData;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Unit tests for <see cref="CompiledTenantUsage"/>: the immutable admission
/// snapshot that joins each registered tenant's quota with its global fold and this
/// cluster's local slot. Covers the registry-keyed join, the global-vs-local slot
/// selection, that a usage-only tenant is skipped, that a registry tenant with no
/// usage yet gets empty aggregates, and the lookup guards.
/// </summary>
[TestFixture]
public sealed class CompiledTenantUsageTests
{
    private const string LocalCluster = "east";

    private static TenantRecord Registry(string tenantId, TenantQuotas quotas) =>
        TenantRecord.Create(
            TenantId.Parse(tenantId),
            TenantStatus.Active,
            quotas,
            TenantPlacement.Shared,
            Clock(1),
            "seed");

    [Test]
    public void Empty_has_no_tenants()
    {
        Assert.Multiple(() =>
        {
            Assert.That(CompiledTenantUsage.Empty.TenantCount, Is.EqualTo(0));
            Assert.That(CompiledTenantUsage.Empty.TryGetView(TenantId.Parse("acme"), out _), Is.False);
        });
    }

    [Test]
    public void Compile_null_arguments_throw()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => CompiledTenantUsage.Compile(null!, [], LocalCluster), Throws.ArgumentNullException);
            Assert.That(() => CompiledTenantUsage.Compile([], null!, LocalCluster), Throws.ArgumentNullException);
            Assert.That(() => CompiledTenantUsage.Compile([], [], null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Compile_joins_the_quota_with_the_global_fold_and_the_local_slot()
    {
        var registry = new[] { Registry("acme", new TenantQuotas { MaxBytes = 10_000 }) };
        var usage = new[]
        {
            UsageRecord("acme", ("east", Sample(100, 1, 10, 1)), ("west", Sample(200, 2, 20, 1))),
        };

        var compiled = CompiledTenantUsage.Compile(registry, usage, LocalCluster);

        Assert.That(compiled.TryGetView(TenantId.Parse("acme"), out var view), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(view.Quotas.MaxBytes, Is.EqualTo(10_000));
            Assert.That(view.GlobalUsage, Is.EqualTo(Sample(300, 3, 30, 2)), "the global fold sums every cluster slot");
            Assert.That(view.LocalUsage, Is.EqualTo(Sample(100, 1, 10, 1)), "the local usage is only this cluster's slot");
        });
    }

    [Test]
    public void UsageFor_selects_the_global_fold_or_the_local_slot_by_scope()
    {
        var registry = new[] { Registry("acme", new TenantQuotas { MaxBytes = 10_000 }) };
        var usage = new[] { UsageRecord("acme", ("east", Sample(100, 1, 10, 1)), ("west", Sample(200, 2, 20, 1))) };

        var compiled = CompiledTenantUsage.Compile(registry, usage, LocalCluster);
        compiled.TryGetView(TenantId.Parse("acme"), out var view);

        Assert.Multiple(() =>
        {
            Assert.That(view.UsageFor(TenantEnforcementScope.GlobalConverged), Is.EqualTo(Sample(300, 3, 30, 2)));
            Assert.That(view.UsageFor(TenantEnforcementScope.PerCluster), Is.EqualTo(Sample(100, 1, 10, 1)));
        });
    }

    [Test]
    public void A_registry_tenant_with_no_usage_record_gets_empty_aggregates()
    {
        var registry = new[] { Registry("acme", new TenantQuotas { MaxBytes = 10_000 }) };

        var compiled = CompiledTenantUsage.Compile(registry, [], LocalCluster);

        Assert.That(compiled.TryGetView(TenantId.Parse("acme"), out var view), Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(view.GlobalUsage, Is.EqualTo(LocalUsageSample.Empty), "enforcement fails open until the first sample lands");
            Assert.That(view.LocalUsage, Is.EqualTo(LocalUsageSample.Empty));
        });
    }

    [Test]
    public void A_usage_record_with_no_registry_record_is_skipped()
    {
        var usage = new[] { UsageRecord("ghost", ("east", Sample(100, 1, 10, 1))) };

        var compiled = CompiledTenantUsage.Compile([], usage, LocalCluster);

        Assert.Multiple(() =>
        {
            Assert.That(compiled.TenantCount, Is.EqualTo(0), "there is no quota to admit a usage-only tenant against");
            Assert.That(compiled.TryGetView(TenantId.Parse("ghost"), out _), Is.False);
        });
    }

    [Test]
    public void The_local_slot_is_empty_when_this_cluster_has_not_published()
    {
        var registry = new[] { Registry("acme", new TenantQuotas { MaxBytes = 10_000 }) };
        var usage = new[] { UsageRecord("acme", ("west", Sample(200, 2, 20, 1))) };

        var compiled = CompiledTenantUsage.Compile(registry, usage, LocalCluster);
        compiled.TryGetView(TenantId.Parse("acme"), out var view);

        Assert.Multiple(() =>
        {
            Assert.That(view.GlobalUsage, Is.EqualTo(Sample(200, 2, 20, 1)), "the global fold still sees the remote slot");
            Assert.That(view.LocalUsage, Is.EqualTo(LocalUsageSample.Empty), "this cluster has no local slot yet");
        });
    }

    [Test]
    public void TryGetView_of_the_no_tenant_value_is_false()
    {
        var registry = new[] { Registry("acme", new TenantQuotas { MaxBytes = 10_000 }) };
        var compiled = CompiledTenantUsage.Compile(registry, [], LocalCluster);

        Assert.That(compiled.TryGetView(default, out _), Is.False);
    }
}
