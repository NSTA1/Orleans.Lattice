namespace Orleans.Lattice.Apps.Tests;

/// <summary>Unit tests for the immutable <see cref="CompiledAppRegistrySnapshot"/>.</summary>
[TestFixture]
public sealed class CompiledAppRegistrySnapshotTests
{
    private static readonly AppSlug Alpha = AppSlug.Parse("alpha");
    private static readonly AppSlug Zeta = AppSlug.Parse("zeta");

    [Test]
    public void Empty_has_epoch_zero_and_no_records()
    {
        var empty = CompiledAppRegistrySnapshot.Empty;

        Assert.That(empty.Epoch, Is.EqualTo(0));
        Assert.That(empty.Count, Is.EqualTo(0));
        Assert.That(empty.Records, Is.Empty);
        Assert.That(empty.TryGet(TenantId.Default, Alpha, out var record), Is.False);
        Assert.That(record, Is.Null);
        Assert.That(empty.GetTenantApps(TenantId.Default), Is.Empty);
        Assert.That(empty.GetEnabledTenantApps(TenantId.Default), Is.Empty);
    }

    [Test]
    public void Compile_indexes_by_tenant_and_slug_in_ordinal_order()
    {
        var acmeZeta = AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled, tenant: AppRegistryTestData.Acme, slug: Zeta);
        var acmeAlpha = AppRegistryTestData.Record(AppRegistryLifecycleState.Disabled, tenant: AppRegistryTestData.Acme, slug: Alpha);
        var defaultAlpha = AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled, slug: Alpha);

        var snapshot = CompiledAppRegistrySnapshot.Compile(new[] { acmeZeta, defaultAlpha, acmeAlpha }, epoch: 7);

        Assert.That(snapshot.Epoch, Is.EqualTo(7));
        Assert.That(snapshot.Count, Is.EqualTo(3));
        Assert.That(snapshot.Records, Is.EqualTo(new[] { acmeAlpha, acmeZeta, defaultAlpha }));
        Assert.That(snapshot.TryGet(AppRegistryTestData.Acme, Zeta, out var found), Is.True);
        Assert.That(found, Is.SameAs(acmeZeta));
        Assert.That(snapshot.TryGet(TenantId.Default, Zeta, out _), Is.False, "slugs are scoped per tenant");
        Assert.That(snapshot.GetTenantApps(AppRegistryTestData.Acme), Is.EqualTo(new[] { acmeAlpha, acmeZeta }));
        Assert.That(snapshot.GetEnabledTenantApps(AppRegistryTestData.Acme), Is.EqualTo(new[] { acmeZeta }));
        Assert.That(snapshot.GetTenantApps(TenantId.Parse("other")), Is.Empty);
    }

    [Test]
    public void Warm_lookups_return_cached_lists()
    {
        var snapshot = CompiledAppRegistrySnapshot.Compile(new[] { AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled) }, 1);

        Assert.That(snapshot.GetTenantApps(TenantId.Default), Is.SameAs(snapshot.GetTenantApps(TenantId.Default)));
        Assert.That(snapshot.GetEnabledTenantApps(TenantId.Default), Is.SameAs(snapshot.GetEnabledTenantApps(TenantId.Default)));
    }

    [Test]
    public void TryGet_warm_path_does_not_allocate()
    {
        var snapshot = CompiledAppRegistrySnapshot.Compile(new[] { AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled) }, 1);
        var slug = AppRegistryTestData.Slug;
        var tenant = TenantId.Default;
        snapshot.TryGet(tenant, slug, out _);
        snapshot.GetEnabledTenantApps(tenant);

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            snapshot.TryGet(tenant, slug, out _);
            snapshot.GetTenantApps(tenant);
            snapshot.GetEnabledTenantApps(tenant);
        }

        Assert.That(GC.GetAllocatedBytesForCurrentThread() - before, Is.EqualTo(0));
    }

    [Test]
    public void Uninitialised_keys_miss_without_throwing()
    {
        var snapshot = CompiledAppRegistrySnapshot.Compile(new[] { AppRegistryTestData.Record(AppRegistryLifecycleState.Enabled) }, 1);

        Assert.That(snapshot.TryGet(default, AppRegistryTestData.Slug, out _), Is.False);
        Assert.That(snapshot.TryGet(TenantId.Default, default, out _), Is.False);
        Assert.That(snapshot.GetTenantApps(default), Is.Empty);
        Assert.That(snapshot.GetEnabledTenantApps(default), Is.Empty);
    }

    [Test]
    public void Compile_null_records_throws()
    {
        Assert.That(() => CompiledAppRegistrySnapshot.Compile(null!, 1), Throws.ArgumentNullException);
    }
}
