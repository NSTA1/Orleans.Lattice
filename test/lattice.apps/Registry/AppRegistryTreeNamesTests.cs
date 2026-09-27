using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>Unit tests for the app registry's reserved tree name and key layout.</summary>
[TestFixture]
public sealed class AppRegistryTreeNamesTests
{
    [Test]
    public void RegistryTree_is_the_reserved_sys_app_registry_tree()
    {
        Assert.That(AppRegistryTreeNames.RegistryTree, Is.EqualTo("sys-app-registry"));
        Assert.That(AppRegistryTreeNames.RegistryTree, Does.StartWith(LatticeConstants.AppRegistryTreePrefix));
        Assert.That(AppRegistryTreeNames.RegistryTree, Does.StartWith(LatticeConstants.SystemDataTreePrefix),
            "subsumed by sys-, so catalog-hidden and guarded against user-origin writes");
    }

    [Test]
    public void ComposeKey_is_tenant_slash_slug()
    {
        Assert.That(AppRegistryTreeNames.ComposeKey(TenantId.Default, AppSlug.Parse("notes")), Is.EqualTo("default/notes"));
        Assert.That(AppRegistryTreeNames.ComposeKey(TenantId.Parse("acme"), AppSlug.Parse("notes")), Is.EqualTo("acme/notes"));
    }

    [Test]
    public void ComposeKey_rejects_uninitialised_values()
    {
        Assert.That(() => AppRegistryTreeNames.ComposeKey(default, AppSlug.Parse("notes")), Throws.ArgumentException);
        Assert.That(() => AppRegistryTreeNames.ComposeKey(TenantId.Default, default), Throws.ArgumentException);
    }

    [Test]
    public void Tenant_range_covers_exactly_the_tenants_keys()
    {
        var tenant = TenantId.Parse("acme");
        var start = AppRegistryTreeNames.TenantRangeStart(tenant);
        var end = AppRegistryTreeNames.TenantRangeEnd(tenant);

        bool InRange(string key) => string.CompareOrdinal(key, start) >= 0 && string.CompareOrdinal(key, end) < 0;

        Assert.That(start, Is.EqualTo("acme/"));
        Assert.That(end, Is.EqualTo("acme0"));
        Assert.That(InRange("acme/a1"), Is.True);
        Assert.That(InRange("acme/z-z-z-z-z-z-z-z-z-z-z-z-z-z-z"), Is.True);
        Assert.That(InRange("acme-2/a1"), Is.False);
        Assert.That(InRange("acme2/a1"), Is.False);
        Assert.That(InRange("acm/a1"), Is.False);
        Assert.That(() => AppRegistryTreeNames.TenantRangeStart(default), Throws.ArgumentException);
        Assert.That(() => AppRegistryTreeNames.TenantRangeEnd(default), Throws.ArgumentException);
    }
}
