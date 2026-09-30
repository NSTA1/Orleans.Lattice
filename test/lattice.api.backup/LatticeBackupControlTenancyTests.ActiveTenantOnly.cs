using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Coverage for <see cref="BackupCatalogRequest.ActiveTenantOnly"/> (issue #4025):
/// a caller whose gate admits every tree - a platform operator - is handed every
/// tenant's backups, and a listing narrowed to its resolved active tenant keeps
/// only that tenant's own. The reserved default tenant is the case that matters:
/// it owns only bare trees, never a <c>t/{tenant}/</c> one.
/// </summary>
public sealed partial class LatticeBackupControlTenancyTests
{
    [Test]
    public async Task ListBackupsAsync_narrowed_to_a_tenant_lists_only_its_own_backups()
    {
        await _fixture.InitializeAsync();
        var globex = await CaptureAsAsync(Globex, LocalName);
        var acme = await CaptureAsAsync(Acme, LocalName);
        var legacy = await CaptureLegacyAsync();

        var page = await ControlFor(Acme).ListBackupsAsync(new BackupCatalogRequest { ActiveTenantOnly = true });
        var newest = await ControlFor(Acme).ListBackupsAsync(
            new BackupCatalogRequest { ActiveTenantOnly = true, OrderByCreatedDescending = true });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.Id), Does.Contain(acme.BackupId));
            Assert.That(page.Entries.Select(e => e.Id), Does.Not.Contain(globex.BackupId));
            Assert.That(page.Entries.Select(e => e.Id), Does.Not.Contain(legacy.BackupId));
            Assert.That(page.Tenant, Is.EqualTo(Acme));
            Assert.That(newest.Entries.Select(e => e.Id), Is.EqualTo(new[] { acme.BackupId }));
            Assert.That(newest.Tenant, Is.EqualTo(Acme));
        });
    }

    [Test]
    public async Task ListBackupsAsync_narrowed_to_the_default_tenant_lists_no_other_tenants_backup()
    {
        await _fixture.InitializeAsync();
        var globex = await CaptureAsAsync(Globex, LocalName);
        var legacy = await CaptureLegacyAsync();
        var control = _fixture.CreateControlForTenant(new FixedTenantResolver(TenantId.Default));

        var page = await control.ListBackupsAsync(new BackupCatalogRequest { ActiveTenantOnly = true, PageSize = 1000 });
        var unnarrowed = await control.ListBackupsAsync(new BackupCatalogRequest { PageSize = 1000 });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.Id), Does.Contain(legacy.BackupId));
            Assert.That(page.Entries.Select(e => e.Scope.TreeId), Has.None.StartsWith("t/"));
            Assert.That(page.Entries.Select(e => e.Id), Does.Not.Contain(globex.BackupId));
            Assert.That(page.Tenant, Is.EqualTo(TenantId.DefaultId));
            Assert.That(unnarrowed.Entries.Select(e => e.Id), Does.Contain(globex.BackupId), "the unnarrowed listing is unchanged");
            Assert.That(unnarrowed.Tenant, Is.Null);
        });
    }

    [Test]
    public async Task ListBackupsAsync_narrowed_under_a_denied_assertion_fails_closed()
    {
        await _fixture.InitializeAsync();
        var control = _fixture.CreateControlForTenant(new FixedTenantResolver(default));

        Assert.ThrowsAsync<LatticeTenantAccessDeniedException>(
            () => control.ListBackupsAsync(new BackupCatalogRequest { ActiveTenantOnly = true }));
    }

    [TestCase("t/acme/orders", "acme", true)]
    [TestCase("t/acme/orders", "globex", false)]
    [TestCase("orders", "default", true)]
    [TestCase("t/acme/orders", "default", false)]
    [TestCase("sys-backup-catalog", "default", false)]
    [TestCase("", "default", false)]
    public void IsOwnedBy_decides_by_the_tenancy_ownership_grammar(string treeId, string tenant, bool owned)
    {
        Assert.That(LatticeBackupControl.IsOwnedBy(treeId, TenantId.Parse(tenant)), Is.EqualTo(owned));
    }
}
