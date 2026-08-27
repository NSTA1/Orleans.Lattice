using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Regression tests for issue #1706: an enumeration must prune the rows a
/// tenant-scoped caller may not see, never fail because of them.
/// </summary>
/// <remarks>
/// <para>
/// The backup facade turns the access gate's throw-on-deny into a boolean so each
/// catalogue row can be filtered. That translation originally caught only
/// <c>LatticeAuthorizationDeniedException</c>, but the tenancy scope refuses with
/// <see cref="LatticeBackupTenantIsolationException"/>, which escaped the probe -
/// so a single row belonging to another tenant failed the caller's whole listing.
/// </para>
/// <para>
/// Two things were wrong and both are pinned here. Functionally, tenant-scoped
/// listing and restore-point selection were unusable on any cluster holding a
/// backup the caller does not own, which on a multi-tenant cluster is the normal
/// state rather than an edge case. And the escaping refusal carried the offending
/// tree id, so the caller learned the name of another tenant's tree - the exact
/// probing the tenancy surface is careful to prevent elsewhere.
/// </para>
/// <para>
/// The fixture's silo does not register the tenancy add-on, so a control built the
/// ordinary way has no tenant scope and never refuses - which is why the sibling
/// tenancy tests could not observe this. These tests inject the refusing scope
/// explicitly, standing in for what the tenancy add-on registers, so the
/// translation under test is genuinely exercised.
/// </para>
/// </remarks>
public sealed partial class LatticeBackupControlTenancyTests
{
    // ---- Cross-tenant enumeration prunes rather than faults ---------------

    /// <summary>
    /// A tenant scope that refuses any tree outside <paramref name="tenant"/>'s
    /// namespace, mirroring the real <c>TenantBackupScope</c> contract: it throws
    /// <see cref="LatticeBackupTenantIsolationException"/> rather than returning a
    /// verdict.
    /// </summary>
    private sealed class RefusingTenantScope(string tenant) : ILatticeBackupTenantScope
    {
        private readonly string _prefix = $"t/{tenant}/";

        public bool IsActive => true;

        public void AuthorizeCapture(string treeId) => Evaluate(treeId, "captured");

        public void AuthorizeRestoreTarget(string treeId) => Evaluate(treeId, "restored into");

        public ValueTask<IBackupRestoreAdmission> BeginRestoreAsync(
            string targetTreeId,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException("Not exercised by these tests.");

        private void Evaluate(string treeId, string verb)
        {
            if (treeId.StartsWith(_prefix, StringComparison.Ordinal))
            {
                return;
            }

            throw new LatticeBackupTenantIsolationException(
                $"Tree '{treeId}' cannot be {verb} by tenant '{tenant}': it is owned by a "
                + "different tenant. A tenant-scoped backup is confined to its own namespace.");
        }
    }

    /// <summary>
    /// A control whose authorizer carries a tenant scope refusing everything outside
    /// <paramref name="tenant"/> - the wiring a tenancy-enabled cluster has.
    /// </summary>
    private ILatticeBackupControl ScopedControlFor(string tenant) =>
        _fixture.CreateControlWith(
            new BackupAccessAuthorizer(
                _fixture.SiloServices.GetRequiredService<ILatticeAccessGate>(),
                membership: null,
                tenantScope: new RefusingTenantScope(tenant)),
            new FixedTenantResolver(TenantId.Parse(tenant)));

    [Test]
    public async Task ListBackupsAsync_prunes_another_tenants_rows_instead_of_failing()
    {
        await _fixture.InitializeAsync();
        var globex = await CaptureAsAsync(Globex, LocalName);
        var acme = await CaptureAsAsync(Acme, LocalName);

        var page = await ScopedControlFor(Acme).ListBackupsAsync(new BackupCatalogRequest());

        var ids = page.Entries.Select(e => e.Id).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(ids, Does.Contain(acme.BackupId),
                "The caller must still see its own backup.");
            Assert.That(ids, Does.Not.Contain(globex.BackupId),
                "Another tenant's backup must be pruned from the page.");
        });
    }

    [Test]
    public async Task ListBackupsAsync_returns_an_empty_page_rather_than_disclosing_a_foreign_tree()
    {
        await _fixture.InitializeAsync();
        await CaptureAsAsync(Globex, LocalName);

        // Every catalogue row belongs to someone else - the case that previously
        // threw, naming the foreign tree in the message.
        var page = await ScopedControlFor(Acme).ListBackupsAsync(new BackupCatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.Empty,
                "A caller entitled to nothing must get an empty page, not an error.");
            Assert.That(
                page.Entries.Select(e => e.Scope.TreeId),
                Has.None.EqualTo(Effective(Globex, LocalName)));
        });
    }

    [Test]
    public async Task ListBackupsAsync_newest_first_prunes_rather_than_failing()
    {
        await _fixture.InitializeAsync();
        await CaptureAsAsync(Globex, LocalName);
        var acme = await CaptureAsAsync(Acme, LocalName);

        var page = await ScopedControlFor(Acme).ListBackupsAsync(
            new BackupCatalogRequest { OrderByCreatedDescending = true });

        Assert.That(page.Entries.Select(e => e.Id), Is.EqualTo(new[] { acme.BackupId }));
    }

    [Test]
    public async Task GetInventoryAsync_reports_a_foreign_scope_without_failing()
    {
        // The capability probe answers "can I back up / restore this scope?", so a
        // tenancy refusal is an answer - false - not a fault.
        await _fixture.InitializeAsync();
        await CaptureAsAsync(Globex, LocalName);

        Assert.That(
            async () => await ScopedControlFor(Acme).GetInventoryAsync(),
            Throws.Nothing,
            "A foreign row must not fail the inventory probe.");
    }

    [Test]
    public async Task An_unscoped_caller_still_sees_the_whole_catalogue()
    {
        // The negative control: pruning must follow the tenant scope rather than
        // being applied unconditionally, so a platform caller is unaffected.
        await _fixture.InitializeAsync();
        var globex = await CaptureAsAsync(Globex, LocalName);
        var acme = await CaptureAsAsync(Acme, LocalName);

        var page = await ControlFor(Acme).ListBackupsAsync(new BackupCatalogRequest());

        var ids = page.Entries.Select(e => e.Id).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(ids, Does.Contain(acme.BackupId));
            Assert.That(ids, Does.Contain(globex.BackupId));
        });
    }
}
