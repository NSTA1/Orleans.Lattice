using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// The complementary half of the tenant-scoping coverage: the tree ids the
/// <see cref="ILatticeBackupControl"/> facade must <b>never</b> compose.
/// <para>
/// A scope read back off a stored <see cref="BackupManifest"/> was written with
/// the already-effective id at capture time. Re-composing it would either
/// double-scope it (<c>t/acme/t/acme/orders</c>) or, for a legacy manifest
/// captured before tenancy, silently re-attribute another tenant's - or the
/// default tenant's - backup to the current caller. Both corrupt restore, so
/// every manifest-derived authorization must present the stored id verbatim.
/// The reserved backup-catalog tree is platform-owned and likewise never
/// composed.
/// </para>
/// </summary>
public sealed partial class LatticeBackupControlTenancyTests
{
    // ---- Restore: the manifest-derived target is never re-composed ------

    [Test]
    public async Task RestoreBackupAsync_does_not_compose_the_manifest_derived_target_tree()
    {
        await _fixture.InitializeAsync();
        var captured = await CaptureAsAsync(Globex, LocalName);

        // A different tenant restores with NO explicit target, so the target falls
        // back to the tree the backup was captured from - an id that is already
        // effective.
        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);
        await control.RestoreBackupAsync(new LatticeRestoreRequest(captured.BackupId));

        var authorized = gate.TreeIdsFor(LatticeOperation.Restore);
        Assert.Multiple(() =>
        {
            Assert.That(authorized, Does.Contain(Effective(Globex, LocalName)));
            Assert.That(authorized, Does.Not.Contain(Effective(Acme, LocalName)));
            Assert.That(
                authorized,
                Has.None.StartsWith(LatticeTenantTrees.Compose(TenantId.Parse(Acme), "t/")),
                "the manifest scope must never be double-composed");
        });
    }

    [Test]
    public async Task ColdRestoreAsync_does_not_compose_the_manifest_derived_target_tree()
    {
        await _fixture.InitializeAsync();
        var captured = await CaptureAsAsync(Globex, LocalName);

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);
        await control.ColdRestoreAsync(new LatticeRestoreRequest(captured.BackupId));

        var authorized = gate.TreeIdsFor(LatticeOperation.Restore);
        Assert.Multiple(() =>
        {
            Assert.That(authorized, Does.Contain(Effective(Globex, LocalName)));
            Assert.That(authorized, Does.Not.Contain(Effective(Acme, LocalName)));
        });
    }

    [Test]
    public async Task RestoreBackupAsync_does_not_re_compose_a_legacy_bare_manifest_scope()
    {
        await _fixture.InitializeAsync();

        // A backup captured before tenancy was switched on: its manifest carries a
        // bare, default-tenant tree id.
        var captured = await CaptureLegacyAsync();

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);
        await control.RestoreBackupAsync(new LatticeRestoreRequest(captured.BackupId));

        // Composing here would quietly adopt a default-tenant backup into the
        // active tenant's namespace instead of letting the gate refuse the
        // crossing.
        Assert.That(gate.TreeIdsFor(LatticeOperation.Restore), Does.Contain(LegacyName));
        Assert.That(
            gate.TreeIdsFor(LatticeOperation.Restore),
            Does.Not.Contain(Effective(Acme, LegacyName)));
    }

    [Test]
    public async Task ColdRestoreAsync_does_not_re_compose_a_legacy_bare_manifest_scope()
    {
        await _fixture.InitializeAsync();
        var captured = await CaptureLegacyAsync();

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);
        await control.ColdRestoreAsync(new LatticeRestoreRequest(captured.BackupId));

        Assert.That(gate.TreeIdsFor(LatticeOperation.Restore), Does.Contain(LegacyName));
        Assert.That(
            gate.TreeIdsFor(LatticeOperation.Restore),
            Does.Not.Contain(Effective(Acme, LegacyName)));
    }

    // ---- Read / delete / health: the manifest scope is presented verbatim
    //
    // Each of these runs against a LEGACY manifest whose stored scope is a bare,
    // default-tenant tree id. That is the case with teeth: composition is
    // deliberately idempotent for an already-qualified t/ id, so only a bare
    // stored id can prove the facade is classifying the scope rather than relying
    // on that idempotence. Composing here would silently adopt a default-tenant
    // backup into the acting tenant's namespace.

    [Test]
    public async Task DescribeBackupAsync_authorizes_the_manifest_scope_verbatim()
    {
        await _fixture.InitializeAsync();
        var qualified = await CaptureAsAsync(Globex, LocalName);
        var legacy = await CaptureLegacyAsync();

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);
        await control.DescribeBackupAsync(qualified.BackupId);
        await control.DescribeBackupAsync(legacy.BackupId);

        AssertStoredScopesOnly(gate);
    }

    [Test]
    public async Task DeleteBackupAsync_authorizes_the_manifest_scope_verbatim()
    {
        await _fixture.InitializeAsync();
        var qualified = await CaptureAsAsync(Globex, LocalName);
        var legacy = await CaptureLegacyAsync();

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);
        await control.DeleteBackupAsync(qualified.BackupId);
        await control.DeleteBackupAsync(legacy.BackupId);

        AssertStoredScopesOnly(gate);
    }

    [Test]
    public async Task ExportArtifactAsync_authorizes_the_manifest_scope_verbatim()
    {
        await _fixture.InitializeAsync();
        var qualified = await CaptureAsAsync(Globex, LocalName);
        var legacy = await CaptureLegacyAsync();

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);
        await DrainArtifactAsync(control, qualified);
        await DrainArtifactAsync(control, legacy);

        AssertStoredScopesOnly(gate);
    }

    [Test]
    public async Task CheckBackupHealthAsync_authorizes_the_manifest_scope_verbatim()
    {
        await _fixture.InitializeAsync();
        var qualified = await CaptureAsAsync(Globex, LocalName);
        var legacy = await CaptureLegacyAsync();

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);
        await control.CheckBackupHealthAsync(qualified.BackupId);
        await control.CheckBackupHealthAsync(legacy.BackupId);

        AssertStoredScopesOnly(gate);
    }

    [Test]
    public async Task GetBackupHealthAsync_authorizes_the_manifest_scope_verbatim()
    {
        await _fixture.InitializeAsync();
        var qualified = await CaptureAsAsync(Globex, LocalName);
        var legacy = await CaptureLegacyAsync();

        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);
        await control.GetBackupHealthAsync(qualified.BackupId);
        await control.GetBackupHealthAsync(legacy.BackupId);

        AssertStoredScopesOnly(gate);
    }

    [Test]
    public async Task ConfigureBackupHealthAsync_authorizes_the_manifest_scope_verbatim()
    {
        await _fixture.InitializeAsync();
        var qualified = await CaptureAsAsync(Globex, LocalName);
        var legacy = await CaptureLegacyAsync();

        var config = new BackupHealthConfig(monitoringEnabled: true, TimeSpan.FromHours(12));
        var gate = new RecordingAccessGate();
        var control = ControlFor(Acme, gate);
        await control.ConfigureBackupHealthAsync(qualified.BackupId, config);
        await control.ConfigureBackupHealthAsync(legacy.BackupId, config);

        AssertStoredScopesOnly(gate);
    }

    // ---- Enumerations: every row is gated by its own stored scope --------

    [Test]
    public async Task ListBackupsAsync_authorizes_each_manifest_scope_verbatim()
    {
        await _fixture.InitializeAsync();
        await CaptureAsAsync(Globex, LocalName);
        await CaptureLegacyAsync();

        var gate = new RecordingAccessGate();
        await ControlFor(Acme, gate).ListBackupsAsync(new BackupCatalogRequest());

        AssertStoredScopesOnly(gate);
    }

    [Test]
    public async Task ListBackupsAsync_newest_first_authorizes_each_manifest_scope_verbatim()
    {
        await _fixture.InitializeAsync();
        await CaptureAsAsync(Globex, LocalName);
        await CaptureLegacyAsync();

        var gate = new RecordingAccessGate();
        await ControlFor(Acme, gate).ListBackupsAsync(
            new BackupCatalogRequest { OrderByCreatedDescending = true });

        AssertStoredScopesOnly(gate);
    }

    [Test]
    public async Task StreamBackupsAsync_authorizes_each_manifest_scope_verbatim()
    {
        await _fixture.InitializeAsync();
        await CaptureAsAsync(Globex, LocalName);
        await CaptureLegacyAsync();

        var gate = new RecordingAccessGate();
        await foreach (var _ in ControlFor(Acme, gate).StreamBackupsAsync())
        {
            // Drain.
        }

        AssertStoredScopesOnly(gate);
    }

    [Test]
    public async Task GetInventoryAsync_authorizes_each_manifest_scope_verbatim()
    {
        await _fixture.InitializeAsync();
        await CaptureAsAsync(Globex, LocalName);
        await CaptureLegacyAsync();

        var gate = new RecordingAccessGate();
        await ControlFor(Acme, gate).GetInventoryAsync();

        AssertStoredScopesOnly(gate);
    }

    // ---- Platform-owned catalog constants --------------------------------

    [Test]
    public async Task RebuildCatalogFromSinkAsync_authorizes_the_catalog_tree_verbatim()
    {
        await _fixture.InitializeAsync();

        var gate = new RecordingAccessGate();
        await ControlFor(Acme, gate).RebuildCatalogFromSinkAsync();

        Assert.That(
            gate.TreeIdsFor(LatticeOperation.Restore),
            Is.EqualTo(new[] { BackupConstants.CatalogTree }));
    }

    [Test]
    public async Task ScrubCatalogAgainstSinkAsync_authorizes_the_catalog_tree_verbatim()
    {
        await _fixture.InitializeAsync();

        var gate = new RecordingAccessGate();
        await ControlFor(Acme, gate).ScrubCatalogAgainstSinkAsync();

        Assert.That(
            gate.TreeIdsFor(LatticeOperation.Restore),
            Is.EqualTo(new[] { BackupConstants.CatalogTree }));
    }

    // ---- Helpers ---------------------------------------------------------

    /// <summary>
    /// Captures a whole-tree backup of <paramref name="localName"/> as
    /// <paramref name="tenant"/>, so the resulting manifest carries that tenant's
    /// effective tree id.
    /// </summary>
    private async Task<LatticeBackupCaptureResult> CaptureAsAsync(string tenant, string localName)
    {
        await SeedAsync(Effective(tenant, localName), "k", "v");
        var captured = await ControlFor(tenant).CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(localName)));

        Assert.That(captured.Manifest.Scope.TreeId, Is.EqualTo(Effective(tenant, localName)));
        return captured;
    }

    /// <summary>
    /// Captures a backup through the untenanted facade, producing a manifest whose
    /// stored scope is a bare, default-tenant tree id - a backup taken before
    /// tenancy was switched on. This is the manifest shape that can actually
    /// detect a wrongly-composed manifest scope, because composition is
    /// deliberately a no-op for an already-qualified <c>t/</c> id.
    /// </summary>
    private async Task<LatticeBackupCaptureResult> CaptureLegacyAsync()
    {
        await SeedAsync(LegacyName, "k", "legacy");
        var captured = await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("legacy", BackupScopeSelector.WholeTree(LegacyName)));

        Assert.That(captured.Manifest.Scope.TreeId, Is.EqualTo(LegacyName));
        return captured;
    }

    private static async Task DrainArtifactAsync(
        ILatticeBackupControl control,
        LatticeBackupCaptureResult captured)
    {
        var artifactId = captured.Manifest.ContentDescriptors[0].ArtifactId;
        await foreach (var _ in control.ExportArtifactAsync(captured.BackupId, artifactId))
        {
            // Drain: the authorization happens before the first chunk is yielded.
        }
    }

    /// <summary>
    /// Asserts the gate saw exactly the tree ids the manifests stored - the other
    /// tenant's qualified id and the legacy bare id - and never anything scoped
    /// into the acting tenant's namespace, whether by re-attribution
    /// (<c>t/acme/legacy-orders</c>) or double composition
    /// (<c>t/acme/t/globex/orders</c>).
    /// </summary>
    private static void AssertStoredScopesOnly(RecordingAccessGate gate)
    {
        var treeIds = gate.Requests.Select(r => r.TreeId).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(treeIds, Is.Not.Empty);
            Assert.That(treeIds, Does.Contain(Effective(Globex, LocalName)));
            Assert.That(treeIds, Does.Contain(LegacyName));
            Assert.That(
                treeIds,
                Has.None.StartsWith(LatticeTenantTrees.ComposePrefix(TenantId.Parse(Acme))),
                "a manifest-derived scope must never be composed under the acting tenant");
        });
    }
}
