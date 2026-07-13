using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// Coverage for the admin / status surface on the <see cref="ILatticeBackupControl"/>
/// facade: <see cref="ILatticeBackupControl.GetInventoryAsync"/> summarizes the
/// catalog counts and the in-memory registry tallies while hiding manifests the
/// caller may not read, and
/// <see cref="ILatticeBackupControl.GetScopeStatusAsync"/> reports a scope's
/// schedule registration and last-run status - returning <c>null</c> for an
/// unknown scope and failing closed under a denying gate.
/// </summary>
[Category("Integration")]
public sealed class LatticeBackupControlAdminTests
{
    private const string Source = "orders";

    private ApiBackupClusterFixture _fixture = null!;

    [SetUp]
    public void SetUp()
    {
        BackupInventoryRegistry.Instance.Reset();
        _fixture = new ApiBackupClusterFixture();
    }

    [TearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    // ---- Inventory ------------------------------------------------------

    [Test]
    public async Task GetInventoryAsync_reports_catalog_counts_and_registry_tallies()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await source.SetAsync("k2", Bytes("v2"));

        var full = await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));
        await source.SetAsync("k3", Bytes("v3"));
        await _fixture.Control.CreateIncrementalBackupAsync(
            new LatticeBackupIncrementalCaptureRequest(
                "incr", BackupScopeSelector.WholeTree(Source), full.BackupId));

        var report = await _fixture.Control.GetInventoryAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.TotalBackupCount, Is.EqualTo(2));
            Assert.That(report.FullBackupCount, Is.EqualTo(1));
            Assert.That(report.IncrementalBackupCount, Is.EqualTo(1));
            Assert.That(report.TotalCatalogBytes, Is.GreaterThan(0));
            Assert.That(report.OldestBackupUtc, Is.Not.Null);
            Assert.That(report.NewestBackupUtc, Is.Not.Null);
            Assert.That(report.NewestBackupUtc, Is.GreaterThanOrEqualTo(report.OldestBackupUtc!.Value));
            Assert.That(report.CaptureFailureCount, Is.Zero);
            Assert.That(report.RestoreFailureCount, Is.Zero);
        });
    }

    [Test]
    public async Task GetInventoryAsync_excludes_manifests_the_caller_may_not_read()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        var denying = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no read grant"), membership: null));

        var report = await denying.GetInventoryAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.TotalBackupCount, Is.Zero);
            Assert.That(report.TotalCatalogBytes, Is.Zero);
            Assert.That(report.OldestBackupUtc, Is.Null);
            Assert.That(report.NewestBackupUtc, Is.Null);
        });
    }

    // ---- Catalog rebuild from sink --------------------------------------

    [Test]
    public async Task RebuildCatalogFromSinkAsync_repopulates_the_catalog_from_the_sink()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));

        var captured = await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        // Drift: drop the catalog row while the sink still holds the manifest, as
        // a non-clean restart would. The backup lists nothing yet is intact in the
        // sink.
        await _fixture.Catalog.RemoveAsync(captured.BackupId);
        Assert.That(await _fixture.Catalog.GetAsync(captured.BackupId), Is.Null);

        var report = await _fixture.Control.RebuildCatalogFromSinkAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.ScannedCount, Is.GreaterThanOrEqualTo(1));
            Assert.That(report.RegisteredCount, Is.GreaterThanOrEqualTo(1));
        });
        Assert.That(await _fixture.Catalog.GetAsync(captured.BackupId), Is.Not.Null);
    }

    [Test]
    public async Task RebuildCatalogFromSinkAsync_is_idempotent_on_re_run()
    {
        await _fixture.InitializeAsync();
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await _fixture.Control.CreateBackupAsync(
            new LatticeBackupCaptureRequest("full", BackupScopeSelector.WholeTree(Source)));

        await _fixture.Control.RebuildCatalogFromSinkAsync();
        var second = await _fixture.Control.RebuildCatalogFromSinkAsync();

        // Everything the sink holds is already catalogued, so the second pass adds
        // nothing new and reconciles the existing rows in place.
        Assert.Multiple(() =>
        {
            Assert.That(second.RegisteredCount, Is.Zero);
            Assert.That(second.ReconciledCount, Is.EqualTo(second.ScannedCount));
        });
    }

    [Test]
    public async Task RebuildCatalogFromSinkAsync_denied_permission_fails_closed()
    {
        await _fixture.InitializeAsync();

        var denying = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no restore grant"), membership: null));

        Assert.That(
            async () => await denying.RebuildCatalogFromSinkAsync(),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());
    }

    // ---- Scope status ---------------------------------------------------

    [Test]
    public async Task GetScopeStatusAsync_unknown_scope_returns_null()
    {
        await _fixture.InitializeAsync();

        var status = await _fixture.Control.GetScopeStatusAsync(
            BackupScopeSelector.WholeTree("never-touched"));

        Assert.That(status, Is.Null);
    }

    [Test]
    public async Task GetScopeStatusAsync_after_scheduled_capture_reports_last_run_and_success()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));

        // Register the schedule and drive a full capture through the scheduler so
        // the scheduler grain records the run and its outcome.
        await _fixture.Scheduler.EnsureScheduleAsync(scope);
        var backupId = await _fixture.Scheduler.TriggerFullBackupAsync(scope);
        Assert.That(backupId, Is.Not.Null);

        var status = await _fixture.Control.GetScopeStatusAsync(scope);

        Assert.That(status, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(status!.Scope.TreeId, Is.EqualTo(Source));
            Assert.That(status.LastFullRunUtc, Is.Not.Null);
            Assert.That(status.LastFullSuccessUtc, Is.Not.Null);
            Assert.That(status.LastRunOutcome, Is.EqualTo(BackupScopeRunOutcome.Success));
            Assert.That(status.ChainDepth, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task GetScopeStatusAsync_carries_runtime_schedule_intervals()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);

        await _fixture.Control.ScheduleBackupAsync(
            new LatticeBackupScheduleRequest(scope, incremental: false, TimeSpan.FromMinutes(20)));
        await _fixture.Control.ScheduleBackupAsync(
            new LatticeBackupScheduleRequest(scope, incremental: true, TimeSpan.FromMinutes(45)));

        var status = await _fixture.Control.GetScopeStatusAsync(scope);

        Assert.That(status, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(status!.RuntimeFullBackupInterval, Is.EqualTo(TimeSpan.FromMinutes(20)));
            Assert.That(status.RuntimeIncrementalBackupInterval, Is.EqualTo(TimeSpan.FromMinutes(45)));
        });
    }

    [Test]
    public async Task GetScopeStatusAsync_denied_permission_fails_closed()
    {
        await _fixture.InitializeAsync();
        var scope = BackupScopeSelector.WholeTree(Source);
        var source = _fixture.GrainFactory.GetGrain<ILattice>(Source);
        await source.SetAsync("k1", Bytes("v1"));
        await _fixture.Control.CreateBackupAsync(new LatticeBackupCaptureRequest("full", scope));

        var denying = _fixture.CreateControlWith(
            new BackupAccessAuthorizer(new DenyingAccessGate("no read grant"), membership: null));

        Assert.That(
            async () => await denying.GetScopeStatusAsync(scope),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());
    }

    // ---- Argument guards ------------------------------------------------

    [Test]
    public async Task GetScopeStatusAsync_null_scope_throws()
    {
        await _fixture.InitializeAsync();
        Assert.That(
            async () => await _fixture.Control.GetScopeStatusAsync(null!),
            Throws.ArgumentNullException);
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>A minimal access gate that denies every request, driving the fail-closed path.</summary>
    private sealed class DenyingAccessGate(string reason) : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(
            in LatticeAccessRequest request,
            CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny(reason));
    }
}
