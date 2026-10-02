using System.Text;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Backup.Tests;

/// <summary>
/// End-to-end coverage of the accept-then-poll backup maintenance verbs of
/// <see cref="ILatticeBackupOperations"/> (#4125) against a live single-silo
/// cluster: a health check, a catalog rebuild and a catalog scrub each return a
/// handle at once and poll to their outcome with real progress; each is authorized
/// exactly as its deprecated blocking twin; a catalog operation needs the restore
/// grant over the catalog to start or cancel; and the deprecated blocking verbs run
/// through the same tracked path and return as before.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class LatticeBackupMaintenanceOperationsIntegrationTests
{
    private const string Tree = "maint-orders";

    private ApiBackupClusterFixture _fixture = null!;
    private string _backupId = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ApiBackupClusterFixture();
        await _fixture.InitializeAsync();
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(Tree);
        for (var i = 0; i < 6; i++)
        {
            await tree.SetAsync($"k{i}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        var capture = await UntilTerminalAsync(
            Operations,
            (await Operations.StartBackupAsync(new LatticeBackupCaptureRequest("maint", BackupScopeSelector.WholeTree(Tree)))).OperationId);
        _backupId = capture.ResultReference!;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ILatticeBackupOperations Operations => (ILatticeBackupOperations)_fixture.Control;

    private ILatticeBackupOperations OperationsWith(ILatticeAccessGate gate) =>
        (ILatticeBackupOperations)_fixture.CreateControlWith(new BackupAccessAuthorizer(gate));

    private static async Task<LatticeOperationStatus> UntilTerminalAsync(ILatticeBackupOperations operations, string operationId)
    {
        LatticeOperationStatus? status = null;
        await TestPoll.UntilAsync(
            async () => (status = await operations.GetOperationStatusAsync(operationId)) is { IsTerminal: true },
            $"operation {operationId} to finish");
        return status!;
    }

    [Test]
    public async Task A_started_health_check_polls_to_the_verdict_and_persists_the_report()
    {
        var handle = await Operations.StartBackupHealthCheckAsync(_backupId, "health-1");

        var status = await UntilTerminalAsync(Operations, handle.OperationId);
        var persisted = await _fixture.Control.GetBackupHealthAsync(_backupId);

        Assert.Multiple(() =>
        {
            Assert.That(handle.Kind, Is.EqualTo(BackupOperationKinds.HealthCheck));
            Assert.That(handle.Created, Is.True);
            Assert.That(handle.Scope.TreeIds, Is.EqualTo(new[] { Tree }), "Scoped to the backup's own tree.");
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
            Assert.That(status.ResultReference, Is.EqualTo(_backupId));
            Assert.That(status.Result[BackupOperationResultKeys.HealthStatus], Is.EqualTo(nameof(BackupHealthStatus.Healthy)));
            Assert.That(status.UnitName, Is.EqualTo(BackupOperationUnits.Artifacts));
            Assert.That(status.TotalUnits, Is.GreaterThan(0));
            Assert.That(status.CompletedUnits, Is.EqualTo(status.TotalUnits), "Every artifact was checked.");
            Assert.That(persisted, Is.Not.Null);
            Assert.That(persisted!.Status, Is.EqualTo(BackupHealthStatus.Healthy));
        });
    }

    [Test]
    public async Task A_retried_health_check_start_with_the_same_id_starts_nothing()
    {
        var first = await Operations.StartBackupHealthCheckAsync(_backupId, "health-idempotent");
        await UntilTerminalAsync(Operations, first.OperationId);

        var second = await Operations.StartBackupHealthCheckAsync(_backupId, "health-idempotent");

        Assert.That(second.Created, Is.False);
    }

    [Test]
    public void A_health_check_of_an_uncatalogued_backup_is_refused_before_anything_starts()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await Operations.StartBackupHealthCheckAsync("no-such-backup"), Throws.TypeOf<KeyNotFoundException>());
            Assert.That(async () => await Operations.StartBackupHealthCheckAsync(string.Empty), Throws.ArgumentException);
            Assert.That(async () => await Operations.StartBackupHealthCheckAsync(_backupId, "bad/id"), Throws.ArgumentException);
        });
    }

    [Test]
    public void A_health_check_is_authorized_as_its_blocking_twin()
    {
        var denied = OperationsWith(new DenyGate());

        Assert.That(
            async () => await denied.StartBackupHealthCheckAsync(_backupId),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task A_started_catalog_rebuild_polls_to_its_report()
    {
        var handle = await Operations.StartCatalogRebuildAsync("rebuild-1");

        var status = await UntilTerminalAsync(Operations, handle.OperationId);

        Assert.Multiple(() =>
        {
            Assert.That(handle.Kind, Is.EqualTo(BackupOperationKinds.CatalogRebuild));
            Assert.That(handle.Scope.TreeIds, Is.EqualTo(new[] { BackupConstants.CatalogTree }));
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
            Assert.That(BackupOperationResults.TryReadCatalogRebuildReport(status.Result, out var report), Is.True);
            Assert.That(report!.ScannedCount, Is.GreaterThanOrEqualTo(1));
            Assert.That(status.CompletedUnits, Is.EqualTo(report.ScannedCount));
            Assert.That(status.TotalUnits, Is.Null, "A streamed sink scan invents no total.");
            Assert.That(status.UnitName, Is.EqualTo(BackupOperationUnits.Manifests));
        });
    }

    [Test]
    public async Task A_started_pruning_scrub_removes_an_orphan_and_reports_it()
    {
        var orphan = await CreateOrphanAsync();

        var handle = await Operations.StartCatalogScrubAsync(pruneOrphans: true, operationId: "scrub-prune");
        var status = await UntilTerminalAsync(Operations, handle.OperationId);

        Assert.Multiple(async () =>
        {
            Assert.That(handle.Kind, Is.EqualTo(BackupOperationKinds.CatalogScrub));
            Assert.That(status.State, Is.EqualTo(LatticeOperationState.Succeeded), status.FailureReason);
            Assert.That(BackupOperationResults.TryReadCatalogScrubReport(status.Result, out var report), Is.True);
            Assert.That(report!.Pruned, Is.True);
            Assert.That(report.OrphanBackupIds, Does.Contain(orphan));
            Assert.That(status.TotalUnits, Is.EqualTo(report.OrphanCount), "The last phase counted the orphans removed.");
            Assert.That(status.CompletedUnits, Is.EqualTo(report.OrphanCount));
            Assert.That(await _fixture.Catalog.GetAsync(orphan), Is.Null, "The orphan row was pruned.");
        });
    }

    [Test]
    public async Task A_non_pruning_scrub_flags_without_removing()
    {
        var orphan = await CreateOrphanAsync();

        var status = await UntilTerminalAsync(Operations, (await Operations.StartCatalogScrubAsync()).OperationId);

        Assert.Multiple(async () =>
        {
            Assert.That(BackupOperationResults.TryReadCatalogScrubReport(status.Result, out var report), Is.True);
            Assert.That(report!.Pruned, Is.False);
            Assert.That(report.OrphanBackupIds, Does.Contain(orphan));
            Assert.That(status.TotalUnits, Is.Null, "Without pruning only the streamed row probe ran.");
            Assert.That(status.CompletedUnits, Is.EqualTo(report.ScannedCount));
            Assert.That(await _fixture.Catalog.GetAsync(orphan), Is.Not.Null);
        });
    }

    [Test]
    public void Catalog_maintenance_needs_the_restore_grant_over_the_catalog()
    {
        var backupOnly = OperationsWith(new AllowAllButRestoreGate());

        Assert.Multiple(() =>
        {
            Assert.That(async () => await backupOnly.StartCatalogRebuildAsync(), Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(async () => await backupOnly.StartCatalogScrubAsync(), Throws.TypeOf<LatticeAuthorizationDeniedException>());
        });
    }

    [Test]
    public async Task Cancelling_a_catalog_operation_needs_the_restore_grant_that_started_it()
    {
        var handle = await Operations.StartCatalogRebuildAsync("rebuild-cancel-grant");
        await UntilTerminalAsync(Operations, handle.OperationId);
        var backupOnly = OperationsWith(new AllowAllButRestoreGate());

        Assert.That(
            async () => await backupOnly.CancelOperationAsync(handle.OperationId),
            Throws.TypeOf<LatticeAuthorizationDeniedException>());
    }

    [Test]
    public async Task Maintenance_operations_are_listed_with_the_other_backup_operations()
    {
        var health = await Operations.StartBackupHealthCheckAsync(_backupId, "list-health");
        var rebuild = await Operations.StartCatalogRebuildAsync("list-rebuild");
        await UntilTerminalAsync(Operations, health.OperationId);
        await UntilTerminalAsync(Operations, rebuild.OperationId);

        var page = await Operations.ListOperationsAsync(new LatticeOperationListRequest { PageSize = 500 });

        Assert.That(page.Operations.Select(o => o.OperationId), Is.SupersetOf(new[] { "list-health", "list-rebuild" }));
    }

    [Test]
    public async Task A_maintenance_operation_is_not_found_by_a_caller_with_no_grant()
    {
        var handle = await Operations.StartCatalogRebuildAsync("rebuild-hidden");
        await UntilTerminalAsync(Operations, handle.OperationId);
        var denied = OperationsWith(new DenyGate());

        Assert.Multiple(async () =>
        {
            Assert.That(await denied.GetOperationStatusAsync(handle.OperationId), Is.Null);
            Assert.That(await denied.CancelOperationAsync(handle.OperationId), Is.Null);
        });
    }

#pragma warning disable LATTICE0002 // The deprecated wrappers are exercised on purpose.
    [Test]
    public async Task The_deprecated_blocking_maintenance_verbs_run_as_tracked_operations_and_return_as_before()
    {
        var before = (await Operations.ListOperationsAsync(new LatticeOperationListRequest { PageSize = 500 })).Operations
            .Select(o => o.OperationId).ToHashSet();

        var health = await _fixture.Control.CheckBackupHealthAsync(_backupId);
        var rebuild = await _fixture.Control.RebuildCatalogFromSinkAsync();
        var scrub = await _fixture.Control.ScrubCatalogAgainstSinkAsync();

        var tracked = (await Operations.ListOperationsAsync(new LatticeOperationListRequest { PageSize = 500 })).Operations
            .Where(o => !before.Contains(o.OperationId))
            .ToList();
        Assert.Multiple(() =>
        {
            Assert.That(health.Status, Is.EqualTo(BackupHealthStatus.Healthy));
            Assert.That(rebuild.ScannedCount, Is.GreaterThanOrEqualTo(1));
            Assert.That(scrub.Pruned, Is.False);
            Assert.That(
                tracked.Select(o => o.Kind),
                Is.EquivalentTo(new[] { BackupOperationKinds.HealthCheck, BackupOperationKinds.CatalogRebuild, BackupOperationKinds.CatalogScrub }));
            Assert.That(tracked.All(o => o.State == LatticeOperationState.Succeeded), Is.True);
        });
    }

    [Test]
    public void The_deprecated_health_check_still_throws_the_engines_own_exception()
    {
        Assert.Multiple(() =>
        {
            Assert.That(async () => await _fixture.Control.CheckBackupHealthAsync("no-such-backup"), Throws.TypeOf<KeyNotFoundException>());
            Assert.That(
                async () => await ((ILatticeBackupControl)OperationsWith(new AllowAllButRestoreGate())).RebuildCatalogFromSinkAsync(),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        });
    }

    [Test]
    public void The_deprecated_maintenance_verbs_honour_an_already_cancelled_token()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await _fixture.Control.CheckBackupHealthAsync(_backupId, cts.Token), Throws.InstanceOf<OperationCanceledException>());
            Assert.That(async () => await _fixture.Control.RebuildCatalogFromSinkAsync(cts.Token), Throws.InstanceOf<OperationCanceledException>());
            Assert.That(async () => await _fixture.Control.ScrubCatalogAgainstSinkAsync(cancellationToken: cts.Token), Throws.InstanceOf<OperationCanceledException>());
        });
    }
#pragma warning restore LATTICE0002

    private async Task<string> CreateOrphanAsync()
    {
        // Drift: capture a tree of its own (backup ids are content-addressed, so a
        // capture of the shared tree could alias the fixture's backup), then drop
        // the sink manifest while the catalog row survives, so the catalog lists a
        // backup the sink can no longer resolve.
        var treeId = "maint-orphan-" + Guid.NewGuid().ToString("N");
        await _fixture.GrainFactory.GetGrain<ILattice>(treeId).SetAsync("k", Encoding.UTF8.GetBytes(treeId));
        var captured = await UntilTerminalAsync(
            Operations,
            (await Operations.StartBackupAsync(new LatticeBackupCaptureRequest("orphan", BackupScopeSelector.WholeTree(treeId)))).OperationId);
        Assert.That(await _fixture.Sink.DeleteManifestAsync(captured.ResultReference!), Is.True);
        return captured.ResultReference!;
    }

    private sealed class DenyGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default) =>
            new(LatticeAccessDecision.Deny("denied by test"));
    }

    private sealed class AllowAllButRestoreGate : ILatticeAccessGate
    {
        public ValueTask<LatticeAccessDecision> AuthorizeAsync(in LatticeAccessRequest request, CancellationToken cancellationToken = default) =>
            new(request.Operation == LatticeOperation.Restore
                ? LatticeAccessDecision.Deny("no restore grant")
                : LatticeAccessDecision.Allow());
    }
}
