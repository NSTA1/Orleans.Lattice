using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// The accept-then-poll half of <see cref="BackupToolInvocationsTests"/>: the start,
/// status, list and cancel invocations, plus the capture helpers the round-trip
/// tests use to start an operation and resolve the backup it produced.
/// </summary>
public sealed partial class BackupToolInvocationsTests
{
    private sealed record Captured(string BackupId, McpBackupManifest Manifest);

    private static async Task<Captured> CaptureAsync(
        FakeLatticeBackupControl control,
        string name,
        string treeId,
        string? scopeKind,
        string? keyOrPrefix,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var handle = await BackupToolInvocations.StartBackupAsync(
            control, name, treeId, scopeKind, keyOrPrefix, pageSize, operationId: null, cancellationToken);
        return await ResolveAsync(control, handle, cancellationToken);
    }

    private static async Task<Captured> CaptureIncrementalAsync(
        FakeLatticeBackupControl control,
        string name,
        string treeId,
        string? scopeKind,
        string? keyOrPrefix,
        string baseBackupId,
        int pageSize,
        CancellationToken cancellationToken)
    {
        var handle = await BackupToolInvocations.StartIncrementalBackupAsync(
            control, name, treeId, scopeKind, keyOrPrefix, baseBackupId, pageSize, operationId: null, cancellationToken);
        return await ResolveAsync(control, handle, cancellationToken);
    }

    private static async Task<Captured> ResolveAsync(
        FakeLatticeBackupControl control,
        McpBackupOperationHandle handle,
        CancellationToken cancellationToken)
    {
        var status = await BackupToolInvocations.GetOperationStatusAsync(control, handle.OperationId, cancellationToken);
        var backupId = status.Operation!.ResultReference!;
        var described = await BackupToolInvocations.DescribeBackupAsync(control, backupId, cancellationToken);
        return new Captured(backupId, described.Manifest!);
    }

    [Test]
    public async Task Start_backup_returns_a_handle_that_polls_to_the_captured_backup()
    {
        var control = new FakeLatticeBackupControl();

        var handle = await BackupToolInvocations.StartBackupAsync(
            control, "nightly", "orders", null, null, 0, operationId: "nightly-1", CancellationToken.None);
        var status = await BackupToolInvocations.GetOperationStatusAsync(control, handle.OperationId, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(handle.OperationId, Is.EqualTo("nightly-1"));
            Assert.That(handle.Kind, Is.EqualTo(BackupOperationKinds.Capture));
            Assert.That(handle.Created, Is.True);
            Assert.That(handle.TreeIds, Is.EqualTo(new[] { "orders" }));
            Assert.That(handle.StatusTool, Is.EqualTo(McpBackupOperationHandle.StatusToolName));
            Assert.That(status.Found, Is.True);
            Assert.That(status.Operation!.State, Is.EqualTo(nameof(LatticeOperationState.Succeeded)));
            Assert.That(status.Operation.ResultReference, Is.EqualTo("bk-0"));
            Assert.That(status.Operation.RestoreResult, Is.Null);
        });
    }

    [Test]
    public async Task A_retried_start_with_the_same_id_reports_the_existing_operation()
    {
        var control = new FakeLatticeBackupControl();
        await BackupToolInvocations.StartBackupAsync(control, "n", "orders", null, null, 0, "same", CancellationToken.None);

        var again = await BackupToolInvocations.StartBackupAsync(control, "n", "orders", null, null, 0, "same", CancellationToken.None);

        Assert.That(again.Created, Is.False);
    }

    [Test]
    public async Task An_empty_operation_id_is_passed_on_as_null_so_one_is_generated()
    {
        var control = new FakeLatticeBackupControl();

        await BackupToolInvocations.StartBackupAsync(control, "n", "orders", null, null, 0, string.Empty, CancellationToken.None);

        Assert.That(control.LastOperationId, Is.Null);
    }

    [Test]
    public async Task Start_backup_set_builds_one_whole_tree_scope_per_tree()
    {
        var control = new FakeLatticeBackupControl();

        var handle = await BackupToolInvocations.StartBackupSetAsync(
            control, "set", ["orders", "users"], crossTreeConsistent: true, pageSize: 0, operationId: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(handle.Kind, Is.EqualTo(BackupOperationKinds.SetCapture));
            Assert.That(control.LastSetRequest!.Scopes.Select(s => s.TreeId), Is.EqualTo(new[] { "orders", "users" }));
            Assert.That(control.LastSetRequest.Scopes.All(s => s.Kind == BackupScopeKind.WholeTree), Is.True);
            Assert.That(control.LastSetRequest.CrossTreeConsistent, Is.True);
        });
    }

    [Test]
    public async Task Status_of_an_unknown_operation_reports_not_found()
    {
        var control = new FakeLatticeBackupControl();

        var status = await BackupToolInvocations.GetOperationStatusAsync(control, "absent", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status.OperationId, Is.EqualTo("absent"));
            Assert.That(status.Found, Is.False);
            Assert.That(status.Operation, Is.Null);
        });
    }

    [Test]
    public async Task Status_projects_progress_without_inventing_a_total()
    {
        var control = new FakeLatticeBackupControl();
        control.SeedOperation(new LatticeOperationStatus
        {
            OperationId = "running",
            Kind = BackupOperationKinds.IncrementalCapture,
            Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
            State = LatticeOperationState.Running,
            Phase = BackupOperationPhases.Capturing,
            PhaseIndex = 0,
            PhaseCount = 2,
            CompletedUnits = 7,
            TotalUnits = null,
            UnitName = BackupOperationUnits.Entries,
        });

        var status = await BackupToolInvocations.GetOperationStatusAsync(control, "running", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(status.Operation!.State, Is.EqualTo(nameof(LatticeOperationState.Running)));
            Assert.That(status.Operation.Phase, Is.EqualTo(BackupOperationPhases.Capturing));
            Assert.That(status.Operation.PhaseIndex, Is.EqualTo(0));
            Assert.That(status.Operation.PhaseCount, Is.EqualTo(2));
            Assert.That(status.Operation.CompletedUnits, Is.EqualTo(7));
            Assert.That(status.Operation.TotalUnits, Is.Null);
            Assert.That(status.Operation.UnitName, Is.EqualTo(BackupOperationUnits.Entries));
        });
    }

    [Test]
    public async Task List_pages_operations_newest_first_with_a_cursor()
    {
        var control = new FakeLatticeBackupControl();
        for (var i = 0; i < 3; i++)
        {
            await BackupToolInvocations.StartBackupAsync(control, "n", "orders", null, null, 0, null, CancellationToken.None);
        }

        var first = await BackupToolInvocations.ListOperationsAsync(control, pageSize: 2, pageToken: null, CancellationToken.None);
        var second = await BackupToolInvocations.ListOperationsAsync(control, 2, first.NextPageToken, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first.Operations.Select(o => o.OperationId), Is.EqualTo(new[] { "op-2", "op-1" }));
            Assert.That(first.NextPageToken, Is.Not.Null);
            Assert.That(second.Operations.Select(o => o.OperationId), Is.EqualTo(new[] { "op-0" }));
            Assert.That(second.NextPageToken, Is.Null);
        });
    }

    [Test]
    public async Task Cancel_reports_the_request_and_not_found_for_an_unknown_id()
    {
        var control = new FakeLatticeBackupControl();
        control.SeedOperation(new LatticeOperationStatus
        {
            OperationId = "long",
            Kind = BackupOperationKinds.Restore,
            Scope = new LatticeOperationScope { TenantId = "default", TreeIds = ["orders"] },
            State = LatticeOperationState.Running,
            Phase = BackupOperationPhases.Applying,
        });

        var cancelled = await BackupToolInvocations.CancelOperationAsync(control, "long", CancellationToken.None);
        var unknown = await BackupToolInvocations.CancelOperationAsync(control, "absent", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(cancelled.Found, Is.True);
            Assert.That(cancelled.Operation!.CancelRequested, Is.True);
            Assert.That(unknown.Found, Is.False);
        });
    }

    [Test]
    public void Unauthorized_caller_is_denied_on_the_operation_tools()
    {
        var control = new FakeLatticeBackupControl { Authorized = false };

        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await BackupToolInvocations.GetOperationStatusAsync(control, "x", CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await BackupToolInvocations.ListOperationsAsync(control, 0, null, CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await BackupToolInvocations.CancelOperationAsync(control, "x", CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
            Assert.That(
                async () => await BackupToolInvocations.StartRestoreAsync(
                    control, "bk-0", null, null, null, null, CancellationToken.None),
                Throws.TypeOf<LatticeAuthorizationDeniedException>());
        });
    }
}
