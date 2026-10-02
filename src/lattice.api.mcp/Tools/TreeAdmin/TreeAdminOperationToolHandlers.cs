using System.ComponentModel;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The thin adapter methods behind the accept-then-poll tree-administration MCP
/// tools: one start tool per long maintenance verb, and the shared status, list and
/// cancel tools. Every method is a stateless shim over
/// <see cref="ILatticeTreeAdminOperations"/>, resolved from the tool invocation's
/// request service provider; the facade owns authorization and lifecycle.
/// </summary>
internal static class TreeAdminOperationToolHandlers
{
    /// <summary>Starts a tracked materialised-view rebuild.</summary>
    public static async Task<McpTreeAdminOperationHandle> StartViewRebuildAsync(
        ILatticeTreeAdminOperations operations,
        [Description("The logical materialised-view name to rebuild. Must not be null or empty.")]
        string viewName,
        [Description("Optional idempotency id (1 to 128 ASCII letters, digits, '-', '_' or '.'). A retried start with the same id returns the existing operation. Omit to generate one.")]
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        return ToHandle(await operations
            .StartViewRebuildAsync(viewName, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false));
    }

    /// <summary>Starts a tracked materialised-view reconcile.</summary>
    public static async Task<McpTreeAdminOperationHandle> StartViewReconcileAsync(
        ILatticeTreeAdminOperations operations,
        [Description("The logical materialised-view name to reconcile. Must not be null or empty.")]
        string viewName,
        [Description("Optional idempotency id (1 to 128 ASCII letters, digits, '-', '_' or '.'). A retried start with the same id returns the existing operation. Omit to generate one.")]
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        return ToHandle(await operations
            .StartViewReconcileAsync(viewName, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false));
    }

    /// <summary>Starts a tracked tag-index reconcile sweep.</summary>
    public static async Task<McpTreeAdminOperationHandle> StartTagIndexReconcileAsync(
        ILatticeTreeAdminOperations operations,
        [Description("The logical tag-index name to reconcile. Must not be null or empty.")]
        string indexName,
        [Description("Optional idempotency id (1 to 128 ASCII letters, digits, '-', '_' or '.'). A retried start with the same id returns the existing operation. Omit to generate one.")]
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        return ToHandle(await operations
            .StartTagIndexReconcileAsync(indexName, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false));
    }

    /// <summary>Starts a tracked WAL partition move.</summary>
    public static async Task<McpTreeAdminOperationHandle> StartWalMoveAsync(
        ILatticeTreeAdminOperations operations,
        [Description("The tree whose WAL partition to move. Must not be null, empty, or a reserved system tree id.")]
        string treeId,
        [Description("The WAL partition index to move. Must be in range for the tree.")]
        int partition,
        [Description("The target storage provider key to move the partition to. Must not be null or empty, and must resolve on every silo.")]
        string targetProviderKey,
        [Description("Optional quiesce lease in seconds for the fenced cutover. Zero or omitted takes the conventional 30-second default.")]
        double quiesceLeaseSeconds = 0,
        [Description("Optional entries copied per page. Zero or omitted takes the conventional 256-entry default.")]
        int copyPageSize = 0,
        [Description("Set true to skip verifying the copied target tail before flipping the placement pin. Defaults to false (verify enabled).")]
        bool disableVerifyAfterCopy = false,
        [Description("Optional idempotency id (1 to 128 ASCII letters, digits, '-', '_' or '.'). A retried start with the same id returns the existing operation. Omit to generate one.")]
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var options = new TreeWalMoveOptions
        {
            QuiesceLeaseSeconds = quiesceLeaseSeconds,
            CopyPageSize = copyPageSize,
            DisableVerifyAfterCopy = disableVerifyAfterCopy,
        };
        return ToHandle(await operations
            .StartWalMoveAsync(treeId, partition, targetProviderKey, options, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false));
    }

    /// <summary>Starts a tracked whole-tree orphaned-leaf audit.</summary>
    public static async Task<McpTreeAdminOperationHandle> StartOrphanedLeavesAuditAsync(
        ILatticeTreeAdminOperations operations,
        [Description("The tree to audit for orphaned leaves. Must not be null or empty.")]
        string treeId,
        [Description("Optional idempotency id (1 to 128 ASCII letters, digits, '-', '_' or '.'). A retried start with the same id returns the existing operation. Omit to generate one.")]
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        return ToHandle(await operations
            .StartOrphanedLeavesAuditAsync(treeId, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false));
    }

    /// <summary>Starts a tracked whole-tree orphaned-leaf repair.</summary>
    public static async Task<McpTreeAdminOperationHandle> StartOrphanedLeavesRepairAsync(
        ILatticeTreeAdminOperations operations,
        [Description("The tree whose orphaned leaves to repair. Must not be null, empty, or a reserved system tree id.")]
        string treeId,
        [Description("Optional idempotency id (1 to 128 ASCII letters, digits, '-', '_' or '.'). A retried start with the same id returns the existing operation. Omit to generate one.")]
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        return ToHandle(await operations
            .StartOrphanedLeavesRepairAsync(treeId, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false));
    }

    /// <summary>Reads a tracked tree-administration operation's status.</summary>
    public static async Task<McpTreeAdminOperationResult> GetOperationStatusAsync(
        ILatticeTreeAdminOperations operations,
        [Description("The operation id a start tool returned.")]
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var status = await operations.GetOperationStatusAsync(operationId, cancellationToken).ConfigureAwait(false);
        return ToResult(operationId, status);
    }

    /// <summary>Lists one page of the caller's tracked tree-administration operations.</summary>
    public static async Task<McpTreeAdminOperationPage> ListOperationsAsync(
        ILatticeTreeAdminOperations operations,
        [Description("The previous page's nextPageToken, passed back unaltered, or omitted for the first page.")]
        string? pageToken = null,
        [Description("The maximum operations to return. Zero or omitted takes the default of 50; clamped to 500.")]
        int pageSize = 0,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var page = await operations
            .ListOperationsAsync(
                new LatticeOperationListRequest { PageToken = NullIfEmpty(pageToken), PageSize = pageSize },
                cancellationToken)
            .ConfigureAwait(false);

        var items = new List<McpTreeAdminOperation>(page.Operations.Count);
        foreach (var status in page.Operations)
        {
            items.Add(ToOperation(status));
        }

        return new McpTreeAdminOperationPage { Operations = items, NextPageToken = page.NextPageToken };
    }

    /// <summary>Requests cancellation of a tracked tree-administration operation.</summary>
    public static async Task<McpTreeAdminOperationResult> CancelOperationAsync(
        ILatticeTreeAdminOperations operations,
        [Description("The operation id to cancel.")]
        string operationId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var status = await operations.CancelOperationAsync(operationId, cancellationToken).ConfigureAwait(false);
        return ToResult(operationId, status);
    }

    /// <summary>Maps a start verb's handle onto the MCP handle.</summary>
    internal static McpTreeAdminOperationHandle ToHandle(LatticeOperationHandle handle) =>
        new()
        {
            OperationId = handle.OperationId,
            Kind = handle.Kind,
            TreeIds = handle.Scope.TreeIds,
            Created = handle.Created,
        };

    /// <summary>Maps a status onto the MCP operation view.</summary>
    internal static McpTreeAdminOperation ToOperation(LatticeOperationStatus status) =>
        new()
        {
            OperationId = status.OperationId,
            Kind = status.Kind,
            TreeIds = status.Scope.TreeIds,
            State = status.State.ToString(),
            Phase = status.Phase,
            PhaseIndex = status.PhaseIndex,
            PhaseCount = status.PhaseCount,
            CompletedUnits = status.CompletedUnits,
            TotalUnits = status.TotalUnits,
            UnitName = status.UnitName,
            StartedAtUtc = status.StartedAtUtc,
            FinishedAtUtc = status.FinishedAtUtc,
            FailureReason = status.FailureReason,
            ResultReference = status.ResultReference,
            Result = status.Result,
            CancelRequested = status.CancelRequested,
        };

    private static McpTreeAdminOperationResult ToResult(string operationId, LatticeOperationStatus? status) =>
        new()
        {
            OperationId = operationId,
            Found = status is not null,
            Operation = status is null ? null : ToOperation(status),
        };

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
}
