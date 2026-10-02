using Orleans.Lattice.Api.Operations;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The kind-agnostic adapter methods behind every accept-then-poll MCP tool: they
/// call an <see cref="ILatticeOperations"/> facade (which owns all scoping and
/// authorization) and project the shared operation contract onto the MCP
/// structured-content DTOs.
/// </summary>
internal static class LatticeOperationToolInvocations
{
    /// <summary>Projects a start verb's handle onto the MCP DTO.</summary>
    /// <param name="handle">The handle. Must not be <c>null</c>.</param>
    /// <param name="statusTool">The tool that polls the operation's status.</param>
    /// <returns>The MCP handle.</returns>
    public static McpLatticeOperationHandle ToMcp(LatticeOperationHandle handle, string statusTool)
    {
        ArgumentNullException.ThrowIfNull(handle);
        return new McpLatticeOperationHandle
        {
            OperationId = handle.OperationId,
            Kind = handle.Kind,
            TreeIds = handle.Scope.TreeIds,
            Created = handle.Created,
            StatusTool = statusTool,
        };
    }

    /// <summary>Projects an operation status onto the MCP DTO.</summary>
    /// <param name="status">The status. Must not be <c>null</c>.</param>
    /// <returns>The MCP view.</returns>
    public static McpLatticeOperation ToMcp(LatticeOperationStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        return new McpLatticeOperation
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
    }

    /// <summary>Reads one operation's status.</summary>
    public static async Task<McpLatticeOperationResult> GetStatusAsync(
        ILatticeOperations operations,
        string operationId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var status = await operations.GetOperationStatusAsync(operationId, cancellationToken).ConfigureAwait(false);
        return ToResult(operationId, status);
    }

    /// <summary>Requests cancellation of one operation.</summary>
    public static async Task<McpLatticeOperationResult> CancelAsync(
        ILatticeOperations operations,
        string operationId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var status = await operations.CancelOperationAsync(operationId, cancellationToken).ConfigureAwait(false);
        return ToResult(operationId, status);
    }

    /// <summary>Lists one page of the caller's operations, newest-first.</summary>
    public static async Task<McpLatticeOperationPage> ListAsync(
        ILatticeOperations operations,
        int pageSize,
        string? pageToken,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var page = await operations
            .ListOperationsAsync(
                new LatticeOperationListRequest
                {
                    PageSize = pageSize,
                    PageToken = string.IsNullOrEmpty(pageToken) ? null : pageToken,
                },
                cancellationToken)
            .ConfigureAwait(false);

        var items = new McpLatticeOperation[page.Operations.Count];
        for (var i = 0; i < items.Length; i++)
        {
            items[i] = ToMcp(page.Operations[i]);
        }

        return new McpLatticeOperationPage { Operations = items, NextPageToken = page.NextPageToken };
    }

    private static McpLatticeOperationResult ToResult(string operationId, LatticeOperationStatus? status) =>
        new()
        {
            OperationId = operationId,
            Found = status is not null,
            Operation = status is null ? null : ToMcp(status),
        };
}