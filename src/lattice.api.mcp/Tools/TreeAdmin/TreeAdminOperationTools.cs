using System.ComponentModel;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The accept-then-poll tools of the tree-administration group (#4126): start,
/// status, list and cancel for a schema compliance scan
/// (<see cref="ILatticeSchemaComplianceOperations"/>) and for a fresh cluster
/// storage-usage refresh (<see cref="ILatticeStorageUsageOperations"/>). Each tool
/// resolves its facade from the request service provider at call time, so a host
/// that does not register a surface fails the call with a clear message instead of
/// advertising the facade as a tool argument. The facades own every authorization
/// decision; these tools add none.
/// </summary>
/// <remarks>
/// They are contributed regardless of the schema-control and lifecycle opt-ins,
/// alongside the read-only <c>lattice_treeadmin_schema_scan_compliance</c> and
/// <c>lattice_treeadmin_storage_usage</c> they supersede: a scan and a refresh are
/// reads of the tree or the cluster, so the start and cancel tools are annotated
/// mutating (they record and stop an operation) but never destructive.
/// </remarks>
internal static class TreeAdminOperationTools
{
    /// <summary>The compliance-scan start tool.</summary>
    public const string ComplianceScanStartToolName = "lattice_treeadmin_schema_compliance_scan_start";

    /// <summary>The compliance-scan status tool.</summary>
    public const string ComplianceScanStatusToolName = "lattice_treeadmin_schema_compliance_scan_status";

    /// <summary>The compliance-scan list tool.</summary>
    public const string ComplianceScanListToolName = "lattice_treeadmin_schema_compliance_scan_list";

    /// <summary>The compliance-scan cancel tool.</summary>
    public const string ComplianceScanCancelToolName = "lattice_treeadmin_schema_compliance_scan_cancel";

    /// <summary>The storage-usage refresh start tool.</summary>
    public const string StorageRefreshStartToolName = "lattice_treeadmin_storage_usage_refresh_start";

    /// <summary>The storage-usage refresh status tool.</summary>
    public const string StorageRefreshStatusToolName = "lattice_treeadmin_storage_usage_refresh_status";

    /// <summary>The storage-usage refresh list tool.</summary>
    public const string StorageRefreshListToolName = "lattice_treeadmin_storage_usage_refresh_list";

    /// <summary>The storage-usage refresh cancel tool.</summary>
    public const string StorageRefreshCancelToolName = "lattice_treeadmin_storage_usage_refresh_cancel";

    private const string PageSizeDescription = "Maximum operations per page; <= 0 uses the server default (50, at most 500).";
    private const string PageTokenDescription = "Continuation cursor from a previous page's nextPageToken; null starts at the newest.";
    private const string OperationIdDescription =
        "Optional idempotency id (1 to 128 ASCII letters, digits, '-', '_' or '.'). Starting again with an id in use "
        + "returns the existing operation with created=false; omit to generate one.";

    /// <summary>Builds the eight operation tools.</summary>
    /// <returns>The tools.</returns>
    public static IReadOnlyList<McpServerTool> Create() =>
    [
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The tree whose current values to scan against its schema policy. Must not be null or empty.")] string treeId,
                CancellationToken cancellationToken,
                [Description(OperationIdDescription)] string? operationId = null) =>
                StartComplianceScanAsync(Compliance(context), treeId, operationId, cancellationToken),
            Options(
                ComplianceScanStartToolName,
                "Start a schema compliance scan",
                "Starts a tracked compliance scan of a tree and returns an operation handle at once. Poll "
                + ComplianceScanStatusToolName + " for progress (entries scanned of the tree's live entry count) and, on "
                + "success, the report in the result map (hasPolicy, compliantCount, nonCompliantCount, scannedCount and "
                + "rule.{i}.reason / rule.{i}.count). Needs read over the tree. Records an operation but never mutates "
                + "data.",
                readOnly: false)),
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The operation id from the start tool's handle.")] string operationId,
                CancellationToken cancellationToken) =>
                LatticeOperationToolInvocations.GetStatusAsync(Compliance(context), operationId, cancellationToken),
            Options(
                ComplianceScanStatusToolName,
                "Compliance scan status",
                "Reads a tracked compliance scan: its state (Queued, Running, Succeeded, Failed or Cancelled), phase "
                + "(Counting, then Scanning), progress as entries scanned of total (the total is null while unknown) and, "
                + "once succeeded, the report in the result map. Reports found=false for an unknown id or one the caller "
                + "may not see. Read-only.",
                readOnly: true)),
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                CancellationToken cancellationToken,
                [Description(PageSizeDescription)] int pageSize = 0,
                [Description(PageTokenDescription)] string? pageToken = null) =>
                LatticeOperationToolInvocations.ListAsync(Compliance(context), pageSize, pageToken, cancellationToken),
            Options(
                ComplianceScanListToolName,
                "List compliance scans",
                "Lists one page of the caller's tracked compliance scans over trees it may read, newest-first, including "
                + "finished ones until their retention window lapses. Read-only.",
                readOnly: true)),
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The operation id to cancel.")] string operationId,
                CancellationToken cancellationToken) =>
                LatticeOperationToolInvocations.CancelAsync(Compliance(context), operationId, cancellationToken),
            Options(
                ComplianceScanCancelToolName,
                "Cancel a compliance scan",
                "Requests cancellation of a tracked compliance scan; it stays Running until the scan observes the "
                + "request, then reads Cancelled. Needs read over the scanned tree.",
                readOnly: false)),
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                CancellationToken cancellationToken,
                [Description(OperationIdDescription)] string? operationId = null) =>
                StartStorageRefreshAsync(Storage(context), operationId, cancellationToken),
            Options(
                StorageRefreshStartToolName,
                "Start a storage-usage refresh",
                "Starts a tracked deep re-measure of every tree's storage usage and returns an operation handle at once. "
                + "Poll " + StorageRefreshStatusToolName + " for progress (trees measured of total) and, on success, the "
                + "cluster totals in the result map; then read the refreshed per-tree figures with "
                + "lattice_treeadmin_storage_usage (deep=false). Requires cluster telemetry authority. Records an "
                + "operation but never mutates data.",
                readOnly: false)),
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The operation id from the start tool's handle.")] string operationId,
                CancellationToken cancellationToken) =>
                LatticeOperationToolInvocations.GetStatusAsync(Storage(context), operationId, cancellationToken),
            Options(
                StorageRefreshStatusToolName,
                "Storage-usage refresh status",
                "Reads a tracked storage-usage refresh: its state, progress as trees measured of total and, once "
                + "succeeded, the cluster totals in the result map (treeCount, walRetainedBytes, snapshotBytes, "
                + "leafStateBytes, totalBytes, partial, sampledAt). Reports found=false for an unknown id or one the "
                + "caller may not see. Read-only.",
                readOnly: true)),
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                CancellationToken cancellationToken,
                [Description(PageSizeDescription)] int pageSize = 0,
                [Description(PageTokenDescription)] string? pageToken = null) =>
                LatticeOperationToolInvocations.ListAsync(Storage(context), pageSize, pageToken, cancellationToken),
            Options(
                StorageRefreshListToolName,
                "List storage-usage refreshes",
                "Lists one page of the caller's tracked storage-usage refreshes, newest-first. Requires cluster "
                + "telemetry authority. Read-only.",
                readOnly: true)),
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The operation id to cancel.")] string operationId,
                CancellationToken cancellationToken) =>
                LatticeOperationToolInvocations.CancelAsync(Storage(context), operationId, cancellationToken),
            Options(
                StorageRefreshCancelToolName,
                "Cancel a storage-usage refresh",
                "Requests cancellation of a tracked storage-usage refresh; it stays Running until the refresh observes "
                + "the request, then reads Cancelled. Requires cluster telemetry authority.",
                readOnly: false)),
    ];

    private static async Task<McpLatticeOperationHandle> StartComplianceScanAsync(
        ILatticeSchemaComplianceOperations operations,
        string treeId,
        string? operationId,
        CancellationToken cancellationToken)
    {
        var handle = await operations
            .StartComplianceScanAsync(treeId, string.IsNullOrEmpty(operationId) ? null : operationId, cancellationToken)
            .ConfigureAwait(false);
        return LatticeOperationToolInvocations.ToMcp(handle, ComplianceScanStatusToolName);
    }

    private static async Task<McpLatticeOperationHandle> StartStorageRefreshAsync(
        ILatticeStorageUsageOperations operations,
        string? operationId,
        CancellationToken cancellationToken)
    {
        var handle = await operations
            .StartStorageUsageRefreshAsync(string.IsNullOrEmpty(operationId) ? null : operationId, cancellationToken)
            .ConfigureAwait(false);
        return LatticeOperationToolInvocations.ToMcp(handle, StorageRefreshStatusToolName);
    }

    private static ILatticeSchemaComplianceOperations Compliance(RequestContext<CallToolRequestParams> context) =>
        context.Services?.GetService<ILatticeSchemaComplianceOperations>()
        ?? throw new InvalidOperationException(
            "This server registers no ILatticeSchemaComplianceOperations, so the compliance-scan operation tools are unavailable.");

    private static ILatticeStorageUsageOperations Storage(RequestContext<CallToolRequestParams> context) =>
        context.Services?.GetService<ILatticeStorageUsageOperations>()
        ?? throw new InvalidOperationException(
            "This server registers no ILatticeStorageUsageOperations, so the storage-usage refresh tools are unavailable.");

    private static McpServerToolCreateOptions Options(string name, string title, string description, bool readOnly) =>
        new()
        {
            Name = name,
            Title = title,
            Description = description,
            SerializerOptions = LatticeApiMcpToolSerialization.Options,
            ReadOnly = readOnly,
            Destructive = false,
            UseStructuredContent = true,
        };
}
