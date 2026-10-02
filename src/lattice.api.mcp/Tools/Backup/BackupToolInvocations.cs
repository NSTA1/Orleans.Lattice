using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The pure adapter layer between the backup MCP tools and the internal
/// <see cref="ILatticeBackupControl"/> and <see cref="ILatticeBackupOperations"/> facades: one method per tool that maps the
/// tool's arguments onto a facade call and projects the facade result onto the
/// compact MCP DTO. These methods hold no transport or authorization concern -
/// the fail-closed backup access gate lives in the facade and the caller
/// credential is stamped on the ambient context by the tool delegate before the
/// method runs - so they are directly unit-testable against a fake facade.
/// </summary>
internal static class BackupToolInvocations
{
    /// <summary>The default artifact-export page budget in bytes (256 KiB).</summary>
    public const int DefaultExportPageBytes = 256 * 1024;

    /// <summary>The maximum artifact-export page budget in bytes (4 MiB).</summary>
    public const int MaxExportPageBytes = 4 * 1024 * 1024;

    /// <summary>Lists one cursor-paged page of the backup catalog.</summary>
    public static async Task<McpBackupCatalogPage> ListBackupsAsync(
        ILatticeBackupControl control,
        int pageSize,
        string? pageToken,
        bool orderByCreatedDescending,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(control);
        var request = new BackupCatalogRequest
        {
            PageSize = pageSize,
            PageToken = string.IsNullOrEmpty(pageToken) ? null : pageToken,
            OrderByCreatedDescending = orderByCreatedDescending,
        };

        var page = await control.ListBackupsAsync(request, cancellationToken).ConfigureAwait(false);
        return BackupToolMappings.ToMcp(page);
    }

    /// <summary>Describes a single backup and its restore chain.</summary>
    public static async Task<McpBackupChain> DescribeBackupAsync(
        ILatticeBackupControl control,
        string backupId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(control);
        var description = await control.DescribeBackupAsync(backupId, cancellationToken).ConfigureAwait(false);
        return BackupToolMappings.ToMcp(description);
    }

    /// <summary>Builds the catalog-wide inventory summary.</summary>
    public static async Task<McpBackupInventory> GetInventoryAsync(
        ILatticeBackupControl control,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(control);
        var report = await control.GetInventoryAsync(cancellationToken).ConfigureAwait(false);
        return BackupToolMappings.ToMcp(report);
    }

    /// <summary>Reads a single scope's schedule and last-run status.</summary>
    public static async Task<McpBackupScopeStatus> GetScopeStatusAsync(
        ILatticeBackupControl control,
        string treeId,
        string? scopeKind,
        string? keyOrPrefix,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(control);
        var scope = BackupToolMappings.ToScope(treeId, scopeKind, keyOrPrefix);
        var status = await control.GetScopeStatusAsync(scope, cancellationToken).ConfigureAwait(false);
        return BackupToolMappings.ToMcp(status);
    }

    /// <summary>
    /// Exports one bounded page of a backup artifact's bytes, resuming from
    /// <paramref name="chunkOffset"/> and draining chunks until the byte budget
    /// is reached. Bounded memory: the binding never buffers the whole artifact.
    /// </summary>
    public static async Task<McpBackupArtifactPage> ExportArtifactAsync(
        ILatticeBackupControl control,
        string backupId,
        string artifactId,
        int chunkOffset,
        int maxBytes,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(control);
        var skip = chunkOffset < 0 ? 0 : chunkOffset;
        var budget = maxBytes <= 0
            ? DefaultExportPageBytes
            : Math.Min(maxBytes, MaxExportPageBytes);

        var buffer = new List<byte>(Math.Min(budget, DefaultExportPageBytes));
        var index = 0;
        var reachedBudget = false;

        await foreach (var chunk in control
            .ExportArtifactAsync(backupId, artifactId, cancellationToken)
            .ConfigureAwait(false))
        {
            if (index < skip)
            {
                index++;
                continue;
            }

            AppendChunk(buffer, chunk);
            index++;

            if (buffer.Count >= budget)
            {
                reachedBudget = true;
                break;
            }
        }

        var bytes = buffer.ToArray();
        return new McpBackupArtifactPage
        {
            BackupId = backupId,
            ArtifactId = artifactId,
            Base64Chunk = bytes.Length == 0 ? string.Empty : Convert.ToBase64String(bytes),
            ByteCount = bytes.Length,
            // When we stopped on the budget there may be more chunks; surface the
            // resume cursor. When the stream drained naturally, this is the last
            // page. A budget stop on the final chunk costs one extra empty page.
            NextChunkOffset = reachedBudget ? index : null,
            EndOfStream = !reachedBudget,
        };
    }

    /// <summary>Starts a tracked full capture of the requested scope.</summary>
    public static async Task<McpBackupOperationHandle> StartBackupAsync(
        ILatticeBackupOperations operations,
        string name,
        string treeId,
        string? scopeKind,
        string? keyOrPrefix,
        int pageSize,
        string? operationId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var scope = BackupToolMappings.ToScope(treeId, scopeKind, keyOrPrefix);
        var request = new LatticeBackupCaptureRequest(name, scope, NormalisePageSize(pageSize));
        var handle = await operations
            .StartBackupAsync(request, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false);
        return BackupToolMappings.ToMcp(handle);
    }

    /// <summary>Starts a tracked incremental capture layered on a base backup.</summary>
    public static async Task<McpBackupOperationHandle> StartIncrementalBackupAsync(
        ILatticeBackupOperations operations,
        string name,
        string treeId,
        string? scopeKind,
        string? keyOrPrefix,
        string baseBackupId,
        int pageSize,
        string? operationId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var scope = BackupToolMappings.ToScope(treeId, scopeKind, keyOrPrefix);
        var request = new LatticeBackupIncrementalCaptureRequest(name, scope, baseBackupId, NormalisePageSize(pageSize));
        var handle = await operations
            .StartIncrementalBackupAsync(request, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false);
        return BackupToolMappings.ToMcp(handle);
    }

    /// <summary>Starts a tracked whole-tree backup-set capture over <paramref name="treeIds"/>.</summary>
    public static async Task<McpBackupOperationHandle> StartBackupSetAsync(
        ILatticeBackupOperations operations,
        string name,
        IReadOnlyList<string> treeIds,
        bool crossTreeConsistent,
        int pageSize,
        string? operationId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(treeIds);
        var scopes = new BackupScopeSelector[treeIds.Count];
        for (var i = 0; i < scopes.Length; i++)
        {
            scopes[i] = BackupScopeSelector.WholeTree(treeIds[i]);
        }

        var request = new LatticeBackupSetCaptureRequest(name, scopes, crossTreeConsistent, NormalisePageSize(pageSize));
        var handle = await operations
            .StartBackupSetAsync(request, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false);
        return BackupToolMappings.ToMcp(handle);
    }

    /// <summary>Starts a tracked restore of a backup into its target tree.</summary>
    public static async Task<McpBackupOperationHandle> StartRestoreAsync(
        ILatticeBackupOperations operations,
        string backupId,
        string? targetTreeId,
        string? mode,
        string? restoreOperationId,
        string? operationId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var request = new LatticeRestoreRequest(
            backupId,
            NullIfEmpty(targetTreeId),
            scope: null,
            mode: BackupToolMappings.ToRestoreMode(mode),
            operationId: NullIfEmpty(restoreOperationId));
        var handle = await operations
            .StartRestoreAsync(request, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false);
        return BackupToolMappings.ToMcp(handle);
    }

    /// <summary>Starts a tracked health check of one backup against the durable sink.</summary>
    public static async Task<McpBackupOperationHandle> StartHealthCheckAsync(
        ILatticeBackupOperations operations,
        string backupId,
        string? operationId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var handle = await operations
            .StartBackupHealthCheckAsync(backupId, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false);
        return BackupToolMappings.ToMcp(handle);
    }

    /// <summary>Starts a tracked rebuild of the backup catalog from the durable sink.</summary>
    public static async Task<McpBackupOperationHandle> StartCatalogRebuildAsync(
        ILatticeBackupOperations operations,
        string? operationId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var handle = await operations
            .StartCatalogRebuildAsync(NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false);
        return BackupToolMappings.ToMcp(handle);
    }

    /// <summary>Starts a tracked scrub of the backup catalog against the durable sink.</summary>
    public static async Task<McpBackupOperationHandle> StartCatalogScrubAsync(
        ILatticeBackupOperations operations,
        bool pruneOrphans,
        string? operationId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var handle = await operations
            .StartCatalogScrubAsync(pruneOrphans, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false);
        return BackupToolMappings.ToMcp(handle);
    }

    /// <summary>Reads a tracked backup operation's status.</summary>
    public static async Task<McpBackupOperationResult> GetOperationStatusAsync(
        ILatticeBackupOperations operations,
        string operationId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var status = await operations.GetOperationStatusAsync(operationId, cancellationToken).ConfigureAwait(false);
        return ToResult(operationId, status);
    }

    /// <summary>Lists one page of the caller's tracked backup operations, newest-first.</summary>
    public static async Task<McpBackupOperationPage> ListOperationsAsync(
        ILatticeBackupOperations operations,
        int pageSize,
        string? pageToken,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var page = await operations
            .ListOperationsAsync(
                new LatticeOperationListRequest { PageSize = pageSize, PageToken = NullIfEmpty(pageToken) },
                cancellationToken)
            .ConfigureAwait(false);

        var items = new McpBackupOperation[page.Operations.Count];
        for (var i = 0; i < items.Length; i++)
        {
            items[i] = BackupToolMappings.ToMcp(page.Operations[i]);
        }

        return new McpBackupOperationPage { Operations = items, NextPageToken = page.NextPageToken };
    }

    /// <summary>Requests cancellation of a tracked backup operation.</summary>
    public static async Task<McpBackupOperationResult> CancelOperationAsync(
        ILatticeBackupOperations operations,
        string operationId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(operations);
        var status = await operations.CancelOperationAsync(operationId, cancellationToken).ConfigureAwait(false);
        return ToResult(operationId, status);
    }
    /// <summary>Reverts a shadow-cutover restore reconstructed from the tool arguments.</summary>
    public static async Task<McpBackupRevertResult> RevertRestoreAsync(
        ILatticeBackupControl control,
        string backupId,
        string targetTreeId,
        string? mode,
        string operationId,
        IReadOnlyList<string>? manifestChain,
        long entriesApplied,
        string? shadowPhysicalTreeId,
        string? previousPhysicalTreeId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(control);
        var restore = new LatticeRestoreResult(
            backupId,
            targetTreeId,
            BackupToolMappings.ToRestoreMode(mode),
            operationId,
            manifestChain ?? Array.Empty<string>(),
            entriesApplied < 0 ? 0 : entriesApplied,
            shadowPhysicalTreeId,
            previousPhysicalTreeId);
        await control.RevertRestoreAsync(restore, cancellationToken).ConfigureAwait(false);
        return new McpBackupRevertResult
        {
            BackupId = backupId,
            TargetTreeId = targetTreeId,
            Reverted = true,
        };
    }

    /// <summary>Deletes a backup and its unshared artifacts.</summary>
    public static async Task<McpBackupDeleteResult> DeleteBackupAsync(
        ILatticeBackupControl control,
        string backupId,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(control);
        var deleted = await control.DeleteBackupAsync(backupId, cancellationToken).ConfigureAwait(false);
        return new McpBackupDeleteResult { BackupId = backupId, Deleted = deleted };
    }

    private static McpBackupOperationResult ToResult(string operationId, LatticeOperationStatus? status) =>
        new()
        {
            OperationId = operationId,
            Found = status is not null,
            Operation = status is null ? null : BackupToolMappings.ToMcp(status),
        };

    private static int NormalisePageSize(int pageSize) =>
        pageSize <= 0 ? LatticeBackupCaptureRequest.DefaultPageSize : pageSize;

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;
    private static void AppendChunk(List<byte> buffer, ReadOnlyMemory<byte> chunk)
    {
        if (chunk.IsEmpty)
        {
            return;
        }

        if (System.Runtime.InteropServices.MemoryMarshal.TryGetArray(chunk, out var segment)
            && segment.Array is not null)
        {
            buffer.AddRange(new ReadOnlySpan<byte>(segment.Array, segment.Offset, segment.Count));
        }
        else
        {
            buffer.AddRange(chunk.Span);
        }
    }
}
