using System.ComponentModel;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.Backup;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The backup tool module: an <see cref="ILatticeApiMcpToolGroup"/> for
/// <see cref="LatticeApiMcpGroup.Backup"/> whose tools are thin adapters over the
/// <see cref="ILatticeBackupControl"/> facade. The read-only inspect
/// tools (list, describe, inventory, scope status, artifact export) are always
/// contributed; the mutating control tools (capture, incremental capture,
/// restore, revert, delete) are contributed only when backup control is opted in
/// via <see cref="LatticeApiMcpOptions.EnableBackupControlTools"/> or
/// <c>AddBackupTools(enableControl: true)</c>. Every control tool is annotated
/// destructive and non-read-only.
/// </summary>
/// <remarks>
/// <para>
/// The tools are built <b>once</b> in the constructor and are stateless: each
/// resolves the facade from the tool invocation's request service provider and
/// stamps the caller credential - bridged from the request's authenticated
/// principal - onto the ambient <see cref="LatticeCredentialContext"/> for the
/// duration of the facade call, so the facade's own fail-closed backup access
/// gate resolves the caller's subject and authorizes every read and mutation.
/// The module adds no authorization path of its own.
/// </para>
/// <para>
/// The facade also exposes an unbounded <c>StreamBackupsAsync</c>; it is not
/// surfaced as its own tool. The MCP-appropriate mapping of that stream is the
/// cursor-paged <c>lattice_backup_list</c> tool, which never materialises the whole
/// catalog in a single call. The one genuinely streamed surface with no paged
/// facade equivalent, artifact export, is mapped by <c>lattice_backup_export_artifact</c>
/// to a bounded page plus a resume cursor.
/// </para>
/// </remarks>
internal sealed class BackupToolGroup : ILatticeApiMcpToolGroup
{
    /// <summary>
    /// Builds the backup tool set once from the resolved MCP options, including
    /// the mutating control tools only when backup control is opted in.
    /// </summary>
    /// <param name="options">The resolved MCP binding options. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <c>null</c>.</exception>
    public BackupToolGroup(IOptions<LatticeApiMcpOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Tools = BuildTools(options.Value.EnableBackupControlTools);
    }

    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.Backup;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools { get; }

    private static IReadOnlyList<McpServerTool> BuildTools(bool enableControl)
    {
        var tools = new List<McpServerTool>(enableControl ? 10 : 5)
        {
            CreateListTool(),
            CreateDescribeTool(),
            CreateInventoryTool(),
            CreateScopeStatusTool(),
            CreateExportArtifactTool(),
        };

        if (enableControl)
        {
            tools.Add(CreateCreateBackupTool());
            tools.Add(CreateCreateIncrementalTool());
            tools.Add(CreateRestoreTool());
            tools.Add(CreateRevertRestoreTool());
            tools.Add(CreateDeleteTool());
        }

        return tools;
    }

    private static McpServerTool CreateListTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                CancellationToken cancellationToken,
                [Description("Maximum manifests per page; <= 0 uses the server default.")] int pageSize = 0,
                [Description("Continuation cursor from a previous page's nextPageToken; null starts from the beginning.")] string? pageToken = null,
                [Description("When true, order newest-first by capture time instead of by backup id.")] bool orderByCreatedDescending = false) =>
            {
                using var scope = StampCredential(context.Services!);
                var control = context.Services!.GetRequiredService<ILatticeBackupControl>();
                return BackupToolInvocations.ListBackupsAsync(control, pageSize, pageToken, orderByCreatedDescending, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_backup_list",
                Title = "List backups",
                Description =
                    "Lists one cursor-paged page of the backup catalog the caller may read, ordered by backup id "
                    + "(or newest-first when requested). Pass the returned nextPageToken to continue. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateDescribeTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The content-addressed backup id to describe.")] string backupId,
                CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var control = context.Services!.GetRequiredService<ILatticeBackupControl>();
                return BackupToolInvocations.DescribeBackupAsync(control, backupId, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_backup_describe",
                Title = "Describe backup",
                Description =
                    "Describes a single backup and its base-first restore chain. Reports found=false when no backup "
                    + "with the id exists. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateInventoryTool()
        => McpServerTool.Create(
            (RequestContext<CallToolRequestParams> context, CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var control = context.Services!.GetRequiredService<ILatticeBackupControl>();
                return BackupToolInvocations.GetInventoryAsync(control, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_backup_inventory",
                Title = "Backup inventory",
                Description =
                    "Summarises the backups the caller may read: counts, byte totals, oldest/newest timestamps, and "
                    + "process-lifetime failure and reclaimed-bytes tallies. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateScopeStatusTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The captured tree id of the scope.")] string treeId,
                CancellationToken cancellationToken,
                [Description("Scope extent: WholeTree (default), Prefix, or Key.")] string? scopeKind = null,
                [Description("The exact key or key prefix for a Prefix/Key scope; null for WholeTree.")] string? keyOrPrefix = null) =>
            {
                using var scope = StampCredential(context.Services!);
                var control = context.Services!.GetRequiredService<ILatticeBackupControl>();
                return BackupToolInvocations.GetScopeStatusAsync(control, treeId, scopeKind, keyOrPrefix, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_backup_scope_status",
                Title = "Backup scope status",
                Description =
                    "Reads a single scope's schedule registration, last-run timestamps and outcome, and chain depth. "
                    + "Reports found=false when the scope has no schedule and no catalogued backup. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateExportArtifactTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The owning backup id.")] string backupId,
                [Description("The artifact id to export.")] string artifactId,
                CancellationToken cancellationToken,
                [Description("Resume cursor: the nextChunkOffset from a previous page; 0 starts from the beginning.")] int chunkOffset = 0,
                [Description("Byte budget for this page; <= 0 uses the server default (256 KiB), capped at 4 MiB.")] int maxBytes = 0) =>
            {
                using var scope = StampCredential(context.Services!);
                var control = context.Services!.GetRequiredService<ILatticeBackupControl>();
                return BackupToolInvocations.ExportArtifactAsync(control, backupId, artifactId, chunkOffset, maxBytes, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_backup_export_artifact",
                Title = "Export backup artifact",
                Description =
                    "Exports one bounded page of a backup artifact's bytes, base64-encoded, resuming from chunkOffset. "
                    + "Surfaces nextChunkOffset and endOfStream so a caller drains a large artifact without the server "
                    + "materialising it whole. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateCreateBackupTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The human-readable backup name recorded on the manifest.")] string name,
                [Description("The tree id to capture.")] string treeId,
                CancellationToken cancellationToken,
                [Description("Scope extent: WholeTree (default), Prefix, or Key.")] string? scopeKind = null,
                [Description("The exact key or key prefix for a Prefix/Key scope; null for WholeTree.")] string? keyOrPrefix = null,
                [Description("Raw-entry drain page size; <= 0 uses the server default.")] int pageSize = 0) =>
            {
                using var scope = StampCredential(context.Services!);
                var control = context.Services!.GetRequiredService<ILatticeBackupControl>();
                return BackupToolInvocations.CreateBackupAsync(control, name, treeId, scopeKind, keyOrPrefix, pageSize, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_backup_create",
                Title = "Create backup",
                Description =
                    "Captures a full backup of the requested scope. Mutating: subject to the fail-closed backup access "
                    + "gate. Requires backup control to be enabled on the server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateCreateIncrementalTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The human-readable backup name recorded on the manifest.")] string name,
                [Description("The tree id to capture.")] string treeId,
                [Description("The id of the base backup this increment is layered on.")] string baseBackupId,
                CancellationToken cancellationToken,
                [Description("Scope extent: WholeTree (default), Prefix, or Key.")] string? scopeKind = null,
                [Description("The exact key or key prefix for a Prefix/Key scope; null for WholeTree.")] string? keyOrPrefix = null,
                [Description("Raw-entry drain page size; <= 0 uses the server default.")] int pageSize = 0) =>
            {
                using var scope = StampCredential(context.Services!);
                var control = context.Services!.GetRequiredService<ILatticeBackupControl>();
                return BackupToolInvocations.CreateIncrementalBackupAsync(control, name, treeId, scopeKind, keyOrPrefix, baseBackupId, pageSize, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_backup_create_incremental",
                Title = "Create incremental backup",
                Description =
                    "Captures an incremental backup layered on a base backup. Mutating: subject to the fail-closed "
                    + "backup access gate. Requires backup control to be enabled on the server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateRestoreTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The content-addressed id of the backup to restore to.")] string backupId,
                CancellationToken cancellationToken,
                [Description("The tree to restore into; null restores into the captured tree.")] string? targetTreeId = null,
                [Description("Restore mode: InPlace (default) or ShadowCutover.")] string? mode = null,
                [Description("Idempotency key that makes a retried restore a no-op; null derives one.")] string? operationId = null) =>
            {
                using var scope = StampCredential(context.Services!);
                var control = context.Services!.GetRequiredService<ILatticeBackupControl>();
                return BackupToolInvocations.RestoreBackupAsync(control, backupId, targetTreeId, mode, operationId, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_backup_restore",
                Title = "Restore backup",
                Description =
                    "Restores a backup into its target tree, walking its base chain. Mutating: subject to the "
                    + "fail-closed backup access gate. For a ShadowCutover restore the result carries the physical "
                    + "tree ids needed to revert. Requires backup control to be enabled on the server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateRevertRestoreTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The backup id from the restore result being reverted.")] string backupId,
                [Description("The target tree id from the restore result being reverted.")] string targetTreeId,
                [Description("The operation id from the restore result being reverted.")] string operationId,
                CancellationToken cancellationToken,
                [Description("The restore mode from the restore result: InPlace or ShadowCutover.")] string? mode = null,
                [Description("The base-first replayed chain from the restore result.")] IReadOnlyList<string>? manifestChain = null,
                [Description("The entries-applied count from the restore result.")] long entriesApplied = 0,
                [Description("The shadow physical tree id from the restore result (shadow-cutover only).")] string? shadowPhysicalTreeId = null,
                [Description("The previous physical tree id from the restore result (shadow-cutover only).")] string? previousPhysicalTreeId = null) =>
            {
                using var scope = StampCredential(context.Services!);
                var control = context.Services!.GetRequiredService<ILatticeBackupControl>();
                return BackupToolInvocations.RevertRestoreAsync(
                    control, backupId, targetTreeId, mode, operationId, manifestChain, entriesApplied,
                    shadowPhysicalTreeId, previousPhysicalTreeId, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_backup_revert_restore",
                Title = "Revert restore",
                Description =
                    "Reverts a shadow-cutover restore, reconstructed from the fields of a prior lattice_backup_restore result. "
                    + "Idempotent. Mutating: subject to the fail-closed backup access gate. Requires backup control to "
                    + "be enabled on the server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool CreateDeleteTool()
        => McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The content-addressed backup id to delete.")] string backupId,
                CancellationToken cancellationToken) =>
            {
                using var scope = StampCredential(context.Services!);
                var control = context.Services!.GetRequiredService<ILatticeBackupControl>();
                return BackupToolInvocations.DeleteBackupAsync(control, backupId, cancellationToken);
            },
            new McpServerToolCreateOptions
            {
                Name = "lattice_backup_delete",
                Title = "Delete backup",
                Description =
                    "Deletes a backup and the artifacts it uniquely owns. Reports deleted=false when no backup with "
                    + "the id existed. Mutating: subject to the fail-closed backup access gate. Requires backup control "
                    + "to be enabled on the server.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static IDisposable StampCredential(IServiceProvider services)
    {
        var httpContext = services.GetService<IHttpContextAccessor>()?.HttpContext;
        if (httpContext is null)
        {
            return NullScope.Instance;
        }

        var credential = services.GetService<ILatticeApiMcpCredentialBridge>()?.Resolve(httpContext);
        // A null credential leaves the ambient context cleared (fail-closed): the
        // facade's access gate then denies the caller as anonymous.
        return LatticeCredentialContext.With(credential);
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose()
        {
        }
    }
}
