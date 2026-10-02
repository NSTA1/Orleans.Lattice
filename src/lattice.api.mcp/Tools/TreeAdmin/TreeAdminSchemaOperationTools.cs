using System.ComponentModel;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The accept-then-poll schema remediation and migration tools of the
/// tree-administration group (#4209): a start tool per long schema verb on
/// <see cref="ILatticeSchemaOperations"/>, and the status, list and cancel tools for
/// the operations they start. Each tool resolves the facade from the request service
/// provider at call time, so a host that does not register it fails the call with a
/// clear message. The facade owns every authorization decision; these tools add none.
/// </summary>
/// <remarks>
/// <para>
/// The status and list tools are reads and are always contributed. The start tools,
/// the cancel tool and the three deprecated aliases
/// (<c>lattice_treeadmin_schema_remediate</c>,
/// <c>lattice_treeadmin_schema_migrate_to_target</c> and
/// <c>lattice_treeadmin_schema_advance_and_migrate</c>) change a tree's schema or
/// stop that change, so they are contributed only when schema control is opted in.
/// </para>
/// <para>
/// The aliases keep their old names and arguments for one release. They now start
/// the operation and return its handle at once rather than blocking until the
/// terminal report, and they are removed in the next major version.
/// </para>
/// </remarks>
internal static class TreeAdminSchemaOperationTools
{
    /// <summary>The remediation start tool.</summary>
    public const string RemediationStartToolName = "lattice_treeadmin_schema_remediation_start";

    /// <summary>The eager-migration start tool.</summary>
    public const string MigrationStartToolName = "lattice_treeadmin_schema_migration_start";

    /// <summary>The advance-and-migrate start tool.</summary>
    public const string AdvanceAndMigrateStartToolName = "lattice_treeadmin_schema_advance_and_migrate_start";

    /// <summary>The schema operation status tool.</summary>
    public const string StatusToolName = "lattice_treeadmin_schema_operation_status";

    /// <summary>The schema operation list tool.</summary>
    public const string ListToolName = "lattice_treeadmin_schema_operation_list";

    /// <summary>The schema operation cancel tool.</summary>
    public const string CancelToolName = "lattice_treeadmin_schema_operation_cancel";

    /// <summary>The deprecated alias of <see cref="RemediationStartToolName"/>.</summary>
    public const string RemediateAliasToolName = "lattice_treeadmin_schema_remediate";

    /// <summary>The deprecated alias of <see cref="MigrationStartToolName"/>.</summary>
    public const string MigrateAliasToolName = "lattice_treeadmin_schema_migrate_to_target";

    /// <summary>The deprecated alias of <see cref="AdvanceAndMigrateStartToolName"/>.</summary>
    public const string AdvanceAndMigrateAliasToolName = "lattice_treeadmin_schema_advance_and_migrate";

    private const string PageSizeDescription = "Maximum operations per page; <= 0 uses the server default (50, at most 500).";
    private const string PageTokenDescription = "Continuation cursor from a previous page's nextPageToken; null starts at the newest.";
    private const string OperationIdDescription =
        "Optional idempotency id (1 to 128 ASCII letters, digits, '-', '_' or '.'). Starting again with an id in use "
        + "returns the existing operation with created=false; omit to generate one.";
    private const string TransformDescription =
        "The per-value remediation transform IR that rewrites each stored value (for example a Passthrough pipeline of "
        + "SetMember / DropMember / RenameMember operations).";
    private const string TargetPolicyDescription =
        "The enforcement policy the transformed values must satisfy for the remediation to cut over. The remediation "
        + "fails on the first value the transform cannot make compliant.";
    private const string NewTargetVersionDescription =
        "The new target version. Must be strictly greater than the tree's current target.";
    private const string DeprecatedAliasNote =
        " Deprecated alias kept for one release and removed in the next major version: it now starts the operation and "
        + "returns a handle at once rather than blocking until the terminal report.";

    /// <summary>Builds the read-only status and list tools.</summary>
    /// <returns>The tools.</returns>
    public static IReadOnlyList<McpServerTool> CreateReadTools() =>
    [
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The operation id from a start tool's handle.")] string operationId,
                CancellationToken cancellationToken) =>
                LatticeOperationToolInvocations.GetStatusAsync(Operations(context), operationId, cancellationToken),
            Options(
                StatusToolName,
                "Schema operation status",
                "Reads a tracked schema remediation, migration or advance-and-migrate: its state (Queued, Running, "
                + "Succeeded, Failed or Cancelled), phase, progress as values processed of total (the total is null while "
                + "unknown) and, once finished, the result map. A remediation that stopped at a value it cannot remediate "
                + "reads Failed and names the value in its result. Reports found=false for an unknown id or one the caller "
                + "may not see. Read-only.",
                readOnly: true,
                destructive: false)),
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                CancellationToken cancellationToken,
                [Description(PageSizeDescription)] int pageSize = 0,
                [Description(PageTokenDescription)] string? pageToken = null) =>
                LatticeOperationToolInvocations.ListAsync(Operations(context), pageSize, pageToken, cancellationToken),
            Options(
                ListToolName,
                "List schema operations",
                "Lists one page of the caller's tracked schema remediations, migrations and advance-and-migrates over "
                + "trees it may read, newest-first, including finished ones until their retention window lapses. "
                + "Read-only.",
                readOnly: true,
                destructive: false)),
    ];

    /// <summary>Builds the start, cancel and deprecated alias tools.</summary>
    /// <returns>The tools.</returns>
    public static IReadOnlyList<McpServerTool> CreateControlTools() =>
    [
        RemediationTool(
            RemediationStartToolName,
            "Start a schema remediation",
            "Starts a tracked remediation that rewrites every stored value of a tree through a value transform and cuts "
            + "the tree over once the rewritten values satisfy a target policy, and returns an operation handle at once. "
            + "Poll " + StatusToolName + " for its phase (dry run, build, cutover) and values processed. It fails without "
            + "cutover on the first value the transform cannot make compliant. Schema-admin-gated and destructive."),
        MigrationTool(
            MigrationStartToolName,
            "Start a schema migration",
            "Starts a tracked eager migration that re-stamps every existing value of a tree to the tree's current target "
            + "version, and returns an operation handle at once; a tree already fully migrated finishes at once. Poll "
            + StatusToolName + ". Requires schema versioning on the server. Schema-admin-gated and destructive."),
        AdvanceAndMigrateTool(
            AdvanceAndMigrateStartToolName,
            "Start a schema advance and migration",
            "Starts a tracked advance of a tree's target schema version to a strictly greater value, then an eager "
            + "migration to it, and returns an operation handle at once. A target that does not advance fails the "
            + "operation in its first phase. Poll " + StatusToolName + ". Requires schema versioning on the server. "
            + "Schema-admin-gated and destructive."),
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The operation id to cancel.")] string operationId,
                CancellationToken cancellationToken) =>
                LatticeOperationToolInvocations.CancelAsync(Operations(context), operationId, cancellationToken),
            Options(
                CancelToolName,
                "Cancel a schema operation",
                "Requests cancellation of a tracked schema remediation, migration or advance-and-migrate. It takes effect "
                + "only before cutover: an operation already cutting over runs on to completion. Requires schema-management "
                + "authority over the tree. Reports found=false when the operation is not visible.",
                readOnly: false,
                destructive: false)),
        RemediationTool(
            RemediateAliasToolName,
            "Remediate a tree's values (deprecated alias)",
            "Use " + RemediationStartToolName + ". Starts a tracked remediation and returns an operation handle; poll "
            + StatusToolName + "." + DeprecatedAliasNote),
        MigrationTool(
            MigrateAliasToolName,
            "Migrate a tree's values (deprecated alias)",
            "Use " + MigrationStartToolName + ". Starts a tracked eager migration and returns an operation handle; poll "
            + StatusToolName + "." + DeprecatedAliasNote),
        AdvanceAndMigrateTool(
            AdvanceAndMigrateAliasToolName,
            "Advance and migrate a tree's schema (deprecated alias)",
            "Use " + AdvanceAndMigrateStartToolName + ". Starts a tracked advance and eager migration and returns an "
            + "operation handle; poll " + StatusToolName + "." + DeprecatedAliasNote),
    ];

    private static McpServerTool RemediationTool(string name, string title, string description) =>
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The governed tree. Must not be null, empty, or a reserved system tree id.")] string treeId,
                [Description(TransformDescription)] LatticeValueTransform transform,
                [Description(TargetPolicyDescription)] LatticeSchemaPolicy targetPolicy,
                CancellationToken cancellationToken,
                [Description(OperationIdDescription)] string? operationId = null) =>
                StartRemediationAsync(Operations(context), treeId, transform, targetPolicy, operationId, cancellationToken),
            Options(name, title, description, readOnly: false, destructive: true));

    private static McpServerTool MigrationTool(string name, string title, string description) =>
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The governed tree. Must not be null, empty, or a reserved system tree id.")] string treeId,
                CancellationToken cancellationToken,
                [Description(OperationIdDescription)] string? operationId = null) =>
                StartMigrationAsync(Operations(context), treeId, operationId, cancellationToken),
            Options(name, title, description, readOnly: false, destructive: true));

    private static McpServerTool AdvanceAndMigrateTool(string name, string title, string description) =>
        McpServerTool.Create(
            (
                RequestContext<CallToolRequestParams> context,
                [Description("The governed tree. Must not be null, empty, or a reserved system tree id.")] string treeId,
                [Description(NewTargetVersionDescription)] uint newTargetVersion,
                CancellationToken cancellationToken,
                [Description(OperationIdDescription)] string? operationId = null) =>
                StartAdvanceAndMigrateAsync(Operations(context), treeId, newTargetVersion, operationId, cancellationToken),
            Options(name, title, description, readOnly: false, destructive: true));

    private static async Task<McpLatticeOperationHandle> StartRemediationAsync(
        ILatticeSchemaOperations operations,
        string treeId,
        LatticeValueTransform transform,
        LatticeSchemaPolicy targetPolicy,
        string? operationId,
        CancellationToken cancellationToken)
    {
        var handle = await operations
            .StartRemediationAsync(treeId, transform, targetPolicy, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false);
        return LatticeOperationToolInvocations.ToMcp(handle, StatusToolName);
    }

    private static async Task<McpLatticeOperationHandle> StartMigrationAsync(
        ILatticeSchemaOperations operations,
        string treeId,
        string? operationId,
        CancellationToken cancellationToken)
    {
        var handle = await operations
            .StartMigrationAsync(treeId, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false);
        return LatticeOperationToolInvocations.ToMcp(handle, StatusToolName);
    }

    private static async Task<McpLatticeOperationHandle> StartAdvanceAndMigrateAsync(
        ILatticeSchemaOperations operations,
        string treeId,
        uint newTargetVersion,
        string? operationId,
        CancellationToken cancellationToken)
    {
        var handle = await operations
            .StartAdvanceAndMigrateAsync(treeId, newTargetVersion, NullIfEmpty(operationId), cancellationToken)
            .ConfigureAwait(false);
        return LatticeOperationToolInvocations.ToMcp(handle, StatusToolName);
    }

    private static ILatticeSchemaOperations Operations(RequestContext<CallToolRequestParams> context) =>
        context.Services?.GetService<ILatticeSchemaOperations>()
        ?? throw new InvalidOperationException(
            "This server registers no ILatticeSchemaOperations, so the schema remediation and migration tools are unavailable.");

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;

    private static McpServerToolCreateOptions Options(string name, string title, string description, bool readOnly, bool destructive) =>
        new()
        {
            Name = name,
            Title = title,
            Description = description,
            SerializerOptions = LatticeApiMcpToolSerialization.Options,
            ReadOnly = readOnly,
            Destructive = destructive,
            UseStructuredContent = true,
        };
}
