using System.ComponentModel;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The data tool module: an <see cref="ILatticeApiMcpToolGroup"/> serving
/// <see cref="LatticeApiMcpGroup.Data"/> with thin MCP tools over the internal
/// <see cref="ILatticeDataApi"/> facade. Point reads and a bounded range read are
/// always contributed; the mutating verbs (set, delete, single-tree atomic batch,
/// cross-tree atomic batch) are contributed only when the host opts writes in.
/// </summary>
/// <remarks>
/// <para>
/// The tools are built <b>once</b> in the constructor - they are stateless
/// adapters that resolve <see cref="ILatticeDataApi"/> from the tool invocation's
/// request service provider and delegate the translation to
/// <see cref="DataToolCore"/> - so the per-session discovery filter selects from a
/// prebuilt list and never re-materialises a tool per <c>tools/list</c>.
/// </para>
/// <para>
/// The module adds no authorization path of its own. Enforcement is inherited
/// fail-closed from the facade: a caller denied a key reads it as absent and
/// cannot write it. The read tools carry <c>readOnlyHint</c>; the write tools
/// carry <c>destructiveHint</c> and are non-<c>readOnlyHint</c>.
/// </para>
/// </remarks>
internal sealed class DataToolGroup : ILatticeApiMcpToolGroup
{
    private readonly IReadOnlyList<McpServerTool> _tools;

    /// <summary>
    /// Builds the data tool module. When <paramref name="enableWrites"/> is
    /// <see langword="false"/> (the default) only the two read tools are
    /// contributed; when <see langword="true"/> the four mutating tools are added.
    /// </summary>
    /// <param name="enableWrites">Whether the mutating data tools are contributed.</param>
    public DataToolGroup(bool enableWrites)
    {
        var tools = new List<McpServerTool>(enableWrites ? 6 : 2)
        {
            BuildGetTool(),
            BuildReadRangeTool(),
        };

        if (enableWrites)
        {
            tools.Add(BuildSetTool());
            tools.Add(BuildDeleteTool());
            tools.Add(BuildSetManyAtomicTool());
            tools.Add(BuildSetManyAtomicCrossTreeTool());
        }

        _tools = tools;
    }

    /// <inheritdoc />
    public LatticeApiMcpGroup Group => LatticeApiMcpGroup.Data;

    /// <inheritdoc />
    public IReadOnlyList<McpServerTool> Tools => _tools;

    private static ILatticeDataApi ResolveApi(RequestContext<CallToolRequestParams> context)
    {
        var services = context.Services
            ?? throw new InvalidOperationException(
                "The MCP request has no service provider; the data tools cannot resolve ILatticeDataApi.");
        return services.GetRequiredService<ILatticeDataApi>();
    }

    private static McpServerTool BuildGetTool()
        => McpServerTool.Create(
            GetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_get",
                Title = "Read a data entry",
                Description =
                    "Reads the value at a key on a tree. A key the caller may not read reports "
                    + "absent (found=false), never a value. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildReadRangeTool()
        => McpServerTool.Create(
            ReadRangeToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_range_read",
                Title = "Read a page of data entries",
                Description =
                    "Reads one page of a bounded, ascending key range on a tree, pruned to the "
                    + "caller's authorized subset. Pass the returned continuationToken back to resume "
                    + "paging. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildSetTool()
        => McpServerTool.Create(
            SetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_set",
                Title = "Write a data entry",
                Description =
                    "Writes a value at a key on a tree. Fails closed: a caller who may not write the "
                    + "key is denied and nothing is persisted. Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildDeleteTool()
        => McpServerTool.Create(
            DeleteToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_delete",
                Title = "Delete a data entry",
                Description =
                    "Deletes a key on a tree, returning whether a live value was removed. Fails "
                    + "closed: a caller who may not delete the key is denied. Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildSetManyAtomicTool()
        => McpServerTool.Create(
            SetManyAtomicToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_set_many_atomic",
                Title = "Commit an atomic batch on one tree",
                Description =
                    "Commits upserts and deletes all-or-nothing on one tree, keyed by operationId for "
                    + "idempotent retry. Every leg is authorized before any apply, so a single denied "
                    + "leg aborts the whole batch with nothing persisted. Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildSetManyAtomicCrossTreeTool()
        => McpServerTool.Create(
            SetManyAtomicCrossTreeToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_set_many_atomic_cross_tree",
                Title = "Commit an atomic batch across trees",
                Description =
                    "Commits per-tree upserts and deletes across every named tree all-or-nothing, "
                    + "keyed by operationId. Returns Committed when every tree's batch committed, or "
                    + "PreconditionFailed when a guard aborted it with nothing committed. Every leg is "
                    + "authorized before any apply. Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static Task<DataGetToolResult> GetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The entry key to read.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.GetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<DataRangePageToolResult> ReadRangeToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("Inclusive lower key bound, or null to start from the first key. Ignored when continuationToken is set.")]
        string? startInclusive,
        [Description("Exclusive upper key bound, or null to read to the last key. Ignored when continuationToken is set.")]
        string? endExclusive,
        [Description("Maximum entries on this page. Non-positive falls back to the configured default; larger values are clamped.")]
        int pageSize,
        [Description("Continuation token from a prior page, or null to open a fresh scan. When set, the range bounds are ignored.")]
        string? continuationToken,
        CancellationToken cancellationToken)
        => DataToolCore.ReadRangeAsync(
            ResolveApi(context), treeId, startInclusive, endExclusive, pageSize, continuationToken, cancellationToken);

    private static Task<DataSetToolResult> SetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The entry key to write.")] string key,
        [Description("The value bytes to store (base64-encoded).")] byte[] value,
        CancellationToken cancellationToken)
        => DataToolCore.SetAsync(ResolveApi(context), treeId, key, value, cancellationToken);

    private static Task<DataDeleteToolResult> DeleteToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The entry key to delete.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.DeleteAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<DataAtomicBatchToolResult> SetManyAtomicToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The key / value pairs to write atomically. May be empty when the batch is delete-only.")]
        IReadOnlyList<DataEntryDto>? upserts,
        [Description("The keys to delete atomically. May be empty when the batch is upsert-only.")]
        IReadOnlyList<string>? deleteKeys,
        [Description("Stable idempotency key. Must be non-empty and must not contain '/'.")] string operationId,
        CancellationToken cancellationToken)
        => DataToolCore.SetManyAtomicAsync(
            ResolveApi(context), treeId, upserts, deleteKeys, operationId, cancellationToken);

    private static Task<DataCrossTreeBatchToolResult> SetManyAtomicCrossTreeToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Per-tree slices to commit atomically. Tree ids must be distinct and non-empty.")]
        IReadOnlyList<DataTreeBatchDto> batches,
        [Description("Required cross-tree idempotency key. Must not contain '/'.")] string operationId,
        CancellationToken cancellationToken)
        => DataToolCore.SetManyAtomicCrossTreeAsync(ResolveApi(context), batches, operationId, cancellationToken);
}
