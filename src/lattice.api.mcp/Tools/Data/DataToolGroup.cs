using System.ComponentModel;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Api.Data;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The data tool module: an <see cref="ILatticeApiMcpToolGroup"/> serving
/// <see cref="LatticeApiMcpGroup.Data"/> with thin MCP tools over the internal
/// <see cref="ILatticeDataApi"/> facade. Point reads and a bounded range read are
/// always contributed; the mutating verbs (set, delete, range delete, non-atomic
/// bulk write, single-tree atomic batch, cross-tree atomic batch) are contributed
/// only when the host opts writes in.
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
internal sealed partial class DataToolGroup : ILatticeApiMcpToolGroup
{
    private readonly IReadOnlyList<McpServerTool> _tools;

    /// <summary>
    /// Builds the data tool module. When <paramref name="enableWrites"/> is
    /// <see langword="false"/> (the default) only the read tools are contributed
    /// (the two point / range reads plus the thirteen typed-CRDT reads); when
    /// <see langword="true"/> the mutating tools (six point / batch writes plus
    /// the thirteen typed-CRDT writes) are added.
    /// </summary>
    /// <param name="enableWrites">Whether the mutating data tools are contributed.</param>
    public DataToolGroup(bool enableWrites)
    {
        var tools = new List<McpServerTool>(enableWrites ? 34 : 15)
        {
            BuildGetTool(),
            BuildReadRangeTool(),
            BuildCounterGetTool(),
            BuildGCounterGetTool(),
            BuildSetGetTool(),
            BuildOrFlagGetTool(),
            BuildRwFlagGetTool(),
            BuildRwSetGetTool(),
            BuildVersionVectorGetTool(),
            BuildRegisterGetTool(),
            BuildMaxRegisterGetTool(),
            BuildMinRegisterGetTool(),
            BuildSequenceGetTool(),
            BuildMapGetTool(),
            BuildGSetGetTool(),
        };

        if (enableWrites)
        {
            tools.Add(BuildSetTool());
            tools.Add(BuildDeleteTool());
            tools.Add(BuildDeleteRangeTool());
            tools.Add(BuildSetManyTool());
            tools.Add(BuildSetManyAtomicTool());
            tools.Add(BuildSetManyAtomicCrossTreeTool());
            tools.Add(BuildCounterWriteTool());
            tools.Add(BuildGCounterWriteTool());
            tools.Add(BuildSetWriteTool());
            tools.Add(BuildOrFlagWriteTool());
            tools.Add(BuildRwFlagWriteTool());
            tools.Add(BuildRwSetWriteTool());
            tools.Add(BuildVersionVectorTickTool());
            tools.Add(BuildRegisterSetTool());
            tools.Add(BuildMaxRegisterSetTool());
            tools.Add(BuildMinRegisterSetTool());
            tools.Add(BuildSequenceWriteTool());
            tools.Add(BuildMapWriteTool());
            tools.Add(BuildGSetWriteTool());
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
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read a data entry",
                Description =
                    "Reads the value at a key on a tree, returned base64-encoded in the result. "
                    + "A routine miss is never a fault: an unknown tree, a missing key, or a key the "
                    + "caller may not read all report found=false with no value. The result reports the "
                    + "entry's per-key mergeMode (e.g. PnCounter, OrSet; null for a plain "
                    + "last-writer-wins value) and always sets raw=true: the data plane returns the raw "
                    + "stored bytes and never decodes a typed CRDT, so when mergeMode is non-null the "
                    + "value is the CRDT's internal serialization - use the matching typed getter "
                    + "(lattice_data_pncounter_get, lattice_data_orset_get, ...) or the state API's "
                    + "scan_entries/get_entry for the logical value. Caller errors (for "
                    + "example a null tree id or key) surface as an invalid-argument error. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildReadRangeTool()
        => McpServerTool.Create(
            ReadRangeToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_read_range",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read a page of data entries",
                Description =
                    "Reads one page of a bounded, ascending key range on a tree, pruned to the "
                    + "caller's authorized subset, with each value base64-encoded. Pass the returned "
                    + "continuationToken back to resume paging; a null token means the range is "
                    + "drained. The continuationToken is a single-use forward-only cursor: each token "
                    + "advances the scan and is consumed by the next page, so a given token cannot be "
                    + "replayed to re-fetch a page already read - page forward and keep only the latest "
                    + "token. An unknown tree returns an empty page, not a fault. An invalid, expired, "
                    + "or already-consumed continuation token is a caller error (invalid-argument). "
                    + "Every returned entry is raw stored bytes (raw=true) and is not CRDT-decoded; the "
                    + "bulk range path does not resolve per-key mergeMode - use lattice_data_get or the "
                    + "state API's scan_entries when you need each entry's mergeMode or a decoded value. "
                    + "Read-only.",
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
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Write a data entry",
                Description =
                    "Writes a base64-encoded value at a key on a tree. Fails closed: a caller who may "
                    + "not write the key is denied and nothing is persisted. Caller errors - a value "
                    + "that is not valid base64, or a null tree id or key - surface as an "
                    + "invalid-argument error, not a server fault. Destructive.",
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
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Delete a data entry",
                Description =
                    "Deletes a key on a tree, returning deleted=true when a live value was removed and "
                    + "deleted=false when the key was already absent (an unknown tree also reports "
                    + "deleted=false). Fails closed: a caller who may not delete the key is denied. "
                    + "Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildDeleteRangeTool()
        => McpServerTool.Create(
            DeleteRangeToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_delete_range",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Delete a range of data entries",
                Description =
                    "Deletes every key the caller is authorized to delete in the half-open range "
                    + "[startInclusive, endExclusive) on a tree, returning deletedCount - the total "
                    + "tombstoned. Both bounds are required. The delete drains a durable cursor to "
                    + "completion in bounded batches, transparently reopening on a transient enumerator "
                    + "loss so a large range completes rather than aborting part-way. Fails closed: a "
                    + "range delete is all-or-nothing across its span, so a caller who may not delete "
                    + "the whole range is denied and nothing is removed. An unknown tree reports "
                    + "deletedCount=0. Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildSetManyTool()
        => McpServerTool.Create(
            SetManyToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_set_many",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Write many entries non-atomically",
                Description =
                    "Writes many base64-encoded key / value pairs on one tree in a single call, the "
                    + "cheap bulk-load path. This is NOT atomic and NOT idempotent: the batch fans out "
                    + "per shard and each slice commits independently, so a mid-flight failure can leave "
                    + "some keys written and others not, with no rollback and no operationId to make a "
                    + "retry a safe no-op. Use lattice_data_set_many_atomic when you need all-or-nothing "
                    + "semantics. Authorization is enforced per key exactly as the atomic batch: a caller "
                    + "who may not write any targeted key is denied and that key is not written. An empty "
                    + "upserts list is a no-op; a value that is not valid base64, or a null tree id, is a "
                    + "caller error (invalid-argument). Destructive.",
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
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Commit an atomic batch on one tree",
                Description =
                    "Commits upserts and deletes all-or-nothing on one tree, keyed by operationId for "
                    + "idempotent retry. Every leg is authorized before any apply, so a single denied "
                    + "leg aborts the whole batch with nothing persisted. Reusing an operationId is a "
                    + "no-op retry only when the batch presents the exact same set of keys; reusing it "
                    + "with a different key set is a caller error (failed-precondition), rejected with "
                    + "nothing applied. A malformed batch (duplicate keys, or an empty operationId or "
                    + "one containing '/') is an invalid-argument caller error. Destructive.",
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
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Commit an atomic batch across trees",
                Description =
                    "Commits per-tree upserts and deletes across every named tree all-or-nothing, "
                    + "keyed by operationId. Returns Committed when every tree's batch committed, or "
                    + "PreconditionFailed when a guard aborted it with nothing committed. Every leg is "
                    + "authorized before any apply. Reusing an operationId is a no-op retry only when "
                    + "it presents the exact same set of trees and keys; reusing it with a different "
                    + "tree or key set is a caller error (failed-precondition), rejected with nothing "
                    + "applied. A malformed batch (duplicate or non-distinct trees or keys, or an "
                    + "operationId containing '/') is an invalid-argument caller error. Destructive.",
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
        string? startInclusive = null,
        [Description("Exclusive upper key bound, or null to read to the last key. Ignored when continuationToken is set.")]
        string? endExclusive = null,
        [Description("Maximum entries on this page. Non-positive falls back to the configured default; larger values are clamped.")]
        int pageSize = 0,
        [Description("Continuation token from a prior page, or null to open a fresh scan. Single-use and forward-only: consumed by the page it fetches and cannot be replayed. When set, the range bounds are ignored.")]
        string? continuationToken = null,
        CancellationToken cancellationToken = default)
        => DataToolCore.ReadRangeAsync(
            ResolveApi(context), treeId, startInclusive, endExclusive, pageSize, continuationToken, cancellationToken);

    private static Task<DataSetToolResult> SetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The entry key to write.")] string key,
        [Description("The value to store, as a base64-encoded byte string (required). Invalid base64 is rejected as a caller error.")] string value,
        CancellationToken cancellationToken)
        => DataToolCore.SetAsync(ResolveApi(context), treeId, key, DecodeBase64Value(value), cancellationToken);

    /// <summary>
    /// Decodes the caller-supplied base64 <c>value</c> argument of
    /// <c>lattice_data_set</c> into the raw bytes the facade stores. A missing or
    /// non-base64 value is a caller error, surfaced as a clean, self-contained
    /// <see cref="McpException"/> rather than the raw JSON deserialization fault
    /// that a <c>byte[]</c> tool parameter would leak.
    /// </summary>
    /// <param name="value">The base64 text supplied for the <c>value</c> argument.</param>
    /// <returns>The decoded value bytes.</returns>
    /// <exception cref="McpException">The value is null or is not valid base64.</exception>
    internal static byte[] DecodeBase64Value(string value)
    {
        if (value is null)
        {
            throw new McpException(
                "The 'value' parameter is required and must be a base64-encoded byte string.");
        }

        try
        {
            return Convert.FromBase64String(value);
        }
        catch (FormatException)
        {
            throw new McpException(
                "The 'value' parameter must be base64-encoded; the supplied text is not valid base64.");
        }
    }

    private static Task<DataDeleteToolResult> DeleteToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The entry key to delete.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.DeleteAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<DataRangeDeleteToolResult> DeleteRangeToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("Inclusive lower key bound. Required.")] string startInclusive,
        [Description("Exclusive upper key bound. Required.")] string endExclusive,
        CancellationToken cancellationToken = default)
        => DataToolCore.DeleteRangeAsync(
            ResolveApi(context), treeId, startInclusive, endExclusive, cancellationToken);

    private static Task<DataSetManyToolResult> SetManyToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The key / value pairs to write non-atomically. Each value is a base64-encoded byte string. An empty list is a no-op.")]
        IReadOnlyList<DataEntryDto>? upserts,
        CancellationToken cancellationToken)
        => DataToolCore.SetManyAsync(ResolveApi(context), treeId, upserts, cancellationToken);

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
