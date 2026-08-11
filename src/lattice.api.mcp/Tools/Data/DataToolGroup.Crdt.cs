using System.ComponentModel;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Typed-CRDT half of the data tool module: the strongly-typed conflict-free
/// replicated data-type tools over <see cref="Orleans.Lattice.Api.Data.ILatticeDataApi"/>.
/// Each CRDT primitive contributes a read tool (always) and a write tool
/// (write-enabled hosts only). Unlike a plain <c>lattice_data_set</c> - which is
/// last-writer-wins and silently drops one side of a concurrent write - these
/// tools converge concurrent writers by construction, each primitive by its own
/// merge rule. Every element / value byte string is carried base64-encoded in the
/// tool's JSON. The <c>replicaId</c> argument names the writer: give each
/// independent writer (cluster, silo, or logical actor) a stable, distinct id so
/// concurrent edits are attributed to different causal lineages.
/// </summary>
internal sealed partial class DataToolGroup
{
    private const string ReplicaIdDescription =
        "Stable, distinct id naming the writer (cluster, silo, or logical actor). Concurrent edits from "
        + "different replica ids are attributed to different causal lineages, which is how the merge resolves them.";

    private const string CrdtModeNote =
        " On a tree enrolled in cross-cluster replication the key's declared merge mode must match this CRDT; "
        + "a mismatched write is rejected. Mix modes freely on a local (non-replicated) tree.";

    private static McpServerTool BuildCounterWriteTool()
        => McpServerTool.Create(
            CounterWriteToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_pncounter",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Increment or decrement a PN-counter",
                Description =
                    "Applies an increment or decrement to a PN-Counter at a key. A PN-Counter converges by "
                    + "per-replica sum: every writer's increments and decrements are tracked independently and "
                    + "summed, so concurrent updates from many clusters all count (likes, stock, quotas). Pass a "
                    + "non-negative amount and choose increment or decrement; the replicaId names the writer whose "
                    + "running tally is adjusted. Fails closed: a caller who may not write the key is denied."
                    + CrdtModeNote + " Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildCounterGetTool()
        => McpServerTool.Create(
            CounterGetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_pncounter_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read a PN-counter total",
                Description =
                    "Reads the converged total of a PN-Counter: the sum across every replica's increments and "
                    + "decrements. An absent or unreadable key reads as zero, never a fault. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildGCounterWriteTool()
        => McpServerTool.Create(
            GCounterWriteToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_gcounter",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Increment a G-counter",
                Description =
                    "Applies an increment to a grow-only G-Counter at a key. A G-Counter only ever increases and "
                    + "converges by per-replica sum: every writer's increments are tracked independently and summed, "
                    + "so concurrent increments from many clusters all count (monotone metrics, event / sequence "
                    + "counters, quota consumption). Pass a non-negative amount; the replicaId names the writer whose "
                    + "running tally is advanced. Unlike a PN-Counter it cannot decrement. Fails closed: a caller who "
                    + "may not write the key is denied." + CrdtModeNote + " Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildGCounterGetTool()
        => McpServerTool.Create(
            GCounterGetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_gcounter_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read a G-counter total",
                Description =
                    "Reads the converged total of a grow-only G-Counter: the sum across every replica's increments. "
                    + "An absent or unreadable key reads as zero, never a fault. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildSetWriteTool()
        => McpServerTool.Create(
            SetWriteToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_orset",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Add or remove an OR-Set element",
                Description =
                    "Adds or removes a base64-encoded element in an OR-Set at a key. An OR-Set converges by "
                    + "add-wins observed-remove: a remove only cancels the adds the writer has actually seen, so a "
                    + "concurrent add and remove of the same element keeps the element. Choose add (needs replicaId) "
                    + "or remove; supply the element bytes as base64. Fails closed: a caller who may not write the "
                    + "key is denied. Invalid base64 is a caller error." + CrdtModeNote + " Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildSetGetTool()
        => McpServerTool.Create(
            SetGetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_orset_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read OR-Set members",
                Description =
                    "Reads the current members of an OR-Set as an unordered list of base64-encoded element bytes. "
                    + "An absent or unreadable key yields an empty list, never a fault. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildOrFlagWriteTool()
        => McpServerTool.Create(
            OrFlagWriteToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_orflag",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Enable or disable an OR-Flag",
                Description =
                    "Enables or disables an OR-Flag at a key. An OR-Flag is a boolean presence bit that converges "
                    + "enable-wins: a concurrent enable beats a disable, so the flag ends on. Choose enable or "
                    + "disable; the replicaId names the writer. Fails closed: a caller who may not write the key is "
                    + "denied." + CrdtModeNote + " Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildOrFlagGetTool()
        => McpServerTool.Create(
            OrFlagGetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_orflag_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read an OR-Flag",
                Description =
                    "Reads the converged state of an OR-Flag (enable-wins). An absent or unreadable key reads as "
                    + "disabled (false), never a fault. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildRwFlagWriteTool()
        => McpServerTool.Create(
            RwFlagWriteToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_rwflag",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Enable or disable an RW-Flag",
                Description =
                    "Enables or disables an RW-Flag at a key. An RW-Flag is a boolean presence bit that converges "
                    + "disable-wins: a concurrent disable beats an enable, so a removal wins the tie (revocation "
                    + "lists, blocklists). Both enable and disable name the writer via replicaId. Fails closed: a "
                    + "caller who may not write the key is denied." + CrdtModeNote + " Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildRwFlagGetTool()
        => McpServerTool.Create(
            RwFlagGetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_rwflag_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read an RW-Flag",
                Description =
                    "Reads the converged state of an RW-Flag (disable-wins). An absent or unreadable key reads as "
                    + "disabled (false), never a fault. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildRwSetWriteTool()
        => McpServerTool.Create(
            RwSetWriteToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_rwset",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Add or remove an RW-Set element",
                Description =
                    "Adds or removes a base64-encoded element in an RW-Set (remove-wins observed-remove set) at a "
                    + "key. An RW-Set converges remove-wins: a concurrent add and remove of the same element keeps "
                    + "the element out, so a revoke is never silently resurrected by a concurrent re-add (membership "
                    + "revocation lists, blocklists). Choose add or remove; both name the writer via replicaId. "
                    + "Supply the element bytes as base64. Fails closed: a caller who may not write the key is "
                    + "denied. Invalid base64 is a caller error." + CrdtModeNote + " Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildRwSetGetTool()
        => McpServerTool.Create(
            RwSetGetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_rwset_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read RW-Set members",
                Description =
                    "Reads the current members of an RW-Set (remove-wins) as an unordered list of base64-encoded "
                    + "element bytes. An absent or unreadable key yields an empty list, never a fault. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildVersionVectorTickTool()
        => McpServerTool.Create(
            VersionVectorTickToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_version_vector_tick",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Tick a version vector",
                Description =
                    "Advances a Version Vector's clock for one replica. A Version Vector tracks causal history - "
                    + "who has seen what - and converges by per-replica max, so it detects concurrency between "
                    + "writers. Each tick bumps the replicaId's entry. Fails closed: a caller who may not write the "
                    + "key is denied." + CrdtModeNote + " Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildVersionVectorGetTool()
        => McpServerTool.Create(
            VersionVectorGetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_version_vector_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read a version vector",
                Description =
                    "Reads a Version Vector as a map of replica id to that replica's clock, each clock formatted "
                    + "\"wallClockTicks:counter\". An absent or unreadable key yields an empty map, never a fault. "
                    + "Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildRegisterSetTool()
        => McpServerTool.Create(
            RegisterSetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_mvregister_set",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Set an MV-Register value",
                Description =
                    "Sets a single base64-encoded value on an MV-Register at a key. An MV-Register holds one value "
                    + "but converges by keeping concurrent values: when two replicas set it at the same time both "
                    + "survive, so a later read can surface every concurrent write instead of silently dropping one. "
                    + "The replicaId names the writer. Fails closed: a caller who may not write the key is denied. "
                    + "Invalid base64 is a caller error." + CrdtModeNote + " Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildRegisterGetTool()
        => McpServerTool.Create(
            RegisterGetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_mvregister_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read an MV-Register",
                Description =
                    "Reads an MV-Register's current values as a list of base64-encoded byte strings: one value "
                    + "normally, more than one only while concurrent writes are unresolved (the application picks). "
                    + "An absent or unreadable key yields an empty list, never a fault. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildMaxRegisterSetTool()
        => McpServerTool.Create(
            MaxRegisterSetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_maxregister_set",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Advance a max-register",
                Description =
                    "Advances a monotone Max-Register at a key towards a base64-encoded value - the high-water-mark "
                    + "primitive that keeps the greatest value ever seen. Candidates are ordered by their raw value "
                    + "bytes (unsigned lexicographic), so a write that is not strictly greater than the current value "
                    + "is a durable no-op; concurrent writers from many clusters converge on the single greatest value. "
                    + "Fails closed: a caller who may not write the key is denied. Invalid base64 is a caller error."
                    + CrdtModeNote + " Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildMaxRegisterGetTool()
        => McpServerTool.Create(
            MaxRegisterGetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_maxregister_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read a max-register",
                Description =
                    "Reads a monotone Max-Register's current value as a list holding zero or one base64-encoded byte "
                    + "string (empty when the register has never been written). An absent or unreadable key yields an "
                    + "empty list, never a fault. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildMinRegisterSetTool()
        => McpServerTool.Create(
            MinRegisterSetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_minregister_set",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Advance a min-register",
                Description =
                    "Advances a monotone Min-Register at a key towards a base64-encoded value - the low-water-mark "
                    + "primitive that keeps the smallest value ever seen. Candidates are ordered by their raw value "
                    + "bytes (unsigned lexicographic), so a write that is not strictly smaller than the current value "
                    + "is a durable no-op; concurrent writers from many clusters converge on the single smallest value. "
                    + "Fails closed: a caller who may not write the key is denied. Invalid base64 is a caller error."
                    + CrdtModeNote + " Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildMinRegisterGetTool()
        => McpServerTool.Create(
            MinRegisterGetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_minregister_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read a min-register",
                Description =
                    "Reads a monotone Min-Register's current value as a list holding zero or one base64-encoded byte "
                    + "string (empty when the register has never been written). An absent or unreadable key yields an "
                    + "empty list, never a fault. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildSequenceWriteTool()
        => McpServerTool.Create(
            SequenceWriteToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_sequence",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Insert into or remove from a sequence",
                Description =
                    "Inserts a base64-encoded value at a position, or removes the element at a position, in a "
                    + "Sequence (an RGA ordered list). A Sequence converges by ordered insert / tombstone, so "
                    + "collaborative edits to an ordered list or text buffer interleave deterministically without "
                    + "losing concurrent inserts. Choose insertAt (needs index, replicaId, and value) or removeAt "
                    + "(needs index); the index is the position in the current visible order. Fails closed: a caller "
                    + "who may not write the key is denied. Invalid base64 is a caller error." + CrdtModeNote
                    + " Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildSequenceGetTool()
        => McpServerTool.Create(
            SequenceGetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_sequence_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read a sequence",
                Description =
                    "Reads a Sequence in visible order as a list of base64-encoded element bytes (tombstoned "
                    + "positions are omitted). An absent or unreadable key yields an empty list, never a fault. "
                    + "Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildMapWriteTool()
        => McpServerTool.Create(
            MapWriteToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_ormap",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Set or remove an OR-Map field",
                Description =
                    "Puts a base64-encoded value under a field, or removes a field, in an OR-Map at a key. An OR-Map "
                    + "is a dictionary that converges by recursive per-key merge: each field is itself a CRDT (here "
                    + "a keep-concurrent-values register), so concurrent writes to different fields both survive and "
                    + "concurrent writes to one field are kept as concurrent values. Choose set (needs field, "
                    + "replicaId, value) or remove (needs field). LIMITATION: an OR-Map requires a host-registered "
                    + "CrdtShape (declared at silo startup via AddOrMapShape<TKey,TValue>); there is no MCP tool to "
                    + "register a shape, so OR-Map writes only work on trees pre-provisioned host-side. A write to a "
                    + "tree with no registered shape returns a clean FailedPrecondition caller error (not a server "
                    + "fault) naming the missing registration - note this is asymmetric with lattice_data_ormap_get, "
                    + "which returns an empty map rather than erroring on the same unprovisioned tree. Fails closed: a "
                    + "caller who may not write the key is denied. Invalid base64 is a "
                    + "caller error." + CrdtModeNote + " Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildMapGetTool()
        => McpServerTool.Create(
            MapGetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_ormap_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read an OR-Map",
                Description =
                    "Reads an OR-Map's live fields, each mapped to its current concurrent value bytes (one "
                    + "normally, more than one only while a field's concurrent writes are unresolved), each value "
                    + "base64-encoded. Tombstoned and absent fields are omitted; an absent or unreadable key yields "
                    + "an empty map, never a fault. Note the asymmetry with lattice_data_ormap (write): on a tree "
                    + "with no host-registered OR-Map shape this read still returns an empty map, whereas a write "
                    + "returns a FailedPrecondition caller error - so an empty read does not imply the map is "
                    + "writable via MCP. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildGSetWriteTool()
        => McpServerTool.Create(
            GSetWriteToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_gset",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Add a G-Set element",
                Description =
                    "Adds a base64-encoded element to a grow-only (G) set at a key. A G-Set converges by set "
                    + "union: every add survives and the add is idempotent, so concurrent adds from many writers "
                    + "all count and re-adding an element is a harmless no-op. It needs no replicaId - a grow-only "
                    + "set carries no causal context. There is no remove operation by design; use lattice_data_orset "
                    + "when elements must ever be removed. Supply the element bytes as base64. Fails closed: a caller "
                    + "who may not write the key is denied. Invalid base64 is a caller error." + CrdtModeNote
                    + " Destructive.",
                ReadOnly = false,
                Destructive = true,
                UseStructuredContent = true,
            });

    private static McpServerTool BuildGSetGetTool()
        => McpServerTool.Create(
            GSetGetToolAsync,
            new McpServerToolCreateOptions
            {
                Name = "lattice_data_gset_get",
                SerializerOptions = LatticeApiMcpToolSerialization.Options,
                Title = "Read G-Set members",
                Description =
                    "Reads the current members of a grow-only (G) set as a list of base64-encoded element bytes in "
                    + "the set's deterministic order. An absent or unreadable key yields an empty list, never a "
                    + "fault. Read-only.",
                ReadOnly = true,
                Destructive = false,
                UseStructuredContent = true,
            });

    private static Task<CrdtWriteToolResult> CounterWriteToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The counter key.")] string key,
        [Description("Whether to increment or decrement the counter.")] CrdtCounterOp operation,
        [Description(ReplicaIdDescription)] string replicaId,
        [Description("The non-negative magnitude to add or subtract.")] long amount,
        CancellationToken cancellationToken)
        => DataToolCore.CounterWriteAsync(ResolveApi(context), treeId, key, operation, replicaId, amount, cancellationToken);

    private static Task<CrdtCounterToolResult> CounterGetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The counter key.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.CounterGetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<CrdtWriteToolResult> GCounterWriteToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The counter key.")] string key,
        [Description(ReplicaIdDescription)] string replicaId,
        [Description("The non-negative amount to add.")] long amount,
        CancellationToken cancellationToken)
        => DataToolCore.GCounterIncrementAsync(ResolveApi(context), treeId, key, replicaId, amount, cancellationToken);

    private static Task<CrdtCounterToolResult> GCounterGetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The counter key.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.GCounterGetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<CrdtWriteToolResult> SetWriteToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The set key.")] string key,
        [Description("Whether to add or observed-remove the element.")] CrdtSetOp operation,
        [Description("The element bytes, base64-encoded. Invalid base64 is rejected as a caller error.")] string element,
        [Description(ReplicaIdDescription)] string replicaId,
        CancellationToken cancellationToken)
        => DataToolCore.SetWriteAsync(
            ResolveApi(context), treeId, key, operation, DecodeBase64Value(element), replicaId, cancellationToken);

    private static Task<CrdtElementsToolResult> SetGetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The set key.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.SetGetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<CrdtWriteToolResult> OrFlagWriteToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The flag key.")] string key,
        [Description("Whether to enable or disable the flag.")] CrdtFlagOp operation,
        [Description(ReplicaIdDescription)] string replicaId,
        CancellationToken cancellationToken)
        => DataToolCore.OrFlagWriteAsync(ResolveApi(context), treeId, key, operation, replicaId, cancellationToken);

    private static Task<CrdtFlagToolResult> OrFlagGetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The flag key.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.OrFlagGetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<CrdtWriteToolResult> RwFlagWriteToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The flag key.")] string key,
        [Description("Whether to enable or disable the flag.")] CrdtFlagOp operation,
        [Description(ReplicaIdDescription)] string replicaId,
        CancellationToken cancellationToken)
        => DataToolCore.RwFlagWriteAsync(ResolveApi(context), treeId, key, operation, replicaId, cancellationToken);

    private static Task<CrdtFlagToolResult> RwFlagGetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The flag key.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.RwFlagGetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<CrdtWriteToolResult> RwSetWriteToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The set key.")] string key,
        [Description("Whether to add or remove-wins remove the element.")] CrdtRwSetOp operation,
        [Description("The element bytes, base64-encoded. Invalid base64 is rejected as a caller error.")] string element,
        [Description(ReplicaIdDescription)] string replicaId,
        CancellationToken cancellationToken)
        => DataToolCore.RwSetWriteAsync(
            ResolveApi(context), treeId, key, operation, DecodeBase64Value(element), replicaId, cancellationToken);

    private static Task<CrdtElementsToolResult> RwSetGetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The set key.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.RwSetGetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<CrdtWriteToolResult> VersionVectorTickToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The version-vector key.")] string key,
        [Description(ReplicaIdDescription)] string replicaId,
        CancellationToken cancellationToken)
        => DataToolCore.VersionVectorTickAsync(ResolveApi(context), treeId, key, replicaId, cancellationToken);

    private static Task<CrdtVersionVectorToolResult> VersionVectorGetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The version-vector key.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.VersionVectorGetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<CrdtWriteToolResult> RegisterSetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The register key.")] string key,
        [Description(ReplicaIdDescription)] string replicaId,
        [Description("The value bytes, base64-encoded. Invalid base64 is rejected as a caller error.")] string value,
        CancellationToken cancellationToken)
        => DataToolCore.RegisterSetAsync(
            ResolveApi(context), treeId, key, replicaId, DecodeBase64Value(value), cancellationToken);

    private static Task<CrdtElementsToolResult> RegisterGetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The register key.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.RegisterGetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<CrdtWriteToolResult> MaxRegisterSetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The register key.")] string key,
        [Description("The candidate value bytes, base64-encoded. Invalid base64 is rejected as a caller error.")] string value,
        CancellationToken cancellationToken)
        => DataToolCore.MaxRegisterSetAsync(
            ResolveApi(context), treeId, key, DecodeBase64Value(value), cancellationToken);

    private static Task<CrdtElementsToolResult> MaxRegisterGetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The register key.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.MaxRegisterGetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<CrdtWriteToolResult> MinRegisterSetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The register key.")] string key,
        [Description("The candidate value bytes, base64-encoded. Invalid base64 is rejected as a caller error.")] string value,
        CancellationToken cancellationToken)
        => DataToolCore.MinRegisterSetAsync(
            ResolveApi(context), treeId, key, DecodeBase64Value(value), cancellationToken);

    private static Task<CrdtElementsToolResult> MinRegisterGetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The register key.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.MinRegisterGetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<CrdtWriteToolResult> SequenceWriteToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The sequence key.")] string key,
        [Description("Whether to insert at, or remove at, the given position.")] CrdtSequenceOp operation,
        [Description("The zero-based position in the current visible order.")] int index,
        [Description(ReplicaIdDescription)] string replicaId,
        [Description("The value bytes to insert, base64-encoded (required for insertAt, ignored for removeAt). Invalid base64 is rejected as a caller error.")]
        string? value = null,
        CancellationToken cancellationToken = default)
        => DataToolCore.SequenceWriteAsync(
            ResolveApi(context), treeId, key, operation, index, replicaId, DecodeOptionalBase64Value(value), cancellationToken);

    private static Task<CrdtElementsToolResult> SequenceGetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The sequence key.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.SequenceGetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<CrdtWriteToolResult> MapWriteToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The map key.")] string key,
        [Description("Whether to set a field's value or observed-remove the field.")] CrdtMapOp operation,
        [Description("The map field name.")] string field,
        [Description(ReplicaIdDescription)] string replicaId,
        [Description("The value bytes to put under the field, base64-encoded (required for set, ignored for remove). Invalid base64 is rejected as a caller error.")]
        string? value = null,
        CancellationToken cancellationToken = default)
        => DataToolCore.MapWriteAsync(
            ResolveApi(context), treeId, key, operation, field, replicaId, DecodeOptionalBase64Value(value), cancellationToken);

    private static Task<CrdtMapToolResult> MapGetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The map key.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.MapGetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static Task<CrdtWriteToolResult> GSetWriteToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The set key.")] string key,
        [Description("The element bytes, base64-encoded. Invalid base64 is rejected as a caller error.")] string element,
        CancellationToken cancellationToken)
        => DataToolCore.GSetAddAsync(
            ResolveApi(context), treeId, key, DecodeBase64Value(element), cancellationToken);

    private static Task<CrdtElementsToolResult> GSetGetToolAsync(
        RequestContext<CallToolRequestParams> context,
        [Description("Logical tree identifier.")] string treeId,
        [Description("The set key.")] string key,
        CancellationToken cancellationToken)
        => DataToolCore.GSetGetAsync(ResolveApi(context), treeId, key, cancellationToken);

    private static byte[]? DecodeOptionalBase64Value(string? value)
        => value is null ? null : DecodeBase64Value(value);
}
