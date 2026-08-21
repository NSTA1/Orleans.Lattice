using Orleans.Lattice;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Transport-agnostic <b>tree administration</b> control facade: one coherent,
/// discoverable, authorized surface for whole-tree lifecycle and administration
/// operations. Every transport binding (the gRPC service, the MCP tool group) is a
/// thin adapter over this single surface, so the control semantics are written and
/// tested once and no transport concern leaks into the control logic.
/// </summary>
/// <remarks>
/// <para>
/// <b>Composition over absorption.</b> Tree administration does not re-implement
/// operations that already have a single-responsibility facade. It <b>wraps</b> the
/// existing schema control facade (<see cref="ILatticeSchemaControl"/>) by
/// delegation, so schema stays its own facade with no breaking change (no wire or
/// alias change), and tree administration still presents one complete surface. The
/// same composition approach applies to any other existing facade a future
/// lifecycle operation needs to reach.
/// </para>
/// <para>
/// <b>Scaffolding scope.</b> This foundation exposes only the capability probe;
/// the whole-tree lifecycle operations (bulk-load, delete/drop, resize, reshard,
/// and the rest) land in the dependent sub-issues, each adding its verb here and a
/// probe flag on <see cref="LatticeTreeAdminCapabilities"/>. Whole-tree operations
/// will use the whole-tree operation gates (<see cref="LatticeOperation.Admin"/> /
/// <see cref="LatticeOperation.BulkLoad"/>), default-denied for anonymous callers.
/// </para>
/// <para>
/// <b>Fail-closed authorization is inherited</b> from the facade access-gate seams;
/// the facade adds no authorization path of its own.
/// </para>
/// </remarks>
public interface ILatticeTreeAdmin
{
    /// <summary>
    /// Probes which tree-administration operations the current caller may perform
    /// over <paramref name="treeId"/>, evaluated through the same fail-closed access
    /// gates the real operations use but with <b>no side effects</b>. Each denied
    /// capability is reported as a <see langword="false"/> flag, default-deny, so a
    /// management UI can grey out controls the caller cannot use. The composed
    /// schema capabilities are delegated to the wrapped
    /// <see cref="ILatticeSchemaControl"/> facade. The reported flags are advisory;
    /// the server still authorizes each real operation on attempt.
    /// </summary>
    /// <param name="treeId">The tree to probe. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The caller's allowed tree-administration operation set for <paramref name="treeId"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    Task<LatticeTreeAdminCapabilities> ProbeCapabilitiesAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a per-shard read/write hotness report for <paramref name="treeId"/>: a
    /// cheap, non-blocking sample of each physical shard's activity counters, used to
    /// spot skew (a few hot shards) before deciding on a reshard. Read-only, but
    /// still gated - the caller must hold <see cref="LatticeOperation.Read"/> over the
    /// whole tree.
    /// </summary>
    /// <param name="treeId">The tree to sample. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The whole-tree hotness report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeHotnessReport> GetShardHotnessAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a whole-tree diagnostic report for <paramref name="treeId"/>: per-shard
    /// depth, live/tombstone counts, activity, and in-flight maintenance flags. When
    /// <paramref name="deep"/> is <see langword="false"/> (the default) the report
    /// comes from the cheap shard-root projection; when <see langword="true"/> it
    /// walks leaf state for authoritative counts at higher cost. Read-only, but still
    /// gated on <see cref="LatticeOperation.Read"/> over the whole tree.
    /// </summary>
    /// <param name="treeId">The tree to diagnose. Must not be <c>null</c> or empty.</param>
    /// <param name="deep">Walk leaf state for authoritative counts; defaults to the cheap projection.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The whole-tree diagnostic report.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeAdminDiagnosticReport> GetDiagnosticsAsync(
        string treeId, bool deep = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Inspects the shard-map topology for <paramref name="treeId"/>: how the virtual
    /// routing space maps onto physical shards, the physical tree id it resolves to,
    /// and the map version. Read-only, but still gated on
    /// <see cref="LatticeOperation.Read"/> over the whole tree.
    /// </summary>
    /// <param name="treeId">The tree to inspect. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The shard-map inspection.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<ShardMapInspection> InspectShardMapAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a leaf-projection digest for a single physical shard of
    /// <paramref name="treeId"/>: a content hash plus counts that identify the shard's
    /// committed state, for cheap divergence detection without shipping the data.
    /// Read-only, but still gated on <see cref="LatticeOperation.Read"/> over the whole
    /// tree.
    /// </summary>
    /// <param name="treeId">The tree the shard belongs to. Must not be <c>null</c> or empty.</param>
    /// <param name="shardIndex">The zero-based physical shard index. Must not be negative.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The shard's projection digest.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="shardIndex"/> is negative.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<ShardProjectionDigestReport> GetProjectionDigestAsync(
        string treeId, int shardIndex, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a rolled-up statistics snapshot for <paramref name="treeId"/>: topology
    /// and live-key counts joined with the tree's storage byte breakdown, in one call.
    /// Read-only, but still gated on <see cref="LatticeOperation.Read"/> over the whole
    /// tree.
    /// </summary>
    /// <param name="treeId">The tree to summarize. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree statistics snapshot.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeStatsReport> GetTreeStatsAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a cluster-wide storage accounting summary across every tree, split by
    /// surface (write-ahead log, snapshots, leaf state). When <paramref name="deep"/>
    /// is <see langword="false"/> (the default) it returns the cheap cached WAL-poll
    /// aggregate; when <see langword="true"/> it forces an expensive fresh leaf-walk
    /// that re-measures every shard. Read-only, but still gated on
    /// <see cref="LatticeOperation.Telemetry"/> over the cluster-wide scope, so a
    /// caller without cluster telemetry authority is refused.
    /// </summary>
    /// <param name="deep">Force a fresh leaf-walk re-measure; defaults to the cheap cached aggregate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The cluster-wide storage usage summary.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized for cluster telemetry.</exception>
    Task<ClusterStorageUsageSummary> GetStorageUsageAsync(
        bool deep = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Explicitly creates (registers) <paramref name="treeId"/> with an optional
    /// initial structural sizing, after authorizing whole-tree administration on the
    /// tree fail-closed. Registration is <b>idempotent</b>: creating a tree that is
    /// already registered is a no-op that preserves the existing configuration (the
    /// supplied sizing is ignored) and reports
    /// <see cref="TreeCreationResult.Created"/> <see langword="false"/>. Reserved
    /// system tree ids (the <c>_lattice_</c> namespace) are rejected.
    /// </summary>
    /// <param name="treeId">The tree to create. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="shardCount">The initial physical shard count, or <c>null</c> for the library default.</param>
    /// <param name="maxLeafKeys">The initial maximum keys per leaf node, or <c>null</c> for the library default.</param>
    /// <param name="maxInternalChildren">The initial maximum children per internal node, or <c>null</c> for the library default.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The creation result, including whether a new tree was registered and its effective sizing.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A supplied sizing value is not strictly positive.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to administer the tree.</exception>
    Task<TreeCreationResult> CreateTreeAsync(
        string treeId,
        int? shardCount = null,
        int? maxLeafKeys = null,
        int? maxInternalChildren = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reports whether <paramref name="treeId"/> is registered in the tree registry,
    /// after authorizing whole-tree read on the tree fail-closed. A pure read with no
    /// side effects.
    /// </summary>
    /// <param name="treeId">The tree to check. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The existence result.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeExistenceResult> CheckTreeExistsAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Points the logical <paramref name="treeId"/> at
    /// <paramref name="physicalTreeId"/> so subsequent reads and writes routed
    /// through the tree target the physical tree, after authorizing whole-tree
    /// administration on the tree fail-closed. Only a single level of indirection is
    /// allowed - the physical target must not itself be aliased. Reserved system tree
    /// ids are rejected.
    /// </summary>
    /// <param name="treeId">The logical tree to alias. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="physicalTreeId">The physical tree to point at. Must not be <c>null</c> or empty, and must differ from <paramref name="treeId"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The resulting alias state.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, or <paramref name="physicalTreeId"/> is <c>null</c>, empty, or equal to <paramref name="treeId"/>.</exception>
    /// <exception cref="InvalidOperationException">The physical target is itself aliased (multi-level indirection).</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to administer the tree.</exception>
    Task<TreeAliasResolution> SetTreeAliasAsync(
        string treeId, string physicalTreeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves the physical tree id the logical <paramref name="treeId"/> currently
    /// maps to, after authorizing whole-tree read on the tree fail-closed. Returns the
    /// logical id itself when no alias is in effect. A pure read with no side effects.
    /// </summary>
    /// <param name="treeId">The logical tree to resolve. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The current alias state.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeAliasResolution> ResolveTreeAliasAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the registry-backed configuration for <paramref name="treeId"/> - its
    /// structural sizing pins, alias target, and per-tree runtime overrides - after
    /// authorizing whole-tree read on the tree fail-closed. An unregistered tree
    /// reports <see cref="TreeConfigurationReport.Exists"/> <see langword="false"/>. A
    /// pure read with no side effects.
    /// </summary>
    /// <param name="treeId">The tree to read. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The configuration snapshot.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeConfigurationReport> GetTreeConfigAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Applies a partial update to <paramref name="treeId"/>'s per-tree runtime
    /// configuration (publish-events, projection-digest maintenance, durable-history
    /// retention), after authorizing whole-tree administration on the tree
    /// fail-closed. Each dimension is written only when its <c>Apply*</c> flag is set;
    /// a <see langword="null"/> value on an applied dimension clears that override.
    /// Reserved system tree ids are rejected. Returns the resulting configuration.
    /// </summary>
    /// <param name="treeId">The tree to configure. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="update">The partial configuration update. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The configuration snapshot after the update.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="update"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An applied history-retention window is not strictly positive.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to administer the tree.</exception>
    Task<TreeConfigurationReport> SetTreeConfigAsync(
        string treeId, TreeConfigurationUpdate update, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the registry-persisted shard map for <paramref name="treeId"/>, after
    /// authorizing whole-tree read on the tree fail-closed. Reports whether a custom
    /// map has been persisted (versus the default identity map) and, when it has, the
    /// persisted slot topology. A pure read with no side effects; shard-map mutation
    /// is driven by the resize / reshard operations, not here.
    /// </summary>
    /// <param name="treeId">The tree to read. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The persisted shard-map view.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeShardMapView> GetShardMapAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Soft-deletes <paramref name="treeId"/>, after authorizing the whole-tree
    /// <see cref="LatticeOperation.TreeLifecycle"/> capability fail-closed. Every
    /// shard is immediately marked deleted (subsequent reads and writes throw), and
    /// a deferred purge is scheduled after the configured soft-delete duration.
    /// Reversible with <see cref="RecoverTreeAsync"/> until that window elapses or
    /// an explicit <see cref="PurgeTreeAsync"/> runs. Idempotent: deleting an
    /// already-deleted tree is a no-op. Reserved system tree ids are rejected.
    /// </summary>
    /// <param name="treeId">The tree to soft-delete. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's deletion status after the soft delete.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="InvalidOperationException">The tree is the source of one or more materialised views.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the tree-lifecycle capability.</exception>
    Task<TreeDeletionStatus> DeleteTreeAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Recovers a soft-deleted <paramref name="treeId"/> within its recovery window,
    /// after authorizing the whole-tree <see cref="LatticeOperation.TreeLifecycle"/>
    /// capability fail-closed. Restores normal operation and cancels the deferred
    /// purge. Reserved system tree ids are rejected.
    /// </summary>
    /// <param name="treeId">The tree to recover. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's deletion status after recovery.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="InvalidOperationException">The tree is not deleted, a purge is in progress, or the data was already purged.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the tree-lifecycle capability.</exception>
    Task<TreeDeletionStatus> RecoverTreeAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Immediately hard-purges a soft-deleted <paramref name="treeId"/>, bypassing
    /// the soft-delete window, after authorizing the whole-tree
    /// <see cref="LatticeOperation.TreeLifecycle"/> capability fail-closed. This is
    /// <b>irreversible</b>: all leaf and internal node state is permanently removed
    /// and the tree is unregistered. As a guard against accidental destruction the
    /// caller must pass <paramref name="confirm"/> <see langword="true"/>; a
    /// <see langword="false"/> value is rejected before any authorization or grain
    /// call. Reserved system tree ids are rejected.
    /// </summary>
    /// <param name="treeId">The tree to purge. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="confirm">Must be <see langword="true"/> to acknowledge the irreversible purge; <see langword="false"/> is rejected.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's deletion status after the purge.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, or <paramref name="confirm"/> is <see langword="false"/>.</exception>
    /// <exception cref="InvalidOperationException">The tree is not deleted or was already purged.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the tree-lifecycle capability.</exception>
    Task<TreeDeletionStatus> PurgeTreeAsync(
        string treeId, bool confirm, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the soft-deletion lifecycle status of <paramref name="treeId"/> - live,
    /// soft-deleted (with the recovery deadline), purge in progress, or purged -
    /// after authorizing whole-tree <see cref="LatticeOperation.Read"/> fail-closed.
    /// A pure read with no side effects.
    /// </summary>
    /// <param name="treeId">The tree to inspect. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's deletion status.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeDeletionStatus> GetTreeDeletionStatusAsync(
        string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a resumable, chunk-paged <b>bulk-load (tree creation)</b> session on
    /// <paramref name="treeId"/>, after authorizing the whole-tree
    /// <see cref="LatticeOperation.BulkLoad"/> capability fail-closed. Bulk-load is a
    /// bottom-up initial-seed primitive, so the tree must be <b>empty</b>: a tree
    /// that already holds data is rejected with <see cref="TreeNotEmptyException"/>
    /// so the caller can distinguish it from a transient fault. Reserved system tree
    /// ids are rejected.
    /// </summary>
    /// <remarks>
    /// The session holds no server-side state. The caller then streams the sorted
    /// entries through <see cref="AppendBulkLoadAsync"/> in ascending key order,
    /// advancing a monotonic chunk index, and finalizes with
    /// <see cref="CommitBulkLoadAsync"/>. A dropped connection is recovered by
    /// re-driving from the last un-acknowledged chunk under the same
    /// <paramref name="operationId"/>; each append is idempotent.
    /// </remarks>
    /// <param name="treeId">The tree to bulk-load into. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="operationId">The caller-supplied idempotency key for the whole session. Must not be <c>null</c> or empty, and must not contain <c>'/'</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The opened bulk-load session.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, or <paramref name="operationId"/> is <c>null</c>, empty, or contains <c>'/'</c>.</exception>
    /// <exception cref="TreeNotEmptyException">The target tree already contains data.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the bulk-load capability.</exception>
    Task<TreeBulkLoadSession> BeginBulkLoadAsync(
        string treeId, string operationId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Appends one <paramref name="chunkIndex"/>-ordered chunk of a bulk-load stream
    /// to <paramref name="treeId"/>, after authorizing the whole-tree
    /// <see cref="LatticeOperation.BulkLoad"/> capability fail-closed. The chunk's
    /// entries must be in <b>strictly ascending key order</b>, and each chunk must
    /// continue the ascending order of the whole stream; an out-of-order chunk is
    /// rejected with <see cref="BulkLoadOrderException"/> before any entry is
    /// applied. Re-driving the same <paramref name="chunkIndex"/> under the same
    /// <paramref name="operationId"/> is an idempotent no-op.
    /// </summary>
    /// <param name="treeId">The tree being loaded. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="operationId">The session operation id supplied to <see cref="BeginBulkLoadAsync"/>. Must not be <c>null</c> or empty, and must not contain <c>'/'</c>.</param>
    /// <param name="chunkIndex">The zero-based, monotonically increasing chunk index. Must not be negative.</param>
    /// <param name="entries">The chunk's entries in strictly ascending key order. An empty chunk is accepted as a no-op.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The chunk acknowledgement, carrying the next expected chunk index.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, or <paramref name="operationId"/> is <c>null</c>, empty, or contains <c>'/'</c>.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="entries"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="chunkIndex"/> is negative.</exception>
    /// <exception cref="BulkLoadOrderException">The chunk is not in strictly ascending key order.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the bulk-load capability.</exception>
    Task<TreeBulkLoadChunkAck> AppendBulkLoadAsync(
        string treeId,
        string operationId,
        long chunkIndex,
        IReadOnlyList<DataEntry> entries,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Finalizes a bulk-load session on <paramref name="treeId"/>, after authorizing
    /// the whole-tree <see cref="LatticeOperation.BulkLoad"/> capability fail-closed.
    /// Commit is the caller's explicit end-of-stream marker; every acknowledged chunk
    /// is already durable, so commit persists no further data and simply confirms the
    /// load and reports the tree's observed live-key count.
    /// </summary>
    /// <param name="treeId">The tree that was loaded. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="operationId">The session operation id supplied to <see cref="BeginBulkLoadAsync"/>. Must not be <c>null</c> or empty, and must not contain <c>'/'</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The bulk-load result.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, or <paramref name="operationId"/> is <c>null</c>, empty, or contains <c>'/'</c>.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the bulk-load capability.</exception>
    Task<TreeBulkLoadResult> CommitBulkLoadAsync(
        string treeId, string operationId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Restores the captured backup <paramref name="backupId"/> into
    /// <paramref name="treeId"/>, after authorizing the whole-tree
    /// <see cref="LatticeOperation.Restore"/> capability fail-closed. Composes the
    /// cluster's backup/restore engine: the backup's base chain is walked and every
    /// referenced artifact validated before anything is installed, then the entries
    /// are replayed HLC-preserving into a fresh shadow physical tree whose alias is
    /// atomically cut over, so the restore is online and reversible. Reserved system
    /// tree ids are rejected. Re-running the same restore under the same
    /// <paramref name="operationId"/> converges to the same state.
    /// </summary>
    /// <param name="treeId">The tree to restore into. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="backupId">The content-addressed id of the backup to restore. Must not be <c>null</c> or empty.</param>
    /// <param name="operationId">An idempotency key that makes a retried restore a no-op, or <c>null</c> to derive one from the request. Must not be empty when supplied.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The restore outcome, including the shadow and previous physical trees needed to revert it.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved, <paramref name="backupId"/> is <c>null</c> or empty, or <paramref name="operationId"/> is empty.</exception>
    /// <exception cref="InvalidOperationException">No backup/restore engine is registered on the cluster, or the backup fails pre-apply validation.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the restore capability.</exception>
    Task<TreeRestoreResult> RestoreTreeAsync(
        string treeId,
        string backupId,
        string? operationId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Restores every tree in the captured backup <b>set</b> identified by
    /// <paramref name="setId"/> as a single unit, composing the cluster's
    /// backup/restore engine. Each member tree is restored via an atomic
    /// shadow-cutover; when any member is replicated the whole set flips together as
    /// a coordinated all-or-nothing saga. Because the set spans multiple member
    /// trees, this verb applies no facade-level whole-tree authorization of its own -
    /// the restore engine authorizes each member's <see cref="LatticeOperation.Restore"/>
    /// scope fail-closed. Re-running the same set restore converges to the same state.
    /// </summary>
    /// <param name="setId">The content-addressed id of the backup set to restore. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The per-member restore results this cluster applied, one per hosted member tree.</returns>
    /// <exception cref="ArgumentException"><paramref name="setId"/> is <c>null</c> or empty, or resolves to no member trees.</exception>
    /// <exception cref="InvalidOperationException">No backup/restore engine is registered on the cluster, or a member backup fails pre-apply validation.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore a member tree's scope.</exception>
    Task<IReadOnlyList<TreeRestoreResult>> RestoreTreeSetAsync(
        string setId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reverts a <see cref="TreeRestoreMode.ShadowCutover"/> restore produced by
    /// <see cref="RestoreTreeAsync"/> by swapping the target tree's registry alias
    /// back to the physical tree it resolved to before the cutover
    /// (<see cref="TreeRestoreResult.PreviousPhysicalTreeId"/>), after authorizing the
    /// whole-tree <see cref="LatticeOperation.Restore"/> capability on the result's
    /// target tree fail-closed. Idempotent. Rejects a result that did not come from a
    /// shadow-cutover restore, and a reserved system target tree id.
    /// </summary>
    /// <param name="restore">The result of the shadow-cutover restore to revert. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the restore has been reverted.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="restore"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="restore"/> is not a shadow-cutover restore result, or its target tree id is reserved.</exception>
    /// <exception cref="InvalidOperationException">No backup/restore engine is registered on the cluster.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the restore capability.</exception>
    Task RevertTreeRestoreAsync(
        TreeRestoreResult restore,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Triggers an <b>online reshard</b> that grows <paramref name="treeId"/> to
    /// <paramref name="targetShardCount"/> distinct physical shards, after authorizing
    /// the whole-tree <see cref="LatticeOperation.TreeLifecycle"/> capability
    /// fail-closed. The tree keeps serving reads and writes throughout: the migration
    /// iteratively splits the largest-slot-owning shards and atomically swaps
    /// virtual-slot routing per split, anchored by reminders so it survives silo
    /// restarts. Returns once the coordinator has accepted the intent; poll completion
    /// with <see cref="GetReshardStatusAsync"/>. <b>Grow-only</b>: the target must be
    /// strictly greater than the current physical shard count (an empty tree may be
    /// re-pinned to any count) and at most the virtual shard space (4096). Idempotent:
    /// a request for the count the tree is already at, or a matching in-flight target,
    /// is a no-op. Reserved system tree ids are rejected.
    /// </summary>
    /// <param name="treeId">The tree to reshard. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="targetShardCount">The desired number of distinct physical shards. Must be at least 2 and at most the virtual shard space.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's reshard status after the trigger, echoing the requested target.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="targetShardCount"/> is out of range or would shrink the tree.</exception>
    /// <exception cref="InvalidOperationException">A reshard with a different target, or a resize, is already in progress.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the tree-lifecycle capability.</exception>
    Task<TreeReshardStatus> ReshardTreeAsync(
        string treeId,
        int targetShardCount,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the online-reshard status of <paramref name="treeId"/> - whether a
    /// reshard is in flight and the tree's current physical shard fan-out as observed
    /// from its <c>ShardMap</c> - after authorizing whole-tree
    /// <see cref="LatticeOperation.Read"/> fail-closed. A pure read with no side
    /// effects.
    /// </summary>
    /// <param name="treeId">The tree to inspect. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's reshard status.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeReshardStatus> GetReshardStatusAsync(
        string treeId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Triggers an <b>online resize</b> that rebuilds <paramref name="treeId"/> with a
    /// new B+ node capacity (<paramref name="newMaxLeafKeys"/> keys per leaf,
    /// <paramref name="newMaxInternalChildren"/> children per internal node), after
    /// authorizing the whole-tree <see cref="LatticeOperation.TreeLifecycle"/>
    /// capability fail-closed. The resize is online: the tree is snapshotted into a
    /// fresh destination physical tree with the new sizing while live writes are
    /// shadow-forwarded, then the registry alias is atomically swapped, all anchored by
    /// reminders so it survives silo restarts. Returns once the coordinator has accepted
    /// the intent; poll completion with <see cref="GetResizeStatusAsync"/> and reverse
    /// it within the recovery window with <see cref="UndoTreeResizeAsync"/>. Idempotent:
    /// a matching in-flight target is a no-op. Reserved system tree ids are rejected.
    /// </summary>
    /// <param name="treeId">The tree to resize. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="newMaxLeafKeys">The new maximum number of keys per leaf node. Must be greater than 1.</param>
    /// <param name="newMaxInternalChildren">The new maximum number of children per internal node. Must be greater than 2.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's resize status after the trigger, echoing the requested capacity.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="newMaxLeafKeys"/> or <paramref name="newMaxInternalChildren"/> is out of range.</exception>
    /// <exception cref="InvalidOperationException">A resize with different parameters, or a reshard, is already in progress.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the tree-lifecycle capability.</exception>
    Task<TreeResizeStatus> ResizeTreeAsync(
        string treeId,
        int newMaxLeafKeys,
        int newMaxInternalChildren,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Undoes the most recent completed <b>online resize</b> of
    /// <paramref name="treeId"/> by swapping the registry alias back to the tree's
    /// pre-resize physical tree and restoring its original sizing, after authorizing the
    /// whole-tree <see cref="LatticeOperation.TreeLifecycle"/> capability fail-closed.
    /// Only available while the pre-resize physical tree is still within its
    /// soft-delete recovery window (before purge completes). Reserved system tree ids
    /// are rejected.
    /// </summary>
    /// <param name="treeId">The tree whose last resize to undo. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's resize status after the undo.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="InvalidOperationException">No completed resize exists to undo, or the pre-resize tree has already been purged.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the tree-lifecycle capability.</exception>
    Task<TreeResizeStatus> UndoTreeResizeAsync(
        string treeId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the online-resize status of <paramref name="treeId"/> - whether a resize
    /// is in flight and the tree's current B+ node capacity as observed from its
    /// registry configuration - after authorizing whole-tree
    /// <see cref="LatticeOperation.Read"/> fail-closed. A pure read with no side
    /// effects.
    /// </summary>
    /// <param name="treeId">The tree to inspect. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's resize status.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeResizeStatus> GetResizeStatusAsync(
        string treeId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Triggers a <b>snapshot capture</b> of <paramref name="treeId"/> into a fresh
    /// <paramref name="destinationTreeId"/>, after authorizing the whole-tree
    /// <see cref="LatticeOperation.Admin"/> capability fail-closed. Every live
    /// key-value pair is copied shard-by-shard into the destination tree, anchored by
    /// reminders so it survives silo restarts. In <see cref="TreeSnapshotMode.Offline"/>
    /// mode the source tree is quiesced for the duration; in
    /// <see cref="TreeSnapshotMode.Online"/> mode the source keeps serving reads and
    /// writes while live mutations are shadow-forwarded to the destination and the
    /// drain converges under last-writer-wins. Returns once the coordinator has
    /// accepted the intent; poll completion with <see cref="GetSnapshotStatusAsync"/>.
    /// Idempotent: a matching in-flight capture to the same destination and mode is a
    /// no-op. Reserved system tree ids are rejected. This is <b>not</b> the Backup
    /// facade: the destination is a live tree, not a durable catalogued artifact.
    /// </summary>
    /// <param name="treeId">The source tree to snapshot. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="destinationTreeId">The id for the new destination tree. Must not be <c>null</c>, empty, reserved, or already exist.</param>
    /// <param name="mode">Whether to quiesce the source tree during the copy.</param>
    /// <param name="maxLeafKeys">Optional leaf sizing override for the destination tree; <c>null</c> inherits the source tree's options.</param>
    /// <param name="maxInternalChildren">Optional internal-node sizing override for the destination tree; <c>null</c> inherits the source tree's options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The source tree's snapshot status after the trigger, echoing the requested destination and mode.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="destinationTreeId"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="InvalidOperationException">A snapshot with different parameters is already in progress, or the destination tree already exists.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the admin capability.</exception>
    Task<TreeSnapshotStatus> SnapshotTreeAsync(
        string treeId,
        string destinationTreeId,
        TreeSnapshotMode mode,
        int? maxLeafKeys = null,
        int? maxInternalChildren = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the snapshot status of <paramref name="treeId"/> - whether a snapshot is
    /// in flight for the source tree - after authorizing whole-tree
    /// <see cref="LatticeOperation.Read"/> fail-closed. A pure read with no side
    /// effects.
    /// </summary>
    /// <param name="treeId">The source tree to inspect. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The source tree's snapshot status.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeSnapshotStatus> GetSnapshotStatusAsync(
        string treeId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Inspects the durable <b>WAL placement</b> of <paramref name="treeId"/> - which
    /// storage provider key backs each WAL partition, plus the placement version and
    /// per-partition resolvability on the reporting silo - after authorizing whole-tree
    /// <see cref="LatticeOperation.Read"/> fail-closed. A pure read with no side effects.
    /// </summary>
    /// <param name="treeId">The tree to inspect. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's WAL partition placement.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeWalPlacement> GetWalPlacementAsync(
        string treeId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Audits the WAL placement of <paramref name="treeId"/> against the resolving
    /// silo's WAL storage provider catalog, surfacing any partition pinned to a
    /// provider key the silo cannot resolve, after authorizing whole-tree
    /// <see cref="LatticeOperation.Read"/> fail-closed. A pure read with no side effects.
    /// </summary>
    /// <param name="treeId">The tree to audit. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's WAL placement audit, including the silo's known provider keys.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeWalPlacementAudit> AuditWalPlacementAsync(
        string treeId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Computes a read-only <b>preview</b> of moving WAL partition
    /// <paramref name="partition"/> of <paramref name="treeId"/> to
    /// <paramref name="targetProviderKey"/> - the range that would be copied and
    /// whether the target key resolves - without quiescing the partition or changing
    /// any placement, after authorizing whole-tree <see cref="LatticeOperation.Read"/>
    /// fail-closed. A pure read with no side effects.
    /// </summary>
    /// <param name="treeId">The tree whose partition would be moved. Must not be <c>null</c> or empty.</param>
    /// <param name="partition">The WAL partition index to preview. Must be in range for the tree.</param>
    /// <param name="targetProviderKey">The target storage provider key. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The read-only move plan.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="targetProviderKey"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is out of range for the tree.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    Task<TreeWalMovePlan> PlanWalMoveAsync(
        string treeId,
        int partition,
        string targetProviderKey,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// <b>Executes an online move</b> of WAL partition <paramref name="partition"/> of
    /// <paramref name="treeId"/> to <paramref name="targetProviderKey"/>, after
    /// authorizing the whole-tree <see cref="LatticeOperation.TreeLifecycle"/> capability
    /// fail-closed. Only the target partition is briefly quiesced while its tail is
    /// copied and the placement pin is atomically flipped; the source tail is retained
    /// (never trimmed by the move) until an explicit
    /// <see cref="ReclaimMovedWalSourceAsync"/> call, so the move is revertible until
    /// reclaimed. Idempotent: a partition already pinned to the target is an idempotent
    /// no-copy repair. Reserved system tree ids are rejected.
    /// </summary>
    /// <param name="treeId">The tree whose partition to move. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="partition">The WAL partition index to move. Must be in range for the tree.</param>
    /// <param name="targetProviderKey">The target storage provider key. Must not be <c>null</c> or empty, and must resolve on the executing silo.</param>
    /// <param name="options">Optional move tunables; <c>null</c> takes the conventional defaults.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The move receipt, recording the copied range and the new placement version.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="targetProviderKey"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is out of range for the tree.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the tree-lifecycle capability.</exception>
    Task<TreeWalMoveReceipt> ExecuteWalMoveAsync(
        string treeId,
        int partition,
        string targetProviderKey,
        TreeWalMoveOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// <b>Reclaims</b> the orphaned source tail left behind by a completed
    /// <see cref="ExecuteWalMoveAsync"/> - discarding partition
    /// <paramref name="partition"/>'s retained log on
    /// <paramref name="sourceProviderKey"/> - after authorizing the whole-tree
    /// <see cref="LatticeOperation.TreeLifecycle"/> capability fail-closed. This is the
    /// <b>irreversible</b> finalisation step, deliberately separate from the move: once
    /// reclaimed the move can no longer be reverted by moving the partition back.
    /// Refused if <paramref name="sourceProviderKey"/> is the partition's live
    /// placement. Reserved system tree ids are rejected.
    /// </summary>
    /// <param name="treeId">The tree whose moved source to reclaim. Must not be <c>null</c>, empty, or reserved.</param>
    /// <param name="partition">The WAL partition index whose orphaned source to reclaim. Must be in range for the tree.</param>
    /// <param name="sourceProviderKey">The provider key of the orphaned source tail. Must not be <c>null</c> or empty, and must not be the partition's live placement.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The reclaim receipt.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> or <paramref name="sourceProviderKey"/> is <c>null</c>, empty, or reserved.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/> is out of range for the tree.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="sourceProviderKey"/> is the partition's live placement.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the tree-lifecycle capability.</exception>
    Task<TreeWalMoveReceipt> ReclaimMovedWalSourceAsync(
        string treeId,
        int partition,
        string sourceProviderKey,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the cluster's <b>runtime-registered materialised views</b> - every view
    /// created at runtime through the view factory and durably recorded in the
    /// cluster-wide runtime-view registry - after authorizing the cluster-wide
    /// <see cref="LatticeOperation.Telemetry"/> capability fail-closed. A pure read with
    /// no side effects.
    /// <para>
    /// Startup-declared views (declared authoritatively through <c>AddLatticeViews</c>)
    /// are <b>not</b> included: they are not runtime registrations and cannot be dropped
    /// at runtime; the State facade's view catalog read surfaces those.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The runtime-registered view catalog.</returns>
    /// <exception cref="InvalidOperationException">The materialised-view subsystem is not enabled on this cluster.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the telemetry capability.</exception>
    Task<TreeViewCatalog> ListViewsAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a runtime materialised view named <paramref name="viewName"/> over
    /// <paramref name="sourceTreeId"/> using the host-registered projection provider
    /// identified by <paramref name="providerKey"/>. The opaque
    /// <paramref name="payload"/> is interpreted only by that provider and is never
    /// returned by this API. The source is authorized for whole-tree
    /// <see cref="LatticeOperation.Admin"/> before the provider is invoked.
    /// </summary>
    /// <param name="viewName">The logical view name. Must not be <c>null</c> or empty.</param>
    /// <param name="sourceTreeId">The directly writable source tree id. A materialised-view tree cannot be a source.</param>
    /// <param name="providerKey">The non-empty host-registered projection provider key.</param>
    /// <param name="payload">Opaque provider state, limited to 64 KiB.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The server-derived status of the created view.</returns>
    /// <exception cref="ArgumentException">A name or provider key is empty, or the source is a materialised-view tree.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="payload"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="payload"/> exceeds 64 KiB.</exception>
    /// <exception cref="InvalidOperationException">The view subsystem or provider is unavailable.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks whole-tree admin authority over the source.</exception>
    Task<TreeViewStatus> CreateViewAsync(
        string viewName,
        string sourceTreeId,
        string providerKey,
        byte[] payload,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(
            "This tree-administration implementation does not support runtime view creation.");

    /// <summary>
    /// Reads the status of the materialised view named <paramref name="viewName"/> -
    /// its source tree, apply lag, and active view tree id - after resolving the view's
    /// source tree and authorizing whole-tree <see cref="LatticeOperation.Read"/> over
    /// that source fail-closed. A materialised view inherits its authorization boundary
    /// from the readability of its source tree. A pure read with no side effects.
    /// </summary>
    /// <param name="viewName">The logical view name. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The view's status.</returns>
    /// <exception cref="ArgumentException"><paramref name="viewName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="InvalidOperationException">The materialised-view subsystem is not enabled on this cluster.</exception>
    /// <exception cref="KeyNotFoundException">No view named <paramref name="viewName"/> is registered (or its source cannot be resolved).</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the view's source tree.</exception>
    Task<TreeViewStatus> GetViewStatusAsync(
        string viewName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// <b>Rebuilds</b> the materialised view named <paramref name="viewName"/> from
    /// current source state using a shadow-swap - a complete new generation tree is
    /// built and the active generation is atomically flipped over in a single durable
    /// commit, so readers never observe a half-built view - after resolving the view's
    /// source tree and authorizing whole-tree <see cref="LatticeOperation.Admin"/> over
    /// that source fail-closed. Online: the source keeps serving reads and writes
    /// throughout.
    /// </summary>
    /// <param name="viewName">The logical view name. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The view's status after the rebuild.</returns>
    /// <exception cref="ArgumentException"><paramref name="viewName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="InvalidOperationException">The materialised-view subsystem is not enabled on this cluster.</exception>
    /// <exception cref="KeyNotFoundException">No view named <paramref name="viewName"/> is registered (or its source cannot be resolved).</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the admin capability over the view's source tree.</exception>
    Task<TreeViewStatus> RebuildViewAsync(
        string viewName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// <b>Reconciles</b> the materialised view named <paramref name="viewName"/> against
    /// current source state - view anti-entropy that builds the expected view into a
    /// shadow generation, compares it to the live view via a content digest, and swaps
    /// the shadow in only when they diverge - after resolving the view's source tree and
    /// authorizing whole-tree <see cref="LatticeOperation.Admin"/> over that source
    /// fail-closed. Online and idempotent: a view that already matches its source is left
    /// untouched.
    /// </summary>
    /// <param name="viewName">The logical view name. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The reconcile result, reporting whether drift was detected and repaired.</returns>
    /// <exception cref="ArgumentException"><paramref name="viewName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="InvalidOperationException">The materialised-view subsystem is not enabled on this cluster.</exception>
    /// <exception cref="KeyNotFoundException">No view named <paramref name="viewName"/> is registered (or its source cannot be resolved).</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the admin capability over the view's source tree.</exception>
    Task<TreeViewReconcileResult> ReconcileViewAsync(
        string viewName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// <b>Drops</b> the materialised view named <paramref name="viewName"/> - stopping
    /// and decommissioning its maintainer, deleting every backing <c>view-{name}</c>
    /// generation, and removing its catalog entry and durable runtime registration -
    /// after resolving the view's source tree and authorizing whole-tree
    /// <see cref="LatticeOperation.Admin"/> over that source fail-closed. Idempotent for
    /// an already-absent view. A view declared at startup through <c>AddLatticeViews</c>
    /// cannot be dropped at runtime (the declaration would re-create it on the next silo
    /// start) and is rejected.
    /// </summary>
    /// <param name="viewName">The logical view name to drop. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentException"><paramref name="viewName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="InvalidOperationException">The materialised-view subsystem is not enabled on this cluster, or the view is declared at startup and cannot be dropped at runtime.</exception>
    /// <exception cref="KeyNotFoundException">No view named <paramref name="viewName"/> is registered (or its source cannot be resolved).</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the admin capability over the view's source tree.</exception>
    Task DropViewAsync(
        string viewName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Lists the cluster's tag indexes - each index's name, backing membership tree,
    /// shard count, and the subject trees it currently covers - after authorizing the
    /// distinct cluster-wide <see cref="LatticeOperation.Telemetry"/> capability
    /// fail-closed. Indexes are discovered from the tree registry (their backing
    /// membership trees carry the reserved <c>tag-</c> prefix). A pure read with no
    /// side effects.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tag-index catalog.</returns>
    /// <exception cref="InvalidOperationException">The tag-index subsystem is not available on this cluster.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the telemetry capability.</exception>
    Task<TreeTagIndexCatalog> ListTagIndexesAsync(
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the status of the tag index named <paramref name="indexName"/> - its
    /// backing membership tree, shard count, covered subject trees, and whether its
    /// background reconciliation coordinator is idle - after authorizing whole-tree
    /// <see cref="LatticeOperation.Read"/> over its backing membership tree
    /// (<c>tag-{indexName}</c>) fail-closed. A tag index inherits its authorization
    /// boundary from its backing membership tree, whose id is derived authoritatively
    /// from the index name and never trusted from the caller. A pure read with no side
    /// effects.
    /// </summary>
    /// <param name="indexName">The logical tag-index name. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tag index's status.</returns>
    /// <exception cref="ArgumentException"><paramref name="indexName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="InvalidOperationException">The tag-index subsystem is not available on this cluster.</exception>
    /// <exception cref="KeyNotFoundException">No tag index named <paramref name="indexName"/> is registered on this cluster.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the index's backing membership tree.</exception>
    Task<TreeTagIndexStatus> GetTagIndexStatusAsync(
        string indexName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// <b>Reconciles</b> the tag index named <paramref name="indexName"/> against current
    /// source state - an online, digest-gated live sweep that removes membership rows
    /// whose subject key no longer exists - after authorizing whole-tree
    /// <see cref="LatticeOperation.Admin"/> over its backing membership tree
    /// (<c>tag-{indexName}</c>) fail-closed. Reconcile writes only to the backing
    /// membership tree, so it authorizes on that tree; the covered subject trees are
    /// scanned as read-only infrastructure. Online and idempotent: an index that already
    /// matches its source is left untouched (no rows removed) and covered trees are never
    /// paused.
    /// </summary>
    /// <param name="indexName">The logical tag-index name to reconcile. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The reconcile report, pairing the index identity with the sweep's counts.</returns>
    /// <exception cref="ArgumentException"><paramref name="indexName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="InvalidOperationException">The tag-index subsystem is not available on this cluster.</exception>
    /// <exception cref="KeyNotFoundException">No tag index named <paramref name="indexName"/> is registered on this cluster.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the admin capability over the index's backing membership tree.</exception>
    Task<TreeTagReconcileReport> ReconcileTagIndexAsync(
        string indexName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Triggers an out-of-cycle tombstone-compaction pass scoped to a single physical
    /// shard of <paramref name="treeId"/>, after authorizing whole-tree
    /// <see cref="LatticeOperation.Admin"/> over the tree fail-closed. Wraps the public
    /// operator-tooling trigger, which bypasses the per-shard cooldown gate that the
    /// background policy trigger enforces. Compaction reaps only tombstones and
    /// TTL-expired entries, never live data, so it is mutating but non-destructive;
    /// it is online (no tree pause), idempotent, and reminder-durable.
    /// </summary>
    /// <param name="treeId">The tree whose shard to compact. Must not be <c>null</c> or empty.</param>
    /// <param name="shardIndex">The physical shard index resolved from the tree's shard map.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The trigger result, pairing the shard with whether the coordinator accepted the pass.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the admin capability over the tree.</exception>
    Task<TreeCompactionTriggerResult> TriggerShardCompactionAsync(
        string treeId,
        int shardIndex,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a tree's effective durable-history retention policy - the resolved
    /// <see cref="TreeHistoryRetentionMode"/> and the age-bound window - after authorizing
    /// whole-tree <see cref="LatticeOperation.Read"/> over the tree fail-closed. A pure
    /// read with no side effects: it reflects the persisted per-tree override, falling
    /// back to the documented defaults (<see cref="TreeHistoryRetentionMode.MetadataOnly"/>,
    /// no age bound) when none is set.
    /// </summary>
    /// <param name="treeId">The tree whose retention policy to read. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's effective history retention policy.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the read capability over the tree.</exception>
    Task<TreeHistoryRetention> GetHistoryRetentionAsync(
        string treeId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets or clears a tree's durable-history retention policy, after authorizing
    /// whole-tree <see cref="LatticeOperation.Admin"/> over the tree fail-closed. Each
    /// argument is independent: pass <c>null</c> for <paramref name="mode"/> to fall back
    /// to the default (<see cref="TreeHistoryRetentionMode.MetadataOnly"/>), or <c>null</c>
    /// for <paramref name="window"/> to remove the age bound. The override is persisted on
    /// the tree's registry entry and survives silo restarts. This configures retention
    /// only - it never trips a view rebuild and is absorbed forward (already-written rows
    /// keep their stamped shape; new rows adopt the new policy). Returns the effective
    /// policy read back after the change.
    /// </summary>
    /// <param name="treeId">The tree whose retention policy to set. Must not be <c>null</c> or empty.</param>
    /// <param name="mode">The retention mode for LWW value bytes, or <c>null</c> to clear the override.</param>
    /// <param name="window">The age after which a revision row expires, or <c>null</c> for no age bound. Must be strictly positive when supplied.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tree's effective history retention policy after the change.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty, or <paramref name="window"/> is not strictly positive.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller lacks the admin capability over the tree.</exception>
    Task<TreeHistoryRetention> SetHistoryRetentionAsync(
        string treeId,
        TreeHistoryRetentionMode? mode,
        TimeSpan? window,
        CancellationToken cancellationToken = default);
}
