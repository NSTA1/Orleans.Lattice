using Orleans.Lattice;
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
}
