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
}
