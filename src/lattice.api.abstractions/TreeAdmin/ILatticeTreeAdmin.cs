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
}
