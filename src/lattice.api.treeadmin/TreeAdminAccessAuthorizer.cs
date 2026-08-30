using Orleans.Lattice;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The tree-administration diagnostics authorization seam. Given the resolved
/// caller and a target scope, it consults the registered core
/// <see cref="ILatticeAccessGate"/> for the capability a read-only diagnostics
/// operation requires - ordinary <see cref="LatticeOperation.Read"/> authority for
/// the per-tree inspection verbs, or the distinct
/// <see cref="LatticeOperation.Telemetry"/> capability for the cluster-wide storage
/// accounting summary - and fails closed by throwing
/// <see cref="LatticeAuthorizationDeniedException"/> when the request is not
/// authorized. It is the single choke point the tree-administration facade consults
/// before touching the in-process grain surface for a diagnostics read.
/// </summary>
/// <remarks>
/// <para>
/// <b>Reuses the existing enforcement primitive unchanged.</b> Every check is
/// delegated to the shared <see cref="LatticeAccessGateEnforcement"/> helper the
/// data plane already uses, so the diagnostics capabilities inherit its behaviour
/// exactly: the <b>system-origin</b> gate-bypass, the <b>zero-cost default</b>
/// short-circuit when only the no-op core gate is registered, caller-subject
/// resolution through the membership seam, and the <b>bootstrap-administrator
/// break-glass</b> honoured by the gate itself.
/// </para>
/// <para>
/// Diagnostics reads are whole-tree scoped (they address a tree or the whole
/// cluster, never a single key), so every check is a whole-tree check: a partial /
/// filtered allow is refused, fail-closed, exactly as an admin operation is. The
/// cluster-wide storage accounting summary addresses no single tree, so it is
/// authorized over the cluster-wide sentinel (<c>"*"</c>, <see cref="ClusterWideScope"/>) -
/// the same scope <c>Orleans.Lattice.Auth.LatticeScope.ClusterWide()</c> authors, so
/// a grant written the documented way is the one that authorizes it (issue #1795).
/// The gate treats a request on the sentinel as a scopeless cluster-wide capability
/// request rather than a data-plane one - a data-plane read or write always names a
/// real tree - and routes it through control-plane isolation, which denies an
/// unmatched request regardless of the data-plane <c>DefaultEffect</c>. The elevated
/// all-tree observability capability therefore still fails closed under
/// <c>DefaultEffect = Allow</c> instead of being inherited by any caller.
/// </para>
/// </remarks>
internal sealed class TreeAdminAccessAuthorizer
{
    /// <summary>
    /// The cluster-wide sentinel scope the cluster-wide storage accounting summary
    /// authorizes against. Mirrors
    /// <c>Orleans.Lattice.Auth.LatticeScope.ClusterWideTreeId</c> (<c>"*"</c>)
    /// without taking a dependency on the auth add-on: the facade never references
    /// it, so the id is repeated locally as a plain constant. A request on the
    /// sentinel can only mean a scopeless cluster-wide capability, so the access gate
    /// routes it through control-plane isolation and denies an unmatched request
    /// independently of the data-plane <c>DefaultEffect</c>; the cluster-telemetry
    /// capability therefore fails closed under <c>DefaultEffect = Allow</c> exactly as
    /// an admin control-plane operation does, while the documented
    /// <c>LatticeScope.ClusterWide()</c> grant is honoured. When no auth gate is
    /// registered the no-op gate still short-circuits to allow at zero cost, so the
    /// auth-off surface is unchanged.
    /// </summary>
    internal const string ClusterWideScope = "*";

    private readonly ILatticeAccessGate _gate;
    private readonly ILatticeMembershipContext? _membership;

    /// <summary>
    /// Initializes a new <see cref="TreeAdminAccessAuthorizer"/>.
    /// </summary>
    /// <param name="gate">
    /// The registered core access gate to consult. Must not be <c>null</c>. In a
    /// host with no authorization add-on this is the no-op gate, so every check
    /// short-circuits to allow at zero cost.
    /// </param>
    /// <param name="membership">
    /// The membership context used to resolve the caller subject, or <c>null</c>
    /// when none is registered (every caller then resolves to
    /// <see cref="LatticeSubject.Anonymous"/>).
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="gate"/> is <c>null</c>.</exception>
    public TreeAdminAccessAuthorizer(ILatticeAccessGate gate, ILatticeMembershipContext? membership = null)
    {
        ArgumentNullException.ThrowIfNull(gate);
        _gate = gate;
        _membership = membership;
    }

    /// <summary>
    /// Authorizes a per-tree diagnostics <b>read</b> (hotness, diagnostics, shard-map
    /// inspection, projection digest, tree statistics) over <paramref name="treeId"/>
    /// for the current caller, throwing <see cref="LatticeAuthorizationDeniedException"/>
    /// when <see cref="LatticeOperation.Read"/> authority is not granted over the whole
    /// tree.
    /// </summary>
    /// <param name="treeId">The tree being read. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the read is authorized.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree.</exception>
    public ValueTask AuthorizeTreeReadAsync(string treeId, CancellationToken cancellationToken = default) =>
        LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
            _gate, _membership, treeId, LatticeOperation.Read, cancellationToken);

    /// <summary>
    /// Authorizes the cluster-wide storage accounting <b>read</b> for the current
    /// caller, throwing <see cref="LatticeAuthorizationDeniedException"/> when the
    /// distinct <see cref="LatticeOperation.Telemetry"/> capability is not granted over
    /// the cluster-wide sentinel scope (<see cref="ClusterWideScope"/>). That is the
    /// scope <c>LatticeScope.ClusterWide()</c> authors, and because a request on the
    /// sentinel can only be a scopeless cluster-wide capability request, the gate
    /// routes it through control-plane isolation and denies an unmatched request
    /// regardless of the data-plane <c>DefaultEffect</c>, so this elevated all-tree
    /// observability capability fails closed under <c>DefaultEffect = Allow</c>.
    /// </summary>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the read is authorized.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized for cluster telemetry.</exception>
    public ValueTask AuthorizeClusterTelemetryAsync(CancellationToken cancellationToken = default) =>
        LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
            _gate, _membership, ClusterWideScope, LatticeOperation.Telemetry, cancellationToken);

    /// <summary>
    /// Authorizes a per-tree lifecycle <b>mutation</b> (tree creation, alias
    /// assignment, per-tree configuration update) over <paramref name="treeId"/> for
    /// the current caller, throwing <see cref="LatticeAuthorizationDeniedException"/>
    /// when <see cref="LatticeOperation.Admin"/> authority is not granted over the
    /// whole tree. Mirrors the schema facade's manage gate: a partial / filtered allow
    /// is refused, fail-closed.
    /// </summary>
    /// <param name="treeId">The tree being administered. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the mutation is authorized.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to administer the tree.</exception>
    public ValueTask AuthorizeTreeAdminAsync(string treeId, CancellationToken cancellationToken = default) =>
        LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
            _gate, _membership, treeId, LatticeOperation.Admin, cancellationToken);

    /// <summary>
    /// Authorizes an <b>irreversible or structural whole-tree</b> operation (drop /
    /// purge, reshard, resize, WAL placement move) over <paramref name="treeId"/>
    /// for the current caller, throwing <see cref="LatticeAuthorizationDeniedException"/>
    /// when the distinct <see cref="LatticeOperation.TreeLifecycle"/> capability is not
    /// granted over the whole tree. Deliberately gated on <see cref="LatticeOperation.TreeLifecycle"/>
    /// rather than <see cref="LatticeOperation.Admin"/>: routine administration must
    /// never silently confer the authority to destroy or rebuild a tree, and a
    /// cluster-wide <c>Admin</c> grant must not reach these verbs. A partial /
    /// filtered allow is refused, fail-closed.
    /// </summary>
    /// <param name="treeId">The tree being administered. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the operation is authorized.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized for the tree's lifecycle operations.</exception>
    public ValueTask AuthorizeTreeLifecycleAsync(string treeId, CancellationToken cancellationToken = default) =>
        LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
            _gate, _membership, treeId, LatticeOperation.TreeLifecycle, cancellationToken);

    /// <summary>
    /// Authorizes a whole-tree <b>bulk-load (tree creation)</b> over
    /// <paramref name="treeId"/> for the current caller, throwing
    /// <see cref="LatticeAuthorizationDeniedException"/> when the distinct
    /// <see cref="LatticeOperation.BulkLoad"/> capability is not granted over the whole
    /// tree. Deliberately gated on <see cref="LatticeOperation.BulkLoad"/> rather than
    /// <see cref="LatticeOperation.Write"/>: seeding a whole tree bottom-up is a
    /// structural operation distinct from per-key writes, so a per-key write grant must
    /// not confer it. A partial / filtered allow is refused, fail-closed.
    /// </summary>
    /// <param name="treeId">The tree being bulk-loaded. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the bulk-load is authorized.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to bulk-load the tree.</exception>
    public ValueTask AuthorizeBulkLoadAsync(string treeId, CancellationToken cancellationToken = default) =>
        LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
            _gate, _membership, treeId, LatticeOperation.BulkLoad, cancellationToken);

    /// <summary>
    /// Authorizes a whole-tree <b>restore</b> (installing a captured backup into the
    /// tree, or reverting such a restore) over <paramref name="treeId"/> for the
    /// current caller, throwing <see cref="LatticeAuthorizationDeniedException"/> when
    /// the distinct <see cref="LatticeOperation.Restore"/> capability is not granted
    /// over the whole tree. Deliberately gated on <see cref="LatticeOperation.Restore"/>
    /// - the same capability the backup engine authorizes against - rather than
    /// <see cref="LatticeOperation.Admin"/> or <see cref="LatticeOperation.BulkLoad"/>:
    /// overwriting a tree from a backup is a distinct trust decision. A partial /
    /// filtered allow is refused, fail-closed.
    /// </summary>
    /// <param name="treeId">The tree being restored. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the restore is authorized.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the tree.</exception>
    public ValueTask AuthorizeRestoreAsync(string treeId, CancellationToken cancellationToken = default) =>
        LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
            _gate, _membership, treeId, LatticeOperation.Restore, cancellationToken);

    /// <summary>
    /// Probes whether the current caller may perform per-tree lifecycle
    /// <b>mutations</b> over <paramref name="treeId"/>, returning <c>true</c> when
    /// authorized and <c>false</c> when denied. Never throws for a plain authorization
    /// denial; other failures propagate. Read-only, no side effects.
    /// </summary>
    /// <param name="treeId">The tree being probed. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns><c>true</c> when the caller may administer the tree; otherwise <c>false</c>.</returns>
    public async ValueTask<bool> IsTreeAdminAuthorizedAsync(string treeId, CancellationToken cancellationToken = default)
    {
        try
        {
            await AuthorizeTreeAdminAsync(treeId, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
    }

    /// <summary>
    /// Probes whether the current caller may perform <b>irreversible or structural</b>
    /// whole-tree operations (drop / purge, reshard, resize, WAL placement move) over
    /// <paramref name="treeId"/>, returning <c>true</c> when the distinct
    /// <see cref="LatticeOperation.TreeLifecycle"/> capability is granted and <c>false</c>
    /// when denied. Never throws for a plain authorization denial; other failures
    /// propagate. Read-only, no side effects.
    /// </summary>
    /// <param name="treeId">The tree being probed. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns><c>true</c> when the caller may perform the tree's lifecycle operations; otherwise <c>false</c>.</returns>
    public async ValueTask<bool> IsTreeLifecycleAuthorizedAsync(string treeId, CancellationToken cancellationToken = default)
    {
        try
        {
            await AuthorizeTreeLifecycleAsync(treeId, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
    }

    /// <summary>
    /// Probes whether the current caller may perform per-tree diagnostics <b>reads</b>
    /// over <paramref name="treeId"/>, returning <c>true</c> when authorized and
    /// <c>false</c> when denied. Never throws for a plain authorization denial; other
    /// failures propagate. Read-only, no side effects.
    /// </summary>
    /// <param name="treeId">The tree being probed. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns><c>true</c> when the caller may read the tree's diagnostics; otherwise <c>false</c>.</returns>
    public async ValueTask<bool> IsTreeReadAuthorizedAsync(string treeId, CancellationToken cancellationToken = default)
    {
        try
        {
            await AuthorizeTreeReadAsync(treeId, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
    }

    /// <summary>
    /// Probes whether the current caller may perform a whole-tree <b>bulk-load</b> over
    /// <paramref name="treeId"/>, returning <c>true</c> when the distinct
    /// <see cref="LatticeOperation.BulkLoad"/> capability is granted and <c>false</c>
    /// when denied. Never throws for a plain authorization denial; other failures
    /// propagate. Read-only, no side effects.
    /// </summary>
    /// <param name="treeId">The tree being probed. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns><c>true</c> when the caller may bulk-load the tree; otherwise <c>false</c>.</returns>
    public async ValueTask<bool> IsBulkLoadAuthorizedAsync(string treeId, CancellationToken cancellationToken = default)
    {
        try
        {
            await AuthorizeBulkLoadAsync(treeId, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
    }

    /// <summary>
    /// Probes whether the current caller may perform a whole-tree <b>restore</b> over
    /// <paramref name="treeId"/>, returning <c>true</c> when the distinct
    /// <see cref="LatticeOperation.Restore"/> capability is granted and <c>false</c>
    /// when denied. Never throws for a plain authorization denial; other failures
    /// propagate. Read-only, no side effects.
    /// </summary>
    /// <param name="treeId">The tree being probed. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns><c>true</c> when the caller may restore the tree; otherwise <c>false</c>.</returns>
    public async ValueTask<bool> IsRestoreAuthorizedAsync(string treeId, CancellationToken cancellationToken = default)
    {
        try
        {
            await AuthorizeRestoreAsync(treeId, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
    }
}
