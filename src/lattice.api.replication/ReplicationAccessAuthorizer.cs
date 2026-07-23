using Orleans.Lattice;

namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// The replication-configuration authorization seam. Given the resolved caller
/// and a target tree id, it consults the registered core
/// <see cref="ILatticeAccessGate"/> for the dedicated
/// <see cref="LatticeOperation.Replication"/> capability and fails closed by
/// throwing <see cref="LatticeAuthorizationDeniedException"/> when the request
/// is not authorized. It is the single choke point the
/// <see cref="LatticeReplicationControl"/> facade consults before authoring or
/// reading replication config, mirroring the sibling
/// <c>BackupAccessAuthorizer</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Reuses the existing enforcement primitive unchanged.</b> Every check is
/// delegated to the shared <see cref="LatticeAccessGateEnforcement"/> helper the
/// data plane already uses, so the replication capability inherits its
/// behaviour exactly: the <b>system-origin</b> gate-bypass, the <b>zero-cost
/// default</b> short-circuit when only the no-op core gate is registered, and
/// caller-subject resolution through the membership seam. The
/// <b>bootstrap-administrator break-glass</b> is honoured by the gate itself, so
/// it applies here without any additional wiring.
/// </para>
/// <para>
/// A tree is authorized at its <b>root</b>: replication configuration is a
/// whole-tree control-plane operation, so a partial / filtered allow is refused
/// (fail-closed), exactly as an admin or bulk-load operation is. Anonymous
/// callers resolve to <see cref="LatticeSubject.Anonymous"/> and are denied by
/// default unless a policy explicitly grants them the capability.
/// </para>
/// </remarks>
internal sealed class ReplicationAccessAuthorizer
{
    private readonly ILatticeAccessGate _gate;
    private readonly ILatticeMembershipContext? _membership;

    /// <summary>
    /// Initializes a new <see cref="ReplicationAccessAuthorizer"/>.
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
    public ReplicationAccessAuthorizer(ILatticeAccessGate gate, ILatticeMembershipContext? membership = null)
    {
        ArgumentNullException.ThrowIfNull(gate);
        _gate = gate;
        _membership = membership;
    }

    /// <summary>
    /// Authorizes configuring replication (enable / disable / read) for
    /// <paramref name="treeId"/> for the current caller, throwing
    /// <see cref="LatticeAuthorizationDeniedException"/> when the
    /// <see cref="LatticeOperation.Replication"/> capability is not granted over
    /// the whole tree.
    /// </summary>
    /// <param name="treeId">The target tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the operation is authorized.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to configure replication for the tree.</exception>
    public ValueTask AuthorizeAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
            _gate, _membership, treeId, LatticeOperation.Replication, cancellationToken);
    }

    /// <summary>
    /// Probes the fail-closed <see cref="LatticeOperation.Replication"/> authority
    /// over a tree with no side effects, translating the gate's throw-on-deny into
    /// a boolean. Used by the permission-scoped config listing so a denied tree is
    /// hidden rather than faulting the whole enumeration.
    /// </summary>
    /// <param name="treeId">The target tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns><c>true</c> when the caller holds the capability; <c>false</c> when the gate denies it.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <c>null</c> or empty.</exception>
    public async ValueTask<bool> IsAuthorizedAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        try
        {
            await AuthorizeAsync(treeId, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
    }
}
