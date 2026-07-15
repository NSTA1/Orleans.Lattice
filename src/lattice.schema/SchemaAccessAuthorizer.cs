using Orleans.Lattice;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The schema-management authorization seam. Given the resolved caller and a
/// target tree id, it consults the registered core <see cref="ILatticeAccessGate"/>
/// for the <see cref="LatticeOperation.SchemaAdmin"/> capability (policy / version /
/// remediation mutations) or ordinary <see cref="LatticeOperation.Read"/> authority
/// (inspect verbs and the compliance audit), and fails closed by throwing
/// <see cref="LatticeAuthorizationDeniedException"/> when the request is not
/// authorized. It is the single choke point the remote schema control facade
/// consults before touching the in-process schema admin surface.
/// </summary>
/// <remarks>
/// <para>
/// <b>Reuses the existing enforcement primitive unchanged.</b> Every check is
/// delegated to the shared <see cref="LatticeAccessGateEnforcement"/> helper the
/// data plane already uses, so the schema capabilities inherit its behaviour
/// exactly: the <b>system-origin</b> gate-bypass, the <b>zero-cost default</b>
/// short-circuit when only the no-op core gate is registered, caller-subject
/// resolution through the membership seam, and the <b>bootstrap-administrator
/// break-glass</b> honoured by the gate itself.
/// </para>
/// <para>
/// Schema operations are whole-tree scoped (they address a tree, never a single
/// key), so every check is a whole-tree check: a partial / filtered allow is
/// refused, fail-closed, exactly as an admin operation is.
/// </para>
/// </remarks>
internal sealed class SchemaAccessAuthorizer
{
    private readonly ILatticeAccessGate _gate;
    private readonly ILatticeMembershipContext? _membership;

    /// <summary>
    /// Initializes a new <see cref="SchemaAccessAuthorizer"/>.
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
    public SchemaAccessAuthorizer(ILatticeAccessGate gate, ILatticeMembershipContext? membership = null)
    {
        ArgumentNullException.ThrowIfNull(gate);
        _gate = gate;
        _membership = membership;
    }

    /// <summary>
    /// Authorizes a schema-management <b>mutation</b> (set / clear policy, version
    /// config changes, remediation) over <paramref name="treeId"/> for the current
    /// caller, throwing <see cref="LatticeAuthorizationDeniedException"/> when the
    /// <see cref="LatticeOperation.SchemaAdmin"/> capability is not granted over the
    /// whole tree.
    /// </summary>
    /// <param name="treeId">The governed tree id.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the mutation is authorized.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to manage the tree's schema.</exception>
    public ValueTask AuthorizeManageAsync(string treeId, CancellationToken cancellationToken = default) =>
        LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
            _gate, _membership, treeId, LatticeOperation.SchemaAdmin, cancellationToken);

    /// <summary>
    /// Authorizes a schema-management <b>read</b> (inspect policy / version config /
    /// dead letters / remediation status, or the compliance audit) over
    /// <paramref name="treeId"/> for the current caller, throwing
    /// <see cref="LatticeAuthorizationDeniedException"/> when
    /// <see cref="LatticeOperation.Read"/> authority is not granted over the whole
    /// tree.
    /// </summary>
    /// <param name="treeId">The governed tree id.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the read is authorized.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to read the tree's schema.</exception>
    public ValueTask AuthorizeReadAsync(string treeId, CancellationToken cancellationToken = default) =>
        LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
            _gate, _membership, treeId, LatticeOperation.Read, cancellationToken);

    /// <summary>
    /// Probes whether the current caller may perform schema-management
    /// <b>mutations</b> over <paramref name="treeId"/>, returning <c>true</c> when
    /// authorized and <c>false</c> when denied. Never throws for a plain
    /// authorization denial; other failures propagate. Read-only, no side effects.
    /// </summary>
    /// <param name="treeId">The governed tree id.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns><c>true</c> when the caller may manage the tree's schema; otherwise <c>false</c>.</returns>
    public async ValueTask<bool> IsManageAuthorizedAsync(string treeId, CancellationToken cancellationToken = default)
    {
        try
        {
            await AuthorizeManageAsync(treeId, cancellationToken);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
    }

    /// <summary>
    /// Probes whether the current caller may perform schema-management <b>reads</b>
    /// over <paramref name="treeId"/>, returning <c>true</c> when authorized and
    /// <c>false</c> when denied. Never throws for a plain authorization denial;
    /// other failures propagate. Read-only, no side effects.
    /// </summary>
    /// <param name="treeId">The governed tree id.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns><c>true</c> when the caller may read the tree's schema; otherwise <c>false</c>.</returns>
    public async ValueTask<bool> IsReadAuthorizedAsync(string treeId, CancellationToken cancellationToken = default)
    {
        try
        {
            await AuthorizeReadAsync(treeId, cancellationToken);
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
    }
}
