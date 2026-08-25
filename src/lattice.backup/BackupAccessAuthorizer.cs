using Orleans.Lattice;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The backup / restore authorization seam. Given the resolved caller and a
/// <see cref="BackupScopeSelector"/> (tree, prefix, or key), it consults the registered
/// core <see cref="ILatticeAccessGate"/> for the dedicated
/// <see cref="LatticeOperation.Backup"/> (capture) or
/// <see cref="LatticeOperation.Restore"/> (author / bulk-load) capability and
/// fails closed by throwing <see cref="LatticeAuthorizationDeniedException"/>
/// when the request is not authorized. It is the single choke point the later
/// capture and restore call sites consult before touching data.
/// </summary>
/// <remarks>
/// <para>
/// <b>Reuses the existing enforcement primitive unchanged.</b> Every check is
/// delegated to the shared <see cref="LatticeAccessGateEnforcement"/> helper the
/// data plane already uses, so the backup / restore capabilities inherit its
/// behaviour exactly: the <b>system-origin</b> gate-bypass (an
/// infrastructure-authored, <c>EnterSystemOrigin</c>-scoped turn is never
/// gated), the <b>zero-cost default</b> short-circuit when only the no-op core
/// gate is registered, and caller-subject resolution through the membership
/// seam. The <b>bootstrap-administrator break-glass</b> is honoured by the gate
/// itself (a bootstrap admin is allowed for every operation, the two new ones
/// included), so it applies here without any additional wiring.
/// </para>
/// <para>
/// A scope is authorized at its <b>root</b>: a tree scope is a whole-tree check
/// (a partial / filtered allow is refused, fail-closed, exactly as a bulk-load
/// or admin operation is), and a prefix or key scope is a point check at the
/// prefix / key, so a matching allow rule at that scope (or a broader one)
/// authorizes it while deny-overrides and prefix specificity are honoured by the
/// gate's own evaluation. Consistent with the high-privilege nature of
/// <see cref="LatticeOperation.Backup"/>, the capture capability is evaluated as
/// its own operation and never narrowed by the per-key read key-filter that an
/// ordinary read honours.
/// </para>
/// </remarks>
internal sealed class BackupAccessAuthorizer
{
    private readonly ILatticeAccessGate _gate;
    private readonly ILatticeMembershipContext? _membership;
    private readonly ILatticeBackupTenantScope _tenantScope;

    /// <summary>
    /// Initializes a new <see cref="BackupAccessAuthorizer"/>.
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
    /// <param name="tenantScope">
    /// The tenancy seam consulted to keep a capture / restore inside the active
    /// tenant's namespace, or <c>null</c> when none is registered (the inert
    /// <see cref="NullLatticeBackupTenantScope"/> is used, so no tenant check runs).
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="gate"/> is <c>null</c>.</exception>
    public BackupAccessAuthorizer(
        ILatticeAccessGate gate,
        ILatticeMembershipContext? membership = null,
        ILatticeBackupTenantScope? tenantScope = null)
    {
        ArgumentNullException.ThrowIfNull(gate);
        _gate = gate;
        _membership = membership;
        _tenantScope = tenantScope ?? NullLatticeBackupTenantScope.Instance;
    }

    /// <summary>
    /// Authorizes capturing (backing up) <paramref name="scope"/> for the current
    /// caller, throwing <see cref="LatticeAuthorizationDeniedException"/> when the
    /// <see cref="LatticeOperation.Backup"/> capability is not granted over the
    /// scope.
    /// </summary>
    /// <param name="scope">The tree / prefix / key scope to capture.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the capture is authorized.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to back up the scope.</exception>
    public ValueTask AuthorizeBackupAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default) =>
        AuthorizeAsync(LatticeOperation.Backup, scope, cancellationToken);

    /// <summary>
    /// Authorizes restoring (authoring a backup) into <paramref name="scope"/> for
    /// the current caller, throwing <see cref="LatticeAuthorizationDeniedException"/>
    /// when the <see cref="LatticeOperation.Restore"/> capability is not granted
    /// over the scope. A restore grant subsumes the target-scope write / bulk-load
    /// authority, so no separate write grant is consulted.
    /// </summary>
    /// <param name="scope">The tree / prefix / key scope to restore into.</param>
    /// <param name="cancellationToken">Cancels the authorization.</param>
    /// <returns>A task that completes when the restore is authorized.</returns>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to restore the scope.</exception>
    public ValueTask AuthorizeRestoreAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default) =>
        AuthorizeAsync(LatticeOperation.Restore, scope, cancellationToken);

    private ValueTask AuthorizeAsync(LatticeOperation operation, BackupScopeSelector scope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(scope);

        // Tenant isolation runs before the capability gate: when a tenancy add-on
        // is active, a capture / restore of a tree the active tenant does not own
        // is refused here, at the single choke point every capture and restore
        // entry point funnels through. When no tenancy add-on is registered the
        // scope is inert and this is a single branch with no further work.
        if (_tenantScope.IsActive)
        {
            if (operation == LatticeOperation.Backup)
            {
                _tenantScope.AuthorizeCapture(scope.TreeId);
            }
            else
            {
                _tenantScope.AuthorizeRestoreTarget(scope.TreeId);
            }
        }

        return scope.Kind switch
        {
            BackupScopeKind.WholeTree => LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
                _gate, _membership, scope.TreeId, operation, cancellationToken),
            BackupScopeKind.Prefix => LatticeAccessGateEnforcement.EnforcePointAsync(
                _gate, _membership, scope.TreeId, operation, scope.KeyOrPrefix!, cancellationToken),
            BackupScopeKind.Key => LatticeAccessGateEnforcement.EnforcePointAsync(
                _gate, _membership, scope.TreeId, operation, scope.KeyOrPrefix!, cancellationToken),
            _ => throw new ArgumentOutOfRangeException(
                nameof(scope), scope.Kind, "Unknown backup scope kind."),
        };
    }
}
