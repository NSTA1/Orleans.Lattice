namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Thrown when a cross-tenant grant lifecycle operation is not legal from the
/// grant's current state: for example approving a grant that has already been
/// revoked, revoking one that was never approved, or re-offering new terms over a
/// live agreement the grantee approved on its existing terms. Carries the state
/// the grant was actually in and the state the call asked for. A transport
/// binding surfaces it as a failed-precondition outcome, matching the sibling
/// <see cref="TenantRegionNotAllowedException"/> and
/// <see cref="TenantLastAdminSubjectException"/> guards.
/// </summary>
/// <remarks>
/// The legal transitions are approve
/// (<see cref="TenantGrantLifecycleState.Pending"/> to
/// <see cref="TenantGrantLifecycleState.Active"/>), reject
/// (<see cref="TenantGrantLifecycleState.Pending"/> to
/// <see cref="TenantGrantLifecycleState.Rejected"/>), and revoke
/// (<see cref="TenantGrantLifecycleState.Active"/> to
/// <see cref="TenantGrantLifecycleState.Revoked"/>). Asking for the state a grant
/// is already in is <em>not</em> an error - it is reported as an idempotent
/// no-op - so a retried call over an unreliable transport never raises this.
/// </remarks>
public sealed class TenantGrantTransitionException : Exception
{
    /// <summary>Initialises the exception for the refused transition.</summary>
    /// <param name="granterTenantId">The tenant that offered the grant.</param>
    /// <param name="granteeTenantId">The tenant the grant was offered to.</param>
    /// <param name="scope">The scope the grant covers.</param>
    /// <param name="currentState">The state the grant is actually in.</param>
    /// <param name="requestedState">The state the refused call asked for.</param>
    public TenantGrantTransitionException(
        string granterTenantId,
        string granteeTenantId,
        string scope,
        TenantGrantLifecycleState currentState,
        TenantGrantLifecycleState requestedState)
        : base($"The cross-tenant grant from tenant '{granterTenantId}' to tenant '{granteeTenantId}' "
            + $"covering scope '{scope}' is '{currentState}' and cannot be moved to '{requestedState}'.")
    {
        GranterTenantId = granterTenantId;
        GranteeTenantId = granteeTenantId;
        Scope = scope;
        CurrentState = currentState;
        RequestedState = requestedState;
    }

    /// <summary>The tenant that offered the grant.</summary>
    public string GranterTenantId { get; }

    /// <summary>The tenant the grant was offered to.</summary>
    public string GranteeTenantId { get; }

    /// <summary>The scope the grant covers.</summary>
    public string Scope { get; }

    /// <summary>The state the grant is actually in.</summary>
    public TenantGrantLifecycleState CurrentState { get; }

    /// <summary>The state the refused call asked for.</summary>
    public TenantGrantLifecycleState RequestedState { get; }
}
