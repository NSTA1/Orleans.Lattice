using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Maps between the transport-agnostic cross-tenant grant contract types in
/// <c>Orleans.Lattice.Api.Abstractions</c> and the tenancy engine's own grant
/// types. The contract package deliberately does not reference the tenancy
/// add-on, so this is the single seam at which the two vocabularies meet -
/// mirroring the sibling <see cref="TenantQuotasMapping"/>.
/// </summary>
internal static class TenantGrantMapping
{
    /// <summary>
    /// Maps a contract operation set onto the engine's flags. Bits this build does
    /// not recognise are dropped rather than forwarded, so a newer client can only
    /// ever narrow what a grant authorizes on an older server, never widen it.
    /// </summary>
    /// <param name="access">The contract operation set.</param>
    /// <returns>The engine operation flags.</returns>
    internal static TenantGrantOperations ToEngine(TenantGrantAccess access)
    {
        var operations = TenantGrantOperations.None;
        if ((access & TenantGrantAccess.Read) != 0)
        {
            operations |= TenantGrantOperations.Read;
        }

        if ((access & TenantGrantAccess.Write) != 0)
        {
            operations |= TenantGrantOperations.Write;
        }

        return operations;
    }

    /// <summary>Maps the engine's operation flags onto the contract operation set.</summary>
    /// <param name="operations">The engine operation flags.</param>
    /// <returns>The contract operation set.</returns>
    internal static TenantGrantAccess ToContract(TenantGrantOperations operations)
    {
        var access = TenantGrantAccess.None;
        if ((operations & TenantGrantOperations.Read) != 0)
        {
            access |= TenantGrantAccess.Read;
        }

        if ((operations & TenantGrantOperations.Write) != 0)
        {
            access |= TenantGrantAccess.Write;
        }

        return access;
    }

    /// <summary>Maps a contract lifecycle state onto the engine's grant state.</summary>
    /// <param name="state">The contract lifecycle state.</param>
    /// <returns>The engine grant state.</returns>
    internal static TenantGrantState ToEngine(TenantGrantLifecycleState state) => state switch
    {
        TenantGrantLifecycleState.Active => TenantGrantState.Active,
        TenantGrantLifecycleState.Pending => TenantGrantState.Pending,
        TenantGrantLifecycleState.Rejected => TenantGrantState.Rejected,
        TenantGrantLifecycleState.Revoked => TenantGrantState.Revoked,

        // An unrecognised state can only come from a newer client. Map it to the
        // terminal, access-denying state rather than guessing, so no unknown value
        // can be steered into the one state that authorizes.
        _ => TenantGrantState.Revoked,
    };

    /// <summary>Maps the engine's grant state onto the contract lifecycle state.</summary>
    /// <param name="state">The engine grant state.</param>
    /// <returns>The contract lifecycle state.</returns>
    internal static TenantGrantLifecycleState ToContract(TenantGrantState state) => state switch
    {
        TenantGrantState.Active => TenantGrantLifecycleState.Active,
        TenantGrantState.Pending => TenantGrantLifecycleState.Pending,
        TenantGrantState.Rejected => TenantGrantLifecycleState.Rejected,
        TenantGrantState.Revoked => TenantGrantLifecycleState.Revoked,

        // An unrecognised state can only come from a newer peer's record. Report
        // it as terminal rather than as the one state that authorizes, so a
        // surface reading this report can never present it as live access.
        _ => TenantGrantLifecycleState.Revoked,
    };

    /// <summary>
    /// Projects one engine grant held by <paramref name="granterTenantId"/> onto
    /// the contract descriptor.
    /// </summary>
    /// <param name="granterTenantId">The tenant whose record holds the grant.</param>
    /// <param name="grant">The engine grant. Its grantee must be a tenant id.</param>
    /// <returns>The contract descriptor.</returns>
    internal static TenantGrantDescriptor Describe(string granterTenantId, CrossTenantGrant grant) =>
        new()
        {
            GranterTenantId = granterTenantId,
            GranteeTenantId = grant.Grantee,
            Scope = grant.Scope,
            Operations = ToContract(grant.Operations),
            State = ToContract(grant.State),
            GrantId = grant.GrantId,
        };
}
