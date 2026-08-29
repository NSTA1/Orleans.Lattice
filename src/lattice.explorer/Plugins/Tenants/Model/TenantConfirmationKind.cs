namespace Orleans.Lattice.Explorer.Tenants;

/// <summary>
/// Which destructive operation a pending confirmation is guarding. Every
/// irreversible or service-affecting action on the Tenants surface routes
/// through one of these, so none of them can fire from a single click.
/// </summary>
public enum TenantConfirmationKind
{
    /// <summary>Nothing is awaiting confirmation.</summary>
    None = 0,

    /// <summary>
    /// Deleting a tenant, which cascades to every tree it owns and is
    /// irreversible.
    /// </summary>
    Delete = 1,

    /// <summary>
    /// Suspending a tenant, which refuses its data-plane operations until an
    /// operator resumes it. Its trees remain intact, so this is reversible - but
    /// it takes a live tenant offline, so it is still confirmed.
    /// </summary>
    Suspend = 2,

    /// <summary>
    /// Revoking a subject's tenant-admin authority. Removing the last admin
    /// subject is refused by the cluster.
    /// </summary>
    RemoveAdminSubject = 3,

    /// <summary>
    /// Withdrawing an active cross-tenant grant, which closes it terminally and
    /// removes the access it was authorizing.
    /// </summary>
    RevokeGrant = 4,

    /// <summary>
    /// Rejecting a pending cross-tenant grant, which closes it terminally. The
    /// offer cannot be un-rejected; the granter must offer again.
    /// </summary>
    RejectGrant = 5,

    /// <summary>
    /// Replacing a tenant's allowed region set in a way that revokes a region it
    /// is still resident in. The cluster refuses that, so the confirmation says
    /// so before the call.
    /// </summary>
    RevokeRegion = 6,
}
