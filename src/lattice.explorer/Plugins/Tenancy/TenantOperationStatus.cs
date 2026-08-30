namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The outcome classification of a tenancy operation. Every refusal the
/// tenant-administration facades document gets its own member, so a panel can
/// say what actually happened - "that region is still resident", "that would
/// leave the tenant with no admins" - instead of collapsing every refusal into
/// one grey failure.
/// <para>
/// Client-side classification is presentational only: the cluster remains the
/// sole enforcement point, so an action must still handle any of these at
/// runtime whatever an advisory gate said beforehand.
/// </para>
/// </summary>
public enum TenantOperationStatus
{
    /// <summary>The operation completed successfully.</summary>
    Succeeded = 0,

    /// <summary>
    /// The server refused an authenticated caller: the caller is neither the
    /// platform operator nor a live admin subject of the tenant it named. Some
    /// reads deliberately answer <see cref="NotFound"/> instead, so tenant
    /// existence cannot be probed.
    /// </summary>
    Denied = 1,

    /// <summary>
    /// The server accepted no credential at all, so the caller must sign in.
    /// Distinct from <see cref="Denied"/> because it is recoverable: the shell
    /// offers a sign-in rather than an inert refusal.
    /// </summary>
    AuthenticationRequired = 2,

    /// <summary>
    /// The cluster does not serve this surface: the tenancy add-on, or the
    /// specific optional facade the operation needs, is not registered. The
    /// caller cannot sign in for it and cannot be granted it, so a tenancy
    /// surface degrades to nothing rather than showing an error.
    /// </summary>
    Unavailable = 3,

    /// <summary>
    /// The tenant is not registered - or the caller may not see it, which some
    /// reads deliberately report identically so the call cannot be used to
    /// probe for tenant existence.
    /// </summary>
    NotFound = 4,

    /// <summary>The tenant being created is already registered.</summary>
    AlreadyExists = 5,

    /// <summary>
    /// The operation targeted the reserved default tenant, which can never be
    /// its target: the default tenant cannot be suspended, deleted, or have its
    /// admin subjects or cross-tenant grants edited.
    /// </summary>
    ReservedTenant = 6,

    /// <summary>
    /// A region was outside the tenant's operator-authorized allowed set, or an
    /// allowed region was revoked while the tenant is still resident in it.
    /// Residency must stay a subset of the allowed set.
    /// </summary>
    RegionNotAllowed = 7,

    /// <summary>
    /// The residency change would have removed the tenant's last resident
    /// region. A tenant must remain resident somewhere, so the caller must add
    /// a region before removing this one.
    /// </summary>
    LastRegion = 8,

    /// <summary>
    /// The change would have removed the tenant's last admin subject, leaving
    /// nobody able to administer it. The caller must add the replacement first.
    /// </summary>
    LastAdminSubject = 9,

    /// <summary>
    /// No cross-tenant grant exists between the two tenants over the named
    /// scope, so there is nothing to approve, reject, or revoke. An unregistered
    /// granting tenant is reported identically, so grants cannot be used to
    /// probe for tenant existence.
    /// </summary>
    GrantNotFound = 10,

    /// <summary>
    /// The cross-tenant grant is not in a state the requested transition can be
    /// applied from - approving a grant that is not pending, revoking one that
    /// is not active, or re-offering terms over a live grant.
    /// </summary>
    GrantTransitionRejected = 11,

    /// <summary>
    /// The cluster refused a well-formed request on the state it holds, without
    /// the transport preserving which of the specific precondition refusals
    /// above it was.
    /// <para>
    /// The gRPC binding maps every precondition breach onto a single
    /// <c>FailedPrecondition</c> code and carries the specific reason only in
    /// the human-readable message, so a caller reached over the wire lands here
    /// rather than on <see cref="ReservedTenant"/>,
    /// <see cref="RegionNotAllowed"/>, <see cref="LastRegion"/>,
    /// <see cref="LastAdminSubject"/>, or
    /// <see cref="GrantTransitionRejected"/>. Those five remain the
    /// classification of the facade's own typed refusals and are what a caller
    /// holding the facade directly observes. Render
    /// <see cref="TenantOperationResult.Message"/>, which carries the server's
    /// specific reason verbatim.
    /// </para>
    /// </summary>
    PreconditionFailed = 12,

    /// <summary>
    /// The request itself was malformed - an empty tenant id, subject id,
    /// scope, or region - so it was rejected before any state was consulted.
    /// </summary>
    InvalidRequest = 13,

    /// <summary>
    /// The operation failed for a transport or server reason none of the above
    /// describes.
    /// </summary>
    Failed = 14,
}
