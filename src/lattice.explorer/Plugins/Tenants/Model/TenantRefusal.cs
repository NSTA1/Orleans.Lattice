using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.Tenants;

/// <summary>
/// Turns a tenancy refusal into a sentence that says what actually happened.
/// <para>
/// The seam classifies every documented facade refusal into its own status, so a
/// surface can say "that region is still resident" or "that would leave the
/// tenant with no admins" instead of collapsing them into one grey failure. This
/// is where that classification is spent. Where the gRPC binding collapses the
/// typed precondition refusals onto a single code, the server's own message
/// carries the reason and is rendered verbatim rather than replaced with a
/// guess.
/// </para>
/// </summary>
/// <remarks>
/// Client classification is presentational. The cluster is the sole enforcement
/// point, so every one of these can arrive at runtime whatever an advisory gate
/// said beforehand (epic decision D6), and the surface renders it as a message
/// rather than an error.
/// </remarks>
public static class TenantRefusal
{
    /// <summary>
    /// The standing explanation of why revoking a resident region is refused,
    /// rendered wherever the allowed set is edited.
    /// </summary>
    public const string ResidentRegionRule =
        "A region cannot be removed from the allowed set while the tenant is still resident in it. "
        + "Drain the tenant's residency out of the region first, then revoke the authorization.";

    /// <summary>
    /// The standing explanation of why the last admin subject cannot be removed.
    /// </summary>
    public const string LastAdminSubjectRule =
        "A tenant must always keep at least one admin subject, or nobody could administer it. "
        + "Add the replacement first, then remove this one.";

    /// <summary>
    /// The status-banner modifier class for an outcome. A refusal the operator
    /// can act on is styled apart from a transport failure they cannot.
    /// </summary>
    /// <param name="status">The outcome to classify.</param>
    /// <returns>The modifier class.</returns>
    public static string ResultClass(TenantOperationStatus status) => status switch
    {
        TenantOperationStatus.Succeeded => "is-success",
        TenantOperationStatus.Denied or TenantOperationStatus.AuthenticationRequired => "is-denied",
        TenantOperationStatus.Unavailable => "is-unavailable",
        TenantOperationStatus.Failed => "is-failed",
        _ => "is-refused",
    };

    /// <summary>
    /// Describes any tenancy outcome. Every refusal the seam classifies gets its
    /// own sentence; the server's own message is appended where it carries the
    /// specific reason.
    /// </summary>
    /// <param name="result">The outcome to describe. Must not be <see langword="null"/>.</param>
    /// <returns>The sentence to render.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="result"/> is <see langword="null"/>.</exception>
    public static string Describe(TenantOperationResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return result.Status switch
        {
            TenantOperationStatus.Succeeded => result.Message,
            TenantOperationStatus.Denied =>
                "The cluster refused this operation for your account. " + result.Message,
            TenantOperationStatus.AuthenticationRequired =>
                "The connection carries no accepted credential. Sign in to administer tenants.",
            TenantOperationStatus.Unavailable =>
                "This cluster does not serve tenant administration. " + result.Message,
            TenantOperationStatus.NotFound =>
                "No such tenant is registered, or it is not visible to your account.",
            TenantOperationStatus.AlreadyExists => "That tenant is already registered.",
            TenantOperationStatus.ReservedTenant =>
                "That is the reserved default tenant. It cannot be suspended, deleted, or have its "
                + "admin subjects or cross-tenant grants edited.",
            TenantOperationStatus.RegionNotAllowed => ResidentRegionRule,
            TenantOperationStatus.LastRegion =>
                "That change would leave the tenant resident in no region at all. Add a region "
                + "before removing this one.",
            TenantOperationStatus.LastAdminSubject => LastAdminSubjectRule,
            TenantOperationStatus.GrantNotFound =>
                "No cross-tenant grant exists between those tenants over that scope, so there is "
                + "nothing to approve, reject, or revoke.",
            TenantOperationStatus.GrantTransitionRejected =>
                "The grant is not in a state that transition can be applied from. Only a pending "
                + "grant can be approved or rejected, and only an active one can be revoked. "
                + result.Message,
            TenantOperationStatus.PreconditionFailed =>
                "The cluster refused the request on the state it holds: " + result.Message,
            TenantOperationStatus.InvalidRequest => "The request was rejected as malformed. " + result.Message,
            _ => "The operation failed. " + result.Message,
        };
    }

    /// <summary>
    /// Describes an allowed-region authorization outcome, naming the
    /// still-resident rule specifically rather than letting it arrive as a
    /// generic precondition failure.
    /// </summary>
    /// <remarks>
    /// The gRPC binding maps every precondition breach onto one code, so a
    /// caller reached over the wire lands on
    /// <see cref="TenantOperationStatus.PreconditionFailed"/> rather than on
    /// <see cref="TenantOperationStatus.RegionNotAllowed"/>. On this surface the
    /// overwhelmingly likely precondition is the resident-region rule, so it is
    /// stated alongside the server's verbatim reason instead of being withheld
    /// until the operator guesses.
    /// </remarks>
    /// <param name="result">The outcome to describe. Must not be <see langword="null"/>.</param>
    /// <returns>The sentence to render.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="result"/> is <see langword="null"/>.</exception>
    public static string DescribeRegionChange(TenantOperationResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return result.Status switch
        {
            TenantOperationStatus.RegionNotAllowed => ResidentRegionRule,
            TenantOperationStatus.PreconditionFailed =>
                "The cluster refused the region change: " + result.Message + " " + ResidentRegionRule,
            _ => Describe(result),
        };
    }

    /// <summary>
    /// Describes an admin-subject change, naming the last-admin rule
    /// specifically rather than letting it arrive as a generic precondition
    /// failure.
    /// </summary>
    /// <param name="result">The outcome to describe. Must not be <see langword="null"/>.</param>
    /// <returns>The sentence to render.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="result"/> is <see langword="null"/>.</exception>
    public static string DescribeAdminChange(TenantOperationResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return result.Status switch
        {
            TenantOperationStatus.LastAdminSubject => LastAdminSubjectRule,
            TenantOperationStatus.PreconditionFailed =>
                "The cluster refused the change: " + result.Message + " " + LastAdminSubjectRule,
            _ => Describe(result),
        };
    }

    /// <summary>
    /// Describes a cross-tenant grant transition, naming the state machine
    /// specifically so a refused transition reads as "the grant was not in the
    /// state you thought" rather than as an unexplained failure.
    /// </summary>
    /// <param name="result">The outcome to describe. Must not be <see langword="null"/>.</param>
    /// <returns>The sentence to render.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="result"/> is <see langword="null"/>.</exception>
    public static string DescribeGrantTransition(TenantOperationResult result)
    {
        ArgumentNullException.ThrowIfNull(result);

        return result.Status switch
        {
            TenantOperationStatus.PreconditionFailed =>
                "The cluster refused the grant transition: " + result.Message
                + " Only a pending grant can be approved or rejected, and only an active one can be revoked.",
            _ => Describe(result),
        };
    }
}
