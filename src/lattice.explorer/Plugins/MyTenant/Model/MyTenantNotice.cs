using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant;

/// <summary>
/// One operation outcome as the surface reports it: the classified status, the
/// cluster's own message <em>verbatim</em>, and the plugin's own guidance for
/// the refusals a tenant admin can actually act on.
/// </summary>
/// <remarks>
/// <para>
/// The server's message is never replaced, only supplemented. That matters
/// because the gRPC binding collapses every precondition refusal onto a single
/// <see cref="TenantOperationStatus.PreconditionFailed"/> and carries the
/// specific reason only in the message: a caller reached over the wire does not
/// see <see cref="TenantOperationStatus.LastAdminSubject"/> or
/// <see cref="TenantOperationStatus.LastRegion"/> at all. Rendering
/// <see cref="Message"/> is therefore the only way those two refusals stay
/// distinguishable, and substituting a tidy generic banner for it would destroy
/// exactly the information the caller needs.
/// </para>
/// <para>
/// The binding deliberately does not sniff the message to reconstruct the type,
/// because parsing server prose breaks on any wording change. The plugin's
/// answer is the same one its region surface already takes: know the invariant
/// client-side and say so before the call, and render the server's own words
/// when it refuses anyway.
/// </para>
/// </remarks>
/// <param name="Status">The classified outcome.</param>
/// <param name="Message">The cluster's message, rendered verbatim.</param>
/// <param name="Guidance">
/// The plugin's own next step for a refusal the caller can resolve, or
/// <see langword="null"/> when there is nothing useful to add.
/// </param>
public readonly record struct MyTenantNotice(
    TenantOperationStatus Status,
    string Message,
    string? Guidance = null)
{
    /// <summary>
    /// What to do about a change that would remove the tenant's last admin
    /// subject.
    /// </summary>
    public const string LastAdminSubjectGuidance =
        "A tenant must keep at least one admin subject, or nobody could administer it. Add the "
        + "replacement subject first, then remove this one.";

    /// <summary>
    /// What to do about a residency change that would leave the tenant resident
    /// nowhere.
    /// </summary>
    public const string LastRegionGuidance =
        "A tenant must stay resident in at least one region. Add another region first, then remove "
        + "this one.";

    /// <summary>
    /// What to do about a residency change naming a region outside the allowed
    /// set. A tenant admin cannot widen that set themselves.
    /// </summary>
    public const string RegionNotAllowedGuidance =
        "Residency must stay within the regions a platform operator has authorized for your tenant. "
        + "Ask an operator to allow the region before making it resident.";

    /// <summary>
    /// What to say about a refused cross-tenant grant transition.
    /// </summary>
    public const string GrantTransitionGuidance =
        "A grant can only be approved or rejected while it is pending, and only withdrawn while it "
        + "is active. Refresh to see its current state.";

    /// <summary>
    /// Builds a notice from a completed operation, attaching the plugin's own
    /// guidance for the refusals it recognises.
    /// </summary>
    /// <param name="result">The operation's result. Must not be <see langword="null"/>.</param>
    /// <returns>The notice to render.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="result"/> is <see langword="null"/>.</exception>
    public static MyTenantNotice For(TenantOperationResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new MyTenantNotice(result.Status, result.Message, GuidanceFor(result.Status));
    }

    /// <summary>
    /// Builds a notice for a refusal the plugin made itself, before any call
    /// left the process.
    /// </summary>
    /// <param name="status">The status to classify the refusal under.</param>
    /// <param name="message">The refusal message. Must not be <see langword="null"/>.</param>
    /// <param name="guidance">Optional guidance to render beneath it.</param>
    /// <returns>The notice to render.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="message"/> is <see langword="null"/>.</exception>
    public static MyTenantNotice Refused(
        TenantOperationStatus status,
        string message,
        string? guidance = null)
    {
        ArgumentNullException.ThrowIfNull(message);
        return new MyTenantNotice(status, message, guidance);
    }

    /// <summary>Whether the operation succeeded.</summary>
    public bool IsSuccess => Status == TenantOperationStatus.Succeeded;

    /// <summary>
    /// The banner's modifier class. A refusal the caller can resolve reads as a
    /// warning rather than a failure, so an ordinary guard-rail is not dressed up
    /// as a fault.
    /// </summary>
    public string Severity => Status switch
    {
        TenantOperationStatus.Succeeded => "is-success",
        TenantOperationStatus.Denied or TenantOperationStatus.AuthenticationRequired => "is-denied",
        TenantOperationStatus.LastAdminSubject
            or TenantOperationStatus.LastRegion
            or TenantOperationStatus.RegionNotAllowed
            or TenantOperationStatus.GrantTransitionRejected
            or TenantOperationStatus.GrantNotFound
            or TenantOperationStatus.AlreadyExists
            or TenantOperationStatus.ReservedTenant
            or TenantOperationStatus.PreconditionFailed => "is-refused",
        _ => "is-failed",
    };

    private static string? GuidanceFor(TenantOperationStatus status) => status switch
    {
        TenantOperationStatus.LastAdminSubject => LastAdminSubjectGuidance,
        TenantOperationStatus.LastRegion => LastRegionGuidance,
        TenantOperationStatus.RegionNotAllowed => RegionNotAllowedGuidance,
        TenantOperationStatus.GrantTransitionRejected => GrantTransitionGuidance,

        // PreconditionFailed is where the wire lands every precondition refusal,
        // so no guidance is guessed for it: the server's own message is the only
        // reliable statement of which invariant was breached, and it is already
        // rendered verbatim. Inventing guidance here would mean sniffing that
        // message, which breaks on any wording change.
        _ => null,
    };
}
