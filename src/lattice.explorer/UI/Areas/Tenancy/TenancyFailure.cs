using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// A failed tenant-facade call, classified so a page can say what happened in
/// one sentence and offer the remedy that fits: a denial is not offered a retry,
/// and an invariant the cluster enforces is named rather than reported as an
/// error.
/// </summary>
/// <remarks>
/// A typed exception is recognised directly. Over gRPC the tenant binding sends
/// its invariant refusals (the last admin subject, the last resident region, the
/// reserved default tenant, a grant in the wrong state) as a failed precondition
/// carrying the server's sentence, which the transport surfaces as an
/// <see cref="InvalidOperationException"/>; that sentence is shown as the
/// refusal. The one <see cref="InvalidOperationException"/> the transport raises
/// itself, for an Explorer with no endpoint, is reported as unavailable.
/// </remarks>
/// <param name="Kind">What went wrong.</param>
/// <param name="Message">The sentence to show.</param>
internal sealed record TenancyFailure(TenancyFailureKind Kind, string Message)
{
    /// <summary>The sentence shown when the caller may not act on the tenant.</summary>
    public const string NotPermittedMessage =
        "You are not permitted to do this. It needs a platform operator, or an admin subject of the tenant.";

    /// <summary>The sentence shown when the cluster does not serve tenant administration.</summary>
    public const string NotServedMessage = "This cluster does not serve tenant administration.";

    /// <summary>The sentence shown when the Explorer has no endpoint.</summary>
    public const string NotConnectedMessage = "The Explorer is not connected to a cluster.";

    /// <summary>The sentence shown when the cluster refuses an admin subject naming another tenant's group.</summary>
    public const string ForeignTenantGroupMessage =
        "Another tenant's group can never administer this tenant. Choose a user, a cluster group or one of this tenant's own groups.";

    /// <summary>The sentence shown when delegated tenant access administration is off.</summary>
    public const string DelegatedAccessOffMessage =
        "Delegated tenant access administration is off, so groups cannot be used here. Ask a platform operator.";

    /// <summary>The sentence shown when the cluster did not answer.</summary>
    public const string NoAnswerMessage = "The cluster did not answer. Try again.";

    /// <summary>Whether offering "Try again" could change the outcome.</summary>
    public bool IsRetryable => Kind == TenancyFailureKind.Unavailable;

    /// <summary>
    /// Classifies <paramref name="exception"/>, or returns <see langword="null"/>
    /// for a cancellation, which is never shown.
    /// </summary>
    /// <param name="exception">The fault.</param>
    public static TenancyFailure? From(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return exception switch
        {
            OperationCanceledException => null,
            LatticeAuthorizationDeniedException => new(TenancyFailureKind.Denied, NotPermittedMessage),
            TenantNotFoundException missing => new(TenancyFailureKind.NotFound, NotFoundMessage(missing.TenantId)),
            TenantGrantNotFoundException => new(TenancyFailureKind.NotFound, "That grant no longer exists."),
            TenantAlreadyExistsException exists => new(TenancyFailureKind.Refused, $"A tenant with the id {exists.TenantId} already exists."),
            ReservedTenantOperationException => new(TenancyFailureKind.Refused,
                "The reserved default tenant cannot be suspended, deleted, given quotas or offered in a grant."),
            TenantLastAdminSubjectException => new(TenancyFailureKind.Refused,
                "A tenant keeps at least one admin subject. Add the replacement before removing the last one."),
            TenantLastRegionException => new(TenancyFailureKind.Refused, "A tenant stays resident in at least one region."),
            TenantAccessConfinementException { Rule: TenantAccessConfinementRule.ForeignTenantGroup } => new(TenancyFailureKind.Refused, ForeignTenantGroupMessage),
            TenantAccessAdministrationDisabledException => new(TenancyFailureKind.Refused, DelegatedAccessOffMessage),
            TenantRegionNotAllowedException region => new(TenancyFailureKind.Refused,
                $"Region {region.RegionId} is not allowed for this tenant, or the tenant is still resident there."),
            TenantGrantTransitionException transition => new(TenancyFailureKind.Refused,
                $"The grant is {TenancyFormat.GrantStateLabel(transition.CurrentState).ToLowerInvariant()}, so it cannot become {TenancyFormat.GrantStateLabel(transition.RequestedState).ToLowerInvariant()}."),
            ArgumentException argument => new(TenancyFailureKind.Invalid, argument.Message),
            KeyNotFoundException => new(TenancyFailureKind.NotFound, "It no longer exists."),
            NotSupportedException => new(TenancyFailureKind.Unavailable, NotServedMessage),
            InvalidOperationException invalid when string.Equals(invalid.Message, ShellTransportChannel.NotConfiguredMessage, StringComparison.Ordinal) =>
                new(TenancyFailureKind.Unavailable, NotConnectedMessage),
            InvalidOperationException refused => new(TenancyFailureKind.Refused, refused.Message),
            _ => new(TenancyFailureKind.Unavailable, NoAnswerMessage),
        };
    }

    private static string NotFoundMessage(string tenantId) =>
        string.IsNullOrEmpty(tenantId)
            ? "That tenant does not exist, or you may not see it."
            : $"Tenant {tenantId} does not exist, or you may not see it.";
}
