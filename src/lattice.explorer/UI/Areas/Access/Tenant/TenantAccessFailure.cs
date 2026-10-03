using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;

/// <summary>
/// Classifies a failed delegated tenant-access call into an <see cref="AccessFailure"/>,
/// in the tenant's own words: a denial names the tenant, a switched-off feature
/// says so, and a confinement or cap refusal is shown beside the field that
/// caused it. Anything else is classified as the auth facade's failures are.
/// </summary>
internal static class TenantAccessFailure
{
    /// <summary>The sentence shown when the cluster has delegated tenant access administration switched off.</summary>
    public const string OffMessage = "Delegated tenant access administration is off on this cluster.";

    /// <summary>
    /// Classifies <paramref name="exception"/>, or returns <see langword="null"/> for
    /// a cancellation, which is never shown.
    /// </summary>
    /// <param name="exception">The fault.</param>
    /// <param name="tenant">The tenant the call named.</param>
    /// <returns>The classified failure, or <see langword="null"/>.</returns>
    public static AccessFailure? From(Exception exception, string tenant)
    {
        ArgumentNullException.ThrowIfNull(exception);
        ArgumentNullException.ThrowIfNull(tenant);
        return exception switch
        {
            OperationCanceledException => null,
            LatticeAuthorizationDeniedException => new(AccessFailureKind.Denied, DeniedMessage(tenant)),
            TenantAccessAdministrationDisabledException => new(AccessFailureKind.Unavailable, OffMessage),
            ReservedTenantOperationException => new(AccessFailureKind.Invalid, $"Tenant {tenant} has no delegated access administration."),
            LatticeQuotaExceededException quota => new(AccessFailureKind.Invalid, quota.Message),
            _ => AccessFailure.From(exception),
        };
    }

    /// <summary>The sentence a denial is shown with.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <returns>The sentence.</returns>
    public static string DeniedMessage(string tenant) =>
        $"You are not permitted to administer access for tenant {tenant}. Ask one of its administrators, or a platform operator.";
}
