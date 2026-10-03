using System.Globalization;
using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The delegated tenant access facades' refinement of <see cref="ShellTenantFaults"/>:
/// it rebuilds the facades' typed refusals from the statuses the tenant-administration
/// binding sends them as, so a page reached over the wire classifies a failure exactly
/// as it does against an in-process facade.
/// </summary>
/// <remarks>
/// <list type="table">
///   <listheader><term>Status</term><description>Exception</description></listheader>
///   <item><term>FailedPrecondition</term><description>
///     <see cref="TenantAccessAdministrationDisabledException"/>,
///     <see cref="ReservedTenantOperationException"/> or
///     <see cref="TenantLastAdminSubjectException"/> when the status detail is that
///     exception's message (the binding sends all three, and any other
///     <see cref="InvalidOperationException"/>, as this one status); otherwise the
///     shared table's <see cref="InvalidOperationException"/>.
///   </description></item>
///   <item><term>ResourceExhausted</term><description>
///     <see cref="LatticeQuotaExceededException"/>, carrying the breached dimension,
///     the usage and the cap from the binding's trailers. The binding withholds the
///     tree the cap protects, so <see cref="LatticeQuotaExceededException.TreeId"/>
///     is empty.
///   </description></item>
///   <item><term>Anything else</term><description><see cref="ShellTenantFaults"/>.</description></item>
/// </list>
/// <para>
/// A confinement refusal (<see cref="TenantAccessConfinementException"/>) arrives as
/// <c>InvalidArgument</c> with the facade's caller-facing message, which the shared
/// table surfaces as an <see cref="ArgumentException"/> carrying that message. The
/// violated rule is not on the wire, so it is not guessed.
/// </para>
/// </remarks>
internal static class ShellTenantAccessFaults
{
    /// <summary>The trailer the binding names a breached cap's dimension in.</summary>
    internal const string QuotaDimensionTrailer = "lattice-quota-dimension";

    /// <summary>The trailer the binding carries a breached cap's usage in.</summary>
    internal const string QuotaCurrentTrailer = "lattice-quota-current";

    /// <summary>The trailer the binding carries a breached cap's limit in.</summary>
    internal const string QuotaLimitTrailer = "lattice-quota-limit";

    private static readonly ShellFaultMessageTemplate Disabled =
        ShellFaultMessageTemplate.Create(static tenant => new TenantAccessAdministrationDisabledException(tenant).Message);

    private static readonly ShellFaultMessageTemplate Reserved =
        ShellFaultMessageTemplate.Create(static (tenant, operation) => new ReservedTenantOperationException(tenant, operation).Message);

    private static readonly ShellFaultMessageTemplate LastAdmin =
        ShellFaultMessageTemplate.Create(static (tenant, subject) => new TenantLastAdminSubjectException(tenant, subject).Message);

    /// <summary>Maps <paramref name="exception"/> for a call about <paramref name="tenantId"/>.</summary>
    /// <param name="exception">The transport fault.</param>
    /// <param name="tenantId">The tenant the call named.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>The exception to throw.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="exception"/> is <see langword="null"/>.</exception>
    public static Exception Map(RpcException exception, string? tenantId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(exception);

        var tenant = tenantId ?? string.Empty;
        switch (exception.StatusCode)
        {
            case StatusCode.FailedPrecondition:
                var detail = exception.Status.Detail;
                if (Disabled.TryMatch(detail, out var disabledTenant, out _))
                {
                    return new TenantAccessAdministrationDisabledException(disabledTenant);
                }

                if (Reserved.TryMatch(detail, out var reservedTenant, out var operation))
                {
                    return new ReservedTenantOperationException(reservedTenant, operation);
                }

                if (LastAdmin.TryMatch(detail, out var adminTenant, out var subject))
                {
                    return new TenantLastAdminSubjectException(adminTenant, subject);
                }

                break;

            case StatusCode.ResourceExhausted:
                return Quota(exception, tenant);
        }

        return ShellTenantFaults.Map(exception, tenantId, cancellationToken);
    }

    private static LatticeQuotaExceededException Quota(RpcException exception, string tenant)
    {
        var message = ShellTransportFaults.Detail(exception);
        var trailers = exception.Trailers;
        var dimension = trailers.GetValue(QuotaDimensionTrailer);
        if (string.IsNullOrEmpty(dimension))
        {
            return new LatticeQuotaExceededException(message);
        }

        _ = long.TryParse(trailers.GetValue(QuotaCurrentTrailer), NumberStyles.Integer, CultureInfo.InvariantCulture, out var current);
        _ = long.TryParse(trailers.GetValue(QuotaLimitTrailer), NumberStyles.Integer, CultureInfo.InvariantCulture, out var limit);
        return new LatticeQuotaExceededException(message, string.Empty, dimension, current, limit, tenant);
    }
}
