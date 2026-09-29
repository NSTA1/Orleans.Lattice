using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The tenancy facades' refinement of <see cref="ShellTransportFaults"/>: the
/// tenant-administration binding maps <see cref="TenantNotFoundException"/> to
/// <c>NotFound</c> and <see cref="TenantAlreadyExistsException"/> to
/// <c>AlreadyExists</c>, so those two statuses are rebuilt into those types, with
/// the tenant id the call was about. Every other status takes the shared table.
/// </summary>
internal static class ShellTenantFaults
{
    /// <summary>Maps <paramref name="exception"/> for a call about <paramref name="tenantId"/>.</summary>
    /// <param name="exception">The transport fault.</param>
    /// <param name="tenantId">
    /// The tenant the call was about, or <see langword="null"/> when the call named
    /// none (the self-service current-tenant read); the rebuilt exception then
    /// carries an empty id.
    /// </param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>The exception to throw.</returns>
    public static Exception Map(RpcException exception, string? tenantId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(exception);

        return exception.StatusCode switch
        {
            StatusCode.NotFound => new TenantNotFoundException(tenantId ?? string.Empty, ShellTransportFaults.Detail(exception)),
            StatusCode.AlreadyExists => new TenantAlreadyExistsException(tenantId ?? string.Empty, ShellTransportFaults.Detail(exception)),
            _ => ShellTransportFaults.Map(exception, cancellationToken),
        };
    }
}
