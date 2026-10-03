using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Argument validation shared by the tenant administration facades, so every
/// facade rejects a malformed or reserved tenant id with the same exception and
/// message.
/// </summary>
internal static class TenantAdminArguments
{
    /// <summary>
    /// Parses a caller-supplied tenant id.
    /// </summary>
    /// <param name="tenantId">The tenant id to parse.</param>
    /// <param name="parameterName">The parameter name reported on failure.</param>
    /// <returns>The parsed tenant id.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="tenantId"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is empty or not a valid tenant id.</exception>
    internal static TenantId ParseTenantId(string tenantId, string parameterName = "tenantId")
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId, parameterName);
        if (!TenantId.TryParse(tenantId, out var tenant))
        {
            throw new ArgumentException($"'{tenantId}' is not a valid tenant id.", parameterName);
        }

        return tenant;
    }

    /// <summary>
    /// Refuses <paramref name="operation"/> against the reserved default tenant.
    /// </summary>
    /// <param name="tenant">The target tenant.</param>
    /// <param name="operation">The operation name reported on refusal.</param>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenant"/> is the default tenant.</exception>
    internal static void ThrowIfReservedTenant(TenantId tenant, string operation)
    {
        if (tenant.IsDefault)
        {
            throw new ReservedTenantOperationException(tenant.Value, operation);
        }
    }
}
