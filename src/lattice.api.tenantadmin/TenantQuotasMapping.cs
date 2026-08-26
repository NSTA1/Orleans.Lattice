using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Pure, allocation-free projections between the tenancy engine's
/// <see cref="TenantQuotas"/> value and the transport-agnostic
/// <see cref="TenantQuotasDescriptor"/> control-API DTO. The control-API contract
/// (<c>Orleans.Lattice.Api.Abstractions</c>) does not reference the tenancy engine,
/// so this facade - which references both - is the single seam that translates
/// between the two. Both directions are struct-to-struct copies with no heap
/// allocation.
/// </summary>
internal static class TenantQuotasMapping
{
    /// <summary>Projects an engine <see cref="TenantQuotas"/> onto its control-API descriptor.</summary>
    /// <param name="quotas">The engine quotas value.</param>
    /// <returns>The equivalent descriptor.</returns>
    public static TenantQuotasDescriptor ToDescriptor(TenantQuotas quotas) => new()
    {
        MaxBytes = quotas.MaxBytes,
        MaxKeys = quotas.MaxKeys,
        MaxMemoryBytes = quotas.MaxMemoryBytes,
        MaxTreeCount = quotas.MaxTreeCount,
        MaxOpsPerSecond = quotas.MaxOpsPerSecond,
        BurstPercent = quotas.BurstPercent,
    };

    /// <summary>Projects a control-API descriptor onto the engine <see cref="TenantQuotas"/> value.</summary>
    /// <param name="descriptor">The control-API descriptor.</param>
    /// <returns>The equivalent engine quotas value.</returns>
    public static TenantQuotas ToQuotas(TenantQuotasDescriptor descriptor) => new()
    {
        MaxBytes = descriptor.MaxBytes,
        MaxKeys = descriptor.MaxKeys,
        MaxMemoryBytes = descriptor.MaxMemoryBytes,
        MaxTreeCount = descriptor.MaxTreeCount,
        MaxOpsPerSecond = descriptor.MaxOpsPerSecond,
        BurstPercent = descriptor.BurstPercent,
    };
}
