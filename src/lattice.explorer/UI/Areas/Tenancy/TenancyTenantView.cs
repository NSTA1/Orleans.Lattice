using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Tenancy;

/// <summary>
/// What the administration overview shows of one tenant: its lifecycle state
/// when the cluster lists the tenant for the caller, and its regions.
/// </summary>
/// <param name="Status">The lifecycle state, or <see langword="null"/> when the tenant is not listed for the caller.</param>
/// <param name="IsDefault">Whether it is the reserved default tenant.</param>
/// <param name="Regions">The per-region status.</param>
internal sealed record TenancyTenantView(TenantLifecycleStatus? Status, bool IsDefault, IReadOnlyList<TenantRegionStatusDescriptor> Regions);
