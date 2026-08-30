using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// A deterministic <see cref="ITenancyDomain"/> for the Tenants plugin's tests:
/// the availability decision and the operator verdict are fixed by the test, so
/// the gate's four-state matrix and the workspace's fail-closed behaviour can be
/// exercised without a live probe. No timing, ordering, or wall-clock
/// dependence.
/// </summary>
internal sealed class FakeTenancyDomain : ITenancyDomain
{
    /// <summary>The tenancy operations surface the workspace runs against.</summary>
    public FakeTenantAdminService Service { get; } = new();

    /// <summary>The decision the availability probe returns.</summary>
    public ExplorerPluginAccess Availability { get; set; } = ExplorerPluginAccess.Allowed;

    /// <summary>Whether the caller validates as a platform operator.</summary>
    public bool IsOperator { get; set; } = true;

    /// <summary>How many times the availability probe was run.</summary>
    public int AvailabilityProbes { get; private set; }

    /// <summary>How many times the operator check was run.</summary>
    public int OperatorProbes { get; private set; }

    /// <inheritdoc />
    public ITenantAdminService Tenants => Service;

    /// <summary>
    /// The same service seen through the tenant-administrator contract this
    /// domain also satisfies. Explicit because the two contracts return
    /// different (widening) types for the one underlying service.
    /// </summary>
    ITenantSelfAdminService IMyTenantDomain.Tenants => Service;

    /// <inheritdoc />
    public bool IsTenancyEnabled { get; set; } = true;

    /// <inheritdoc />
    public ExplorerTenantId? ActiveTenant { get; set; }

    /// <inheritdoc />
    public ExplorerTenantVisibility RequestedVisibility { get; set; } = ExplorerTenantVisibility.ActiveTenant;

    /// <inheritdoc />
    public ValueTask<ExplorerPluginAccess> ProbeAvailabilityAsync(
        CancellationToken cancellationToken = default)
    {
        AvailabilityProbes++;
        return new ValueTask<ExplorerPluginAccess>(Availability);
    }

    /// <inheritdoc />
    public ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default)
    {
        OperatorProbes++;
        return new ValueTask<bool>(IsOperator);
    }

    /// <inheritdoc />
    public ValueTask<bool> SwitchTenantAsync(
        ExplorerTenantId tenant,
        CancellationToken cancellationToken = default)
    {
        if (!IsOperator)
        {
            return new ValueTask<bool>(false);
        }

        ActiveTenant = tenant;
        return new ValueTask<bool>(true);
    }

    /// <inheritdoc />
    public ValueTask<bool> SetVisibilityAsync(
        ExplorerTenantVisibility visibility,
        CancellationToken cancellationToken = default)
    {
        if (!IsOperator)
        {
            return new ValueTask<bool>(false);
        }

        RequestedVisibility = visibility;
        return new ValueTask<bool>(true);
    }
}
