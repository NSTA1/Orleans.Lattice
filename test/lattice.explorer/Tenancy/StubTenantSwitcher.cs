using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Tenancy;

/// <summary>
/// A deterministic <see cref="IExplorerTenantSwitcher"/> whose activity, active
/// tenant, operator verdict, and mutation outcome are all fixed by the test, so
/// the tenancy seam's identity integration can be exercised without a live
/// operator probe. No timing, ordering, or wall-clock dependence.
/// </summary>
internal sealed class StubTenantSwitcher(bool isActive = true, bool isOperator = false) : IExplorerTenantSwitcher
{
    public bool IsActive { get; set; } = isActive;

    public ExplorerTenantId? ActiveTenant { get; set; } = new ExplorerTenantId(SampleTenant.TenantId);

    public ExplorerTenantVisibility RequestedVisibility { get; set; } = ExplorerTenantVisibility.ActiveTenant;

    public bool IsOperator { get; set; } = isOperator;

    public ExplorerTenantId? SwitchedTo { get; private set; }

    public ExplorerTenantVisibility? RequestedScope { get; private set; }

    public ValueTask<bool> IsOperatorAsync(CancellationToken cancellationToken = default) =>
        new(IsActive && IsOperator);

    public ValueTask<bool> SetVisibilityAsync(
        ExplorerTenantVisibility visibility,
        CancellationToken cancellationToken = default)
    {
        if (!IsActive || !IsOperator)
        {
            return new ValueTask<bool>(false);
        }

        RequestedScope = visibility;
        RequestedVisibility = visibility;
        return new ValueTask<bool>(true);
    }

    public ValueTask<bool> SwitchTenantAsync(
        ExplorerTenantId tenant,
        CancellationToken cancellationToken = default)
    {
        if (!IsActive || !IsOperator)
        {
            return new ValueTask<bool>(false);
        }

        SwitchedTo = tenant;
        ActiveTenant = tenant;
        return new ValueTask<bool>(true);
    }
}
