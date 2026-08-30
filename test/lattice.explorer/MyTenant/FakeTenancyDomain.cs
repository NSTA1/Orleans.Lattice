using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// A scriptable <see cref="ITenancyDomain"/> over
/// <see cref="FakeTenantAdminService"/>: the plugin's whole reach, under a
/// test's control.
/// <para>
/// Because the plugin declares this one contract and receives nothing else from
/// the host, substituting it here exercises the surface exactly as the shell
/// drives it - there is no second channel to the cluster to stub out.
/// </para>
/// </summary>
internal sealed class FakeTenancyDomain : ITenancyDomain
{
    /// <summary>The operations surface every call lands on.</summary>
    public FakeTenantAdminService Service { get; } = new();

    /// <inheritdoc />
    public ITenantAdminService Tenants => Service;

    /// <inheritdoc />
    public bool IsTenancyEnabled { get; set; } = true;

    /// <inheritdoc />
    public ExplorerTenantId? ActiveTenant { get; set; } = new(MyTenantSample.TenantId);

    /// <inheritdoc />
    public ExplorerTenantVisibility RequestedVisibility { get; set; } =
        ExplorerTenantVisibility.ActiveTenant;

    /// <summary>The decision the availability probe returns.</summary>
    public ExplorerPluginAccess Availability { get; set; } = ExplorerPluginAccess.Allowed;

    /// <summary>Whether the caller validates as a platform operator.</summary>
    public bool IsOperator { get; set; }

    /// <summary>Whether a tenant switch is honoured.</summary>
    public bool AllowSwitch { get; set; } = true;

    /// <summary>Whether a visibility request is honoured.</summary>
    public bool AllowVisibilityChange { get; set; }

    /// <summary>Every tenant the caller was switched to, in call order.</summary>
    public List<string> SwitchedTo { get; } = [];

    /// <summary>Every visibility requested, in call order.</summary>
    public List<ExplorerTenantVisibility> VisibilityRequests { get; } = [];

    /// <inheritdoc />
    public ValueTask<ExplorerPluginAccess> ProbeAvailabilityAsync(
        CancellationToken cancellationToken = default) =>
        new(Availability);

    /// <inheritdoc />
    public ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default) =>
        new(IsOperator);

    /// <inheritdoc />
    public ValueTask<bool> SwitchTenantAsync(
        ExplorerTenantId tenant,
        CancellationToken cancellationToken = default)
    {
        SwitchedTo.Add(tenant.Value);
        if (!AllowSwitch)
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
        VisibilityRequests.Add(visibility);
        if (!AllowVisibilityChange)
        {
            return new ValueTask<bool>(false);
        }

        RequestedVisibility = visibility;
        return new ValueTask<bool>(true);
    }
}
