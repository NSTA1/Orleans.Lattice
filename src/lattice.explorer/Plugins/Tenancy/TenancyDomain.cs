using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The default <see cref="ITenancyDomain"/>: the host-side adapter that binds
/// the tenancy plugins' declared contract to the three things they actually
/// need - the tenancy operations, the availability probe, and the Explorer's
/// existing tenant-identity seam.
/// <para>
/// This type, not a panel, is where a tenancy plugin's reach is decided, which
/// is the point of the controlled domain seam.
/// </para>
/// </summary>
/// <remarks>
/// The switcher is optional and <see langword="null"/> for a head that never
/// called <c>AddExplorerTenantView()</c>. That is not an error: it is the same
/// "no tenancy here" posture the inactive tenant view takes, so
/// <see cref="IsTenancyEnabled"/> reports <see langword="false"/>, every
/// identity mutation is a no-op, and
/// <see cref="ProbeAvailabilityAsync"/> reports unavailable so the plugins
/// render nothing at all.
/// </remarks>
/// <param name="tenants">The tenancy operations surface.</param>
/// <param name="availability">The availability probe.</param>
/// <param name="switcher">
/// The Explorer's existing operator-gated tenant switcher, or
/// <see langword="null"/> when tenant scoping is not enabled.
/// </param>
public sealed class TenancyDomain(
    ITenantAdminService tenants,
    ITenancyAvailability availability,
    IExplorerTenantSwitcher? switcher = null) : ITenancyDomain
{
    private readonly ITenantAdminService _tenants = tenants ?? throw new ArgumentNullException(nameof(tenants));

    private readonly ITenancyAvailability _availability =
        availability ?? throw new ArgumentNullException(nameof(availability));

    private readonly IExplorerTenantSwitcher? _switcher = switcher;

    /// <inheritdoc />
    public ITenantAdminService Tenants => _tenants;

    /// <inheritdoc />
    public bool IsTenancyEnabled => _switcher is { IsActive: true };

    /// <inheritdoc />
    public ExplorerTenantId? ActiveTenant => _switcher?.ActiveTenant;

    /// <inheritdoc />
    public ExplorerTenantVisibility RequestedVisibility =>
        _switcher?.RequestedVisibility ?? ExplorerTenantVisibility.ActiveTenant;

    /// <inheritdoc />
    public ValueTask<ExplorerPluginAccess> ProbeAvailabilityAsync(CancellationToken cancellationToken = default) =>
        _availability.ProbeAsync(cancellationToken);

    /// <inheritdoc />
    public ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default) =>
        _switcher is null
            ? new ValueTask<bool>(false)
            : _switcher.IsOperatorAsync(cancellationToken);

    /// <inheritdoc />
    public ValueTask<bool> SwitchTenantAsync(
        ExplorerTenantId tenant,
        CancellationToken cancellationToken = default) =>
        _switcher is null
            ? new ValueTask<bool>(false)
            : _switcher.SwitchTenantAsync(tenant, cancellationToken);

    /// <inheritdoc />
    public ValueTask<bool> SetVisibilityAsync(
        ExplorerTenantVisibility visibility,
        CancellationToken cancellationToken = default) =>
        _switcher is null
            ? new ValueTask<bool>(false)
            : _switcher.SetVisibilityAsync(visibility, cancellationToken);
}
