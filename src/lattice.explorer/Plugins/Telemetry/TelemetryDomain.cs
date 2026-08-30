using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins;

namespace Orleans.Lattice.Explorer.Telemetry;

/// <summary>
/// The production <see cref="ITelemetryDomain"/>. It composes the operations
/// surface and the availability probe, and reads the head's currently requested
/// visibility from the shell's tenant switcher when the deployment has one.
/// </summary>
/// <remarks>
/// <para>
/// The switcher is optional: a head without the tenancy add-on registers none,
/// and the domain then reports tenant scoping disabled and the fail-closed
/// active-tenant request. It resolves and works either way rather than failing to
/// construct.
/// </para>
/// <para>
/// <b>The switcher is read, never consulted for a decision.</b> Its requested
/// visibility is copied onto a request for the facade to validate. The domain
/// derives no tenant from it, and never narrows a result by one.
/// </para>
/// </remarks>
/// <param name="queries">The operations surface. Must not be <see langword="null"/>.</param>
/// <param name="availability">The availability probe. Must not be <see langword="null"/>.</param>
/// <param name="switcher">The shell's tenant switcher, when the head has one.</param>
public sealed class TelemetryDomain(
    ITelemetryQueryService queries,
    ITelemetryAvailability availability,
    IExplorerTenantSwitcher? switcher = null) : ITelemetryDomain
{
    private readonly ITelemetryQueryService _queries = queries ?? throw new ArgumentNullException(nameof(queries));

    private readonly ITelemetryAvailability _availability =
        availability ?? throw new ArgumentNullException(nameof(availability));

    private readonly IExplorerTenantSwitcher? _switcher = switcher;

    /// <inheritdoc />
    public ITelemetryQueryService Queries => _queries;

    /// <inheritdoc />
    public bool IsTenancyEnabled => _switcher is { IsActive: true };

    /// <inheritdoc />
    public ExplorerTelemetryVisibility RequestedVisibility =>
        _switcher is { IsActive: true } active
            ? TelemetryProjection.FromTenantVisibility(active.RequestedVisibility)
            : ExplorerTelemetryVisibility.ActiveTenant;

    /// <inheritdoc />
    public ValueTask<ExplorerPluginAccess> ProbeAvailabilityAsync(CancellationToken cancellationToken = default) =>
        _availability.ProbeAsync(cancellationToken);
}
