using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// The reachable-tenant list for the journey head, read from
/// <see cref="JourneyWorld"/> for the currently signed-in identity.
/// </summary>
/// <remarks>
/// This is the single source of truth the shipped tenant scope control and the
/// identity resolver both consult, so withdrawing a tenant from the world is enough to
/// make a remembered choice stop resolving - which is exactly the condition the
/// fail-closed restore journey has to reproduce.
/// </remarks>
/// <param name="session">The circuit's authentication session.</param>
/// <param name="world">The journey world holding the demo cluster's facts.</param>
internal sealed class JourneyAccessibleTenantSource(IExplorerAuthSession session, JourneyWorld world)
    : IExplorerAccessibleTenantSource
{
    private readonly IExplorerAuthSession _session =
        session ?? throw new ArgumentNullException(nameof(session));

    private readonly JourneyWorld _world = world ?? throw new ArgumentNullException(nameof(world));

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<ExplorerTenantId>> GetAccessibleTenantsAsync(
        CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(_world.AccessibleTenants(
            _session.IsAuthenticated ? _session.Username : null));
}
