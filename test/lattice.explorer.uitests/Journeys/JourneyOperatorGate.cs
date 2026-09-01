using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// The platform-operator gate for the journey head: it validates exactly
/// <see cref="JourneyWorld.PlatformAdmin"/>, and only while that identity is actually
/// signed in.
/// </summary>
/// <remarks>
/// The shipped default fails closed for everyone, which is correct for a head with no
/// administrative surface but leaves the tenant picker permanently unreachable.
/// Deciding from the live <see cref="IExplorerAuthSession"/> rather than from a fixed
/// answer is what lets one head serve both the operator journeys and the
/// restricted-identity journey.
/// </remarks>
/// <param name="session">The circuit's authentication session.</param>
internal sealed class JourneyOperatorGate(IExplorerAuthSession session) : IExplorerTenantOperatorGate
{
    private readonly IExplorerAuthSession _session =
        session ?? throw new ArgumentNullException(nameof(session));

    /// <inheritdoc />
    public ValueTask<bool> IsPlatformOperatorAsync(CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(_session.IsAuthenticated && JourneyWorld.IsOperator(_session.Username));
}
