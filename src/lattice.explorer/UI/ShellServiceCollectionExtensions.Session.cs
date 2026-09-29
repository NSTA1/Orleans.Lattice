using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.UI.Design.Slots;
using Orleans.Lattice.Explorer.UI.Session;

namespace Orleans.Lattice.Explorer.UI;

/// <summary>The session chrome's registrations (S2, issue #3816).</summary>
internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the session chrome: its per-circuit state and connection tester,
    /// the default sign-in options, and its three slot contributions - the
    /// connection indicator, the identity menu and the session overlay.
    /// </summary>
    /// <remarks>
    /// The session chrome consumes Core's configuration, connection, sign-in,
    /// preference and (optional) tenant services unchanged; the head registers
    /// those. Everything registered here that holds state is scoped, so each
    /// circuit has its own and no singleton ever reaches a circuit's credential or
    /// connection. The one singleton, <see cref="SessionSignInOptions"/>, is
    /// immutable configuration.
    /// </remarks>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddSession(IServiceCollection services)
    {
        services.TryAddScoped<SessionChromeState>();
        services.TryAddScoped<SessionConnectionAnnouncer>();
        services.TryAddScoped<IConnectionTester, LatticeConnectionTester>();
        services.TryAddSingleton(new SessionSignInOptions());

        services.AddShellSlot<ConnectionIndicator>(ShellSlotNames.HeaderConnection);
        services.AddShellSlot<IdentityMenu>(ShellSlotNames.HeaderIdentity);
        services.AddShellSlot<SessionOverlay>(ShellSlotNames.OverlaySession);
    }
}
