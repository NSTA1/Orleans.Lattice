using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>
/// The re-authentication interstitial: shown when the current sign-in latched
/// into its revoked state, it sends the browser through a fresh interactive
/// sign-in and back to the address the operator was on.
/// </summary>
/// <remarks>
/// The destination is Core's <see cref="ExplorerReauthChallenge.BuildUrl"/> over
/// the registered <see cref="ExplorerReauthOptions"/>: the provider's challenge
/// endpoint with the current path and query as its return URL, or a plain reload
/// of the current address when no provider maps one. The navigation is a full
/// page load, which is what makes an OpenID Connect middleware redeem a fresh
/// authorization code even while a session cookie is still valid.
/// </remarks>
public partial class ReauthInterstitial
{
    [Inject]
    private NavigationManager Navigation { get; set; } = default!;

    [Inject]
    private IServiceProvider Services { get; set; } = default!;

    private void Reauthenticate()
    {
        var options = Services.GetService<ExplorerReauthOptions>();
        var currentLocalPath = new Uri(Navigation.Uri).PathAndQuery;
        Navigation.NavigateTo(ExplorerReauthChallenge.BuildUrl(options, currentLocalPath), forceLoad: true);
    }
}
