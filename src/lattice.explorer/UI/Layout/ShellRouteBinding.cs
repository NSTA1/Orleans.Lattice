using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Routing;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// The single place the browser's address and the Explorer's route model are
/// kept in step. Renders nothing; it exists only to close the loop in both
/// directions.
/// </summary>
/// <remarks>
/// <para>
/// The router (<see cref="IExplorerShellRouter"/>) owns the route but knows
/// nothing about Blazor, which is what makes the whole route model unit-testable
/// without a renderer. This component feeds every location change into
/// <see cref="IExplorerShellRouter.SetAddress"/>, and performs the navigations
/// the router asks for through
/// <see cref="IExplorerShellRouter.NavigationRequested"/>. Because the address is
/// the input, browser Back and Forward work with no extra handling - they are
/// just location changes.
/// </para>
/// <para>
/// <b>Why it is not the routable page.</b> The binding used to live on the shell
/// page, and the shell page is <em>not</em> always mounted: the app shell hands
/// the working surface to a contributed area's view and stops rendering its own
/// child content, which disposes the page and took the binding down with it. From
/// that moment nothing performed a navigation the router asked for and nothing
/// observed a location change, so the rail could not return to the home area
/// (the page re-mounted, re-read the stale address, and bounced the route
/// straight back) and browser Back out of an area was never seen at all. The
/// binding therefore belongs to the layout, which outlives every surface swap.
/// </para>
/// <para>
/// It also remembers where the user is: every route change is written back
/// through the preference contract, so the next bare <c>/</c> lands there. That
/// is bookkeeping behind a navigation the user already made, so a storage write
/// that fails must not surface as a navigation error and must not make the
/// caller wait. Deciding what a bare address <em>means</em> is not done here -
/// that is <see cref="ExplorerShellEntryPolicy"/>'s single job, applied once per
/// session entry by the shell page.
/// </para>
/// </remarks>
public sealed class ShellRouteBinding : ComponentBase, IDisposable
{
    private bool _canNavigate;
    private ExplorerNavigationRequest? _pendingNavigation;

    [Inject]
    private NavigationManager Navigation { get; set; } = default!;

    [Inject]
    private IExplorerShellRouter Router { get; set; } = default!;

    [Inject]
    private IExplorerShellPreferences Preferences { get; set; } = default!;

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        Navigation.LocationChanged += OnLocationChanged;
        Router.NavigationRequested += OnNavigationRequested;
        Router.RouteChanged += OnRouteChanged;

        Router.SetAddress(Navigation.Uri);
    }

    /// <inheritdoc />
    protected override void OnAfterRender(bool firstRender)
    {
        if (!firstRender)
        {
            return;
        }

        // Navigation is only safe once there is a live circuit, which is why any
        // request raised before now was buffered rather than performed.
        _canNavigate = true;

        if (_pendingNavigation is { } pending)
        {
            _pendingNavigation = null;
            Perform(pending);
        }
    }

    /// <summary>Detaches from the browser and the router.</summary>
    public void Dispose()
    {
        Navigation.LocationChanged -= OnLocationChanged;
        Router.NavigationRequested -= OnNavigationRequested;
        Router.RouteChanged -= OnRouteChanged;
    }

    private void OnLocationChanged(object? sender, LocationChangedEventArgs e) =>
        Router.SetAddress(e.Location);

    private void OnRouteChanged(ExplorerRoute route) =>
        // Remembering is best-effort bookkeeping behind a user's navigation: a
        // browser storage write that fails must not surface as a navigation
        // error, and must not make the caller wait.
        _ = RememberAsync(route);

    private async Task RememberAsync(ExplorerRoute route)
    {
        try
        {
            await Preferences.RememberRouteAsync(route);
        }
        catch (Exception) when (!Preferences.IsLoaded)
        {
            // Storage unreachable (prerender, or a browser with storage denied).
            // The route still shows; only the memory of it is lost.
        }
    }

    private void OnNavigationRequested(ExplorerNavigationRequest request)
    {
        if (!_canNavigate)
        {
            // Before the first render there is no circuit to navigate on. Keep
            // only the latest request: they are absolute addresses, so the last
            // one is the whole intent.
            _pendingNavigation = request;
            return;
        }

        Perform(request);
    }

    private void Perform(ExplorerNavigationRequest request)
    {
        if (string.Equals(
                Navigation.ToBaseRelativePath(Navigation.Uri),
                request.Address.TrimStart('/'),
                StringComparison.Ordinal))
        {
            // Already there. Navigating anyway would push a duplicate history
            // entry that Back would have to walk through.
            return;
        }

        Navigation.NavigateTo(request.Address, forceLoad: false, replace: request.Replace);
    }
}
