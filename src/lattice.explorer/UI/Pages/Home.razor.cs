using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Routing;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.UI.Pages;

/// <summary>
/// The shell's route binding: the single place the browser's address and the
/// Explorer's route model are kept in step.
/// </summary>
/// <remarks>
/// <para>
/// The router (<see cref="IExplorerShellRouter"/>) owns the route but knows
/// nothing about Blazor, which is what makes the whole route model unit-testable
/// without a renderer. This page closes the loop in both directions: it feeds
/// every location change into <see cref="IExplorerShellRouter.SetAddress"/>, and
/// it performs the navigations the router asks for through
/// <see cref="IExplorerShellRouter.NavigationRequested"/>. Because the address
/// is the input, browser Back and Forward work with no extra handling - they are
/// just location changes.
/// </para>
/// <para>
/// It also arbitrates the one place the URL and the preference contract meet. A
/// bare <c>/</c> carries no state, so the remembered view is restored into the
/// address bar with a history <em>replace</em>, leaving Back pointing wherever
/// the user came from rather than at the shell's own bookkeeping. Any other
/// address is explicit and wins outright; it is merely remembered, so the next
/// bare visit lands there.
/// </para>
/// <para>
/// The restore runs once per session entry. A later, deliberate navigation to
/// <c>/</c> is taken at face value: a user who asks for the plain home surface
/// must be able to reach it, rather than being bounced back to where they were
/// yesterday every time.
/// </para>
/// </remarks>
public partial class Home : IDisposable
{
    private bool _restoreApplied;
    private bool _canNavigate;
    private ExplorerNavigationRequest? _pendingNavigation;

    /// <summary>
    /// The catch-all route value. Never read: the address is parsed from
    /// <see cref="NavigationManager.Uri"/> by
    /// <see cref="ExplorerRoutePath.Parse"/> so an escaped selection id survives,
    /// which Blazor's own segment binding would split. The parameter exists
    /// because the catch-all template declares it.
    /// </summary>
    [Parameter]
    public string? ShellPath { get; set; }

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
    protected override Task OnInitializedAsync() => HydrateAndSettleAsync();

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (!firstRender)
        {
            return;
        }

        // Browser storage is unreachable during a server prerender, so the first
        // hydration attempt can legitimately have done nothing. Navigation is
        // likewise only safe once there is a live circuit, which is why any
        // request raised before now was buffered rather than performed.
        _canNavigate = true;

        if (_pendingNavigation is { } pending)
        {
            _pendingNavigation = null;
            Perform(pending);
        }

        await HydrateAndSettleAsync();
    }

    /// <summary>Detaches from the browser and the router.</summary>
    public void Dispose()
    {
        Navigation.LocationChanged -= OnLocationChanged;
        Router.NavigationRequested -= OnNavigationRequested;
        Router.RouteChanged -= OnRouteChanged;
    }

    private async Task HydrateAndSettleAsync()
    {
        await Preferences.EnsureLoadedAsync();

        if (_restoreApplied || !Preferences.IsLoaded)
        {
            // Still unhydrated: leave the shell on whatever the address says and
            // try again after the first render. Restoring from an unhydrated
            // mirror would look exactly like "nothing was remembered".
            return;
        }

        _restoreApplied = true;

        var entry = ExplorerShellEntryPolicy.Decide(
            Router.Status,
            Router.Current,
            Preferences.GetRememberedRoute());

        switch (entry.Action)
        {
            case ExplorerShellEntryAction.RestoreRemembered:
                // Replace rather than push: the user asked for '/', so Back
                // should return them to wherever they came from, not to the
                // address the shell substituted for them.
                Router.NavigateTo(entry.Route, replace: true);
                return;

            case ExplorerShellEntryAction.Canonicalize:
                Router.Canonicalize();
                break;
        }

        await Preferences.RememberRouteAsync(Router.Current);
    }

    private void OnLocationChanged(object? sender, LocationChangedEventArgs e) =>
        Router.SetAddress(e.Location);

    private void OnRouteChanged(ExplorerRoute route)
    {
        // Remembering is best-effort bookkeeping behind a user's navigation: a
        // browser storage write that fails must not surface as a navigation
        // error, and must not make the caller wait.
        _ = RememberAsync(route);
        StateHasChanged();
    }

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
