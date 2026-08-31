namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// Default <see cref="IExplorerShellRouter"/>: a scoped holder of the current
/// route that translates addresses in and navigation requests out, with no
/// dependency on any UI framework.
/// </summary>
/// <remarks>
/// <para>
/// The echo problem this solves: a navigation the router initiates comes back to
/// it moments later as a location change, and a naive implementation would treat
/// that as a fresh navigation and raise a second change. Rather than guess with a
/// flag or a timer, the router compares routes by value - <see cref="ExplorerRoute"/>
/// is a record - so re-adopting the address it just emitted is recognised as the
/// same route and stays silent. That makes the whole cycle timing-independent,
/// which is what makes it testable without a renderer.
/// </para>
/// <para>
/// Scoped per session, like the rest of the shell's state, so two circuits never
/// share a route.
/// </para>
/// </remarks>
public sealed class ExplorerShellRouter : IExplorerShellRouter
{
    private ExplorerRoute _current = ExplorerRoute.Root;

    /// <inheritdoc />
    public ExplorerRoute Current => _current;

    /// <inheritdoc />
    public ExplorerRouteStatus Status { get; private set; } = ExplorerRouteStatus.Bare;

    /// <inheritdoc />
    public event Action<ExplorerRoute>? RouteChanged;

    /// <inheritdoc />
    public event Action<ExplorerNavigationRequest>? NavigationRequested;

    /// <inheritdoc />
    public void NavigateTo(ExplorerRoute route, bool replace = false)
    {
        ArgumentNullException.ThrowIfNull(route);

        var address = ExplorerRoutePath.Format(route);
        var changed = !route.Equals(_current);

        _current = route;

        // An explicit navigation is by definition an addressable location, even
        // when it happens to be the root: the caller asked for it rather than
        // arriving with nothing.
        Status = route.IsBare && route.Tenant.Length == 0 && !route.AllTenants && route.Parameters.Count == 0
            ? ExplorerRouteStatus.Bare
            : ExplorerRouteStatus.Canonical;

        NavigationRequested?.Invoke(new ExplorerNavigationRequest(address, replace));

        if (changed)
        {
            RouteChanged?.Invoke(route);
        }
    }

    /// <inheritdoc />
    public ExplorerRouteStatus SetAddress(string? address)
    {
        var parsed = ExplorerRoutePath.Parse(address);
        Status = parsed.Status;

        if (parsed.Route.Equals(_current))
        {
            // The echo of a navigation this router just made, or a reload of the
            // same address. Nothing moved, so nothing is announced.
            return Status;
        }

        _current = parsed.Route;
        RouteChanged?.Invoke(parsed.Route);
        return Status;
    }

    /// <inheritdoc />
    public void Canonicalize()
    {
        Status = _current.IsBare && _current.Tenant.Length == 0 && !_current.AllTenants && _current.Parameters.Count == 0
            ? ExplorerRouteStatus.Bare
            : ExplorerRouteStatus.Canonical;

        NavigationRequested?.Invoke(
            new ExplorerNavigationRequest(ExplorerRoutePath.Format(_current), Replace: true));
    }
}
