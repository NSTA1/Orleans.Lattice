namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// The Explorer shell's single source of truth for <em>where you are</em>: the
/// current <see cref="ExplorerRoute"/>, the address that produced it, and the
/// way every part of the shell moves between views.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is the seam to code against.</b> A component that wants to change the
/// view builds a route (<c>router.Current.WithArea("tenants")</c>) and calls
/// <see cref="NavigateTo"/>; a component that wants to follow the view
/// subscribes to <see cref="RouteChanged"/> and reads <see cref="Current"/>. No
/// component should hold navigation state in its own fields, because state that
/// is not in the route is state that is not in the URL - which is exactly how
/// the shell ended up with no deep links, no working Back, and nothing to share.
/// </para>
/// <para>
/// <b>The framework binding is somebody else's job.</b> The router never touches
/// the browser. The head's routable page feeds it the address through
/// <see cref="SetAddress"/> whenever the location changes, and performs the
/// navigations the router asks for through <see cref="NavigationRequested"/>.
/// That is what lets the whole route model be unit-tested without a renderer.
/// </para>
/// <para>
/// <b>Restoring versus addressing.</b> A bare <c>/</c> parses to
/// <see cref="ExplorerRouteStatus.Bare"/>, which is the signal to restore the
/// remembered view from <see cref="Session.IExplorerShellPreferences"/>. Any
/// other address is explicit and wins over what was remembered.
/// </para>
/// </remarks>
public interface IExplorerShellRouter
{
    /// <summary>
    /// The route currently showing. Never <see langword="null"/>; before the head
    /// has supplied an address it is <see cref="ExplorerRoute.Root"/>.
    /// </summary>
    ExplorerRoute Current { get; }

    /// <summary>
    /// How well the address that produced <see cref="Current"/> matched the
    /// grammar. <see cref="ExplorerRouteStatus.Bare"/> means restore from
    /// preferences; <see cref="ExplorerRouteStatus.Malformed"/> means the link was
    /// not fully understood and the user should be told.
    /// </summary>
    ExplorerRouteStatus Status { get; }

    /// <summary>
    /// Raised after <see cref="Current"/> changes to a different route. Not raised
    /// when an address resolves to the route already showing, so a component may
    /// re-render on it freely.
    /// </summary>
    event Action<ExplorerRoute>? RouteChanged;

    /// <summary>
    /// Raised when the router needs the address bar updated. The head's routable
    /// page subscribes to this and performs the navigation; nothing else should.
    /// </summary>
    event Action<ExplorerNavigationRequest>? NavigationRequested;

    /// <summary>
    /// Moves to <paramref name="route"/>, updating <see cref="Current"/> and
    /// asking the head to put the corresponding address in the address bar. A
    /// no-op when the route is already showing, so a component may call it
    /// unconditionally.
    /// </summary>
    /// <param name="route">The route to show. Must not be <see langword="null"/>.</param>
    /// <param name="replace">
    /// Whether to replace the current history entry rather than push a new one.
    /// Push (the default) for a navigation the user asked for, so Back returns
    /// them; replace for a correction they did not.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="route"/> is <see langword="null"/>.</exception>
    void NavigateTo(ExplorerRoute route, bool replace = false);

    /// <summary>
    /// Adopts <paramref name="address"/> as the current location. Called by the
    /// head's routable page on first render and on every browser location change,
    /// including Back and Forward.
    /// </summary>
    /// <param name="address">
    /// The address to adopt, absolute or root-relative. Never throws: an address
    /// that cannot be understood yields a safe fallback route and a
    /// <see cref="ExplorerRouteStatus.Malformed"/> status.
    /// </param>
    /// <returns>The resulting <see cref="Status"/>.</returns>
    ExplorerRouteStatus SetAddress(string? address);

    /// <summary>
    /// Rewrites the address bar to the canonical spelling of
    /// <see cref="Current"/>, replacing the current history entry. The remedy for
    /// a <see cref="ExplorerRouteStatus.Normalized"/> or
    /// <see cref="ExplorerRouteStatus.Malformed"/> address: the view is already
    /// right, but the link the user would copy is not.
    /// </summary>
    void Canonicalize();
}
