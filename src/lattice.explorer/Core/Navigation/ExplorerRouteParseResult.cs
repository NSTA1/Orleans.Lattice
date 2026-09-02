namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// The outcome of parsing an address into an <see cref="ExplorerRoute"/>: always
/// a usable route, plus how well the address matched.
/// </summary>
/// <remarks>
/// Parsing never throws and never yields a null route. That is the whole point:
/// a malformed or no-longer-valid URL must degrade to something the shell can
/// render rather than wedging it. The <see cref="Status"/> tells the caller
/// whether to rewrite the address bar
/// (<see cref="ExplorerRouteStatus.Normalized"/>), restore the remembered view
/// (<see cref="ExplorerRouteStatus.Bare"/>), or say that the link was not
/// understood (<see cref="ExplorerRouteStatus.Malformed"/>).
/// </remarks>
/// <param name="Route">The parsed route. Never <see langword="null"/>.</param>
/// <param name="Status">How well the address matched the grammar.</param>
public readonly record struct ExplorerRouteParseResult(ExplorerRoute Route, ExplorerRouteStatus Status)
{
    /// <summary>
    /// Whether the whole address was understood. <see langword="false"/> only for
    /// <see cref="ExplorerRouteStatus.Malformed"/>, where the route is a
    /// best-effort fallback rather than what the address asked for.
    /// </summary>
    public bool IsUnderstood => Status != ExplorerRouteStatus.Malformed;

    /// <summary>
    /// Whether the shell should rewrite the address bar to the canonical
    /// spelling. True for a normalised or malformed address, both of which leave
    /// the user holding a link that does not match what they are looking at.
    /// </summary>
    public bool ShouldCanonicalize =>
        Status is ExplorerRouteStatus.Normalized or ExplorerRouteStatus.Malformed;
}
