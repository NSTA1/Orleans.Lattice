namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// What the shell should do with the address it has just landed on.
/// </summary>
public enum ExplorerShellEntryAction
{
    /// <summary>
    /// Show the address as given. It is explicit and canonical, so it wins over
    /// anything remembered and needs no correction.
    /// </summary>
    ShowAddress,

    /// <summary>
    /// The address carried no state and there is a remembered view to return to.
    /// Navigate to it, replacing the history entry so Back still leads out of the
    /// Explorer rather than into its own bookkeeping.
    /// </summary>
    RestoreRemembered,

    /// <summary>
    /// The address resolved to the right view but is not spelled the way the
    /// shell would spell it. Show it, and rewrite the address bar so the link the
    /// user copies next is the canonical one.
    /// </summary>
    Canonicalize,
}

/// <summary>
/// The decision the shell reached on entry: what to do, and the route to do it
/// with.
/// </summary>
/// <param name="Action">The action to take.</param>
/// <param name="Route">
/// The route to show. For <see cref="ExplorerShellEntryAction.RestoreRemembered"/>
/// this is the remembered route; otherwise it is the route the address produced.
/// </param>
public readonly record struct ExplorerShellEntry(ExplorerShellEntryAction Action, ExplorerRoute Route);

/// <summary>
/// The rule that arbitrates between the URL and the preference contract when the
/// shell lands on an address.
/// </summary>
/// <remarks>
/// <para>
/// A pure function, deliberately: this is the single sentence the whole epic's
/// state model turns on - <em>an explicit URL always wins; a bare address
/// restores</em> - and keeping it out of a component means it can be stated
/// once, tested exhaustively without a renderer, and reused by every consumer
/// that needs to make the same call rather than re-deriving it.
/// </para>
/// <para>
/// It deliberately does not decide whether the remembered route still
/// <em>resolves</em> - whether that tree still exists, whether that area is
/// reachable by this identity. That knowledge lives with the consumer that owns
/// the area or the catalog, and is handled through
/// <see cref="Session.IExplorerShellPreferences.RestoreAsync{T, TState}"/>. This
/// decides only which source of truth applies.
/// </para>
/// </remarks>
public static class ExplorerShellEntryPolicy
{
    /// <summary>
    /// Decides what to do with the address that produced
    /// <paramref name="current"/>.
    /// </summary>
    /// <param name="status">How well the address matched the grammar.</param>
    /// <param name="current">The route the address produced. Must not be <see langword="null"/>.</param>
    /// <param name="remembered">
    /// The remembered route from the preference contract, or
    /// <see cref="ExplorerRoute.Root"/> when nothing is remembered. Must not be
    /// <see langword="null"/>.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="current"/> or <paramref name="remembered"/> is <see langword="null"/>.</exception>
    public static ExplorerShellEntry Decide(
        ExplorerRouteStatus status,
        ExplorerRoute current,
        ExplorerRoute remembered)
    {
        ArgumentNullException.ThrowIfNull(current);
        ArgumentNullException.ThrowIfNull(remembered);

        if (status == ExplorerRouteStatus.Bare)
        {
            // Nothing was asked for, so the remembered view is the best answer -
            // unless nothing was remembered either, in which case the bare
            // address genuinely is the view and the shell shows its default.
            return remembered.IsBare
                ? new ExplorerShellEntry(ExplorerShellEntryAction.ShowAddress, current)
                : new ExplorerShellEntry(ExplorerShellEntryAction.RestoreRemembered, remembered);
        }

        // An address that was normalised, or one the shell could only partly
        // understand, still shows the right view - but the address bar is not
        // reproducible, and a stale bookmark that silently keeps working while
        // showing something else is worse than one that corrects itself.
        return status is ExplorerRouteStatus.Normalized or ExplorerRouteStatus.Malformed
            ? new ExplorerShellEntry(ExplorerShellEntryAction.Canonicalize, current)
            : new ExplorerShellEntry(ExplorerShellEntryAction.ShowAddress, current);
    }
}
