namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// How well an inbound URL matched the shell's route grammar. The shell always
/// gets a usable <see cref="ExplorerRoute"/> back; this says how much it had to
/// do to get there, and therefore what the shell should do next.
/// </summary>
public enum ExplorerRouteStatus
{
    /// <summary>
    /// The address carried no navigation state at all - a bare <c>/</c>. Not an
    /// error: it is the signal to restore the remembered view from the
    /// preference contract rather than to show a default one.
    /// </summary>
    Bare,

    /// <summary>
    /// The address parsed cleanly and was already spelled canonically. Nothing to
    /// do; leave the address bar alone.
    /// </summary>
    Canonical,

    /// <summary>
    /// The address parsed cleanly but was not spelled canonically - an upper-case
    /// segment, a trailing slash, or a tolerated query spelling. The route is
    /// correct; the shell should replace the address bar with
    /// <see cref="ExplorerRoutePath.Format"/> so the link the user copies next is
    /// the canonical one.
    /// </summary>
    Normalized,

    /// <summary>
    /// Part of the address could not be understood - a bad escape sequence, more
    /// path segments than the grammar has, or a segment that normalises to
    /// nothing. The accompanying route is the best-effort fallback and is always
    /// safe to show, so the shell degrades rather than erroring; it should also
    /// tell the user the link was not fully understood, because silently landing
    /// somewhere else is the confusing outcome.
    /// </summary>
    Malformed,
}
