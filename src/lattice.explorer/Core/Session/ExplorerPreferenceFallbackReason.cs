namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// Why a preference read did not return a remembered value.
/// </summary>
public enum ExplorerPreferenceFallbackReason
{
    /// <summary>A remembered value was found and it still resolves. Nothing fell back.</summary>
    None,

    /// <summary>
    /// Nothing was remembered under the key for this scope - a first visit, a new
    /// cluster, or a view that was reset. Unremarkable: the caller shows its
    /// default and says nothing.
    /// </summary>
    NotStored,

    /// <summary>
    /// The preference store has not hydrated yet, so no read is meaningful. The
    /// caller should show its default now and re-read once
    /// <see cref="IExplorerShellPreferences.IsLoaded"/> is
    /// <see langword="true"/>, rather than persisting the default over the
    /// user's real choice.
    /// </summary>
    NotLoaded,

    /// <summary>
    /// A value was remembered but no longer resolves against the live cluster or
    /// the caller's permissions - a deleted tree, a renamed view, an area this
    /// identity may no longer reach. The one case the user is likely to find
    /// confusing, so the resolution carries an explanation to show them.
    /// </summary>
    NotResolvable,
}
