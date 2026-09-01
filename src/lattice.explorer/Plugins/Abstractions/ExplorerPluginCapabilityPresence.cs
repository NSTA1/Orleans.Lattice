namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// Whether the capability a plugin surfaces exists on this cluster at all - the
/// question that separates
/// <see cref="ExplorerPluginAccessState.Unavailable"/> from every state that is
/// about the caller.
/// <para>
/// <see cref="Present"/> is <c>0</c> so an unset value keeps the entry visible
/// and lets the caller-facing rules decide. That is the fail-closed default
/// here: defaulting to <see cref="Absent"/> would make an unprobed plugin
/// silently vanish, which hides a surface rather than refusing it.
/// </para>
/// </summary>
public enum ExplorerPluginCapabilityPresence
{
    /// <summary>
    /// The cluster serves the capability, so the entry is worth rendering and
    /// the remaining question is whether this caller may use it.
    /// </summary>
    Present = 0,

    /// <summary>
    /// The cluster does not serve the capability - the add-on is not installed,
    /// the facade is not registered, or the head never enabled it. There is
    /// nothing to sign in for and nothing to be granted.
    /// </summary>
    Absent = 1,
}
