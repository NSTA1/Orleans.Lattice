namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// Shared session-store key naming for the currently inspected entry key, so the
/// Data tab and the History tab agree on a single selection per tree. The Data
/// tab writes the key the user drills into; the History tab reads it as the key
/// whose timeline to open, reusing the same selection model across tabs.
/// </summary>
public static class DataSelection
{
    /// <summary>
    /// The session-store key under which the inspected entry key for
    /// <paramref name="treeId"/> is held. Composed with a feature prefix so it
    /// never collides with other per-selection UI state.
    /// </summary>
    public static string SelectedKey(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return $"data-selected-key:{treeId}";
    }
}
