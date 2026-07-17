namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// Central definition of the top-level area switcher: the ordered set of areas,
/// their labels, the default, and the advisory enable rule for each. Keeping the
/// registration in one place is the seam that lets a new area (a future
/// access-control surface, for example) join the switcher by adding one
/// descriptor here, with no change to the shell.
/// </summary>
public static class AppAreas
{
    /// <summary>The area selected when the shell first loads.</summary>
    public const AppArea Default = AppArea.Explore;

    /// <summary>
    /// The registered areas in left-to-right display order. Explore is always
    /// enabled; Backups is enabled only when the capability map reports at least
    /// list / read backup access (the coarse top-level rule); Access is enabled
    /// only when the auth-admin control plane grants the coarse administrator
    /// probe (the <see cref="ExplorerCapabilities.AuthAdminAllowed"/> gate); Schema
    /// is enabled only when the schema control plane is reachable (the
    /// <see cref="ExplorerCapabilities.SchemaAllowed"/> gate).
    /// </summary>
    public static IReadOnlyList<AppAreaDescriptor> Ordered { get; } = new[]
    {
        new AppAreaDescriptor
        {
            Area = AppArea.Explore,
            Label = "Explore",
            IsEnabled = static _ => true,
        },
        new AppAreaDescriptor
        {
            Area = AppArea.Backups,
            Label = "Backups",
            IsEnabled = static caps => caps.BackupListAllowed
                || caps.BackupByScope.Values.Any(static s => s.CanList),
        },
        new AppAreaDescriptor
        {
            Area = AppArea.Access,
            Label = "Access",
            IsEnabled = static caps => caps.AuthAdminAllowed,
        },
        new AppAreaDescriptor
        {
            Area = AppArea.Schema,
            Label = "Schema",
            IsEnabled = static caps => caps.SchemaAllowed,
        },
    };

    /// <summary>The human-readable label for <paramref name="area"/>.</summary>
    /// <param name="area">The area to label.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="area"/> is not a registered area.</exception>
    public static string Label(AppArea area) => Describe(area).Label;

    /// <summary>
    /// Reports whether <paramref name="area"/> is enabled for
    /// <paramref name="capabilities"/>. A disabled area stays visible but greyed
    /// out; the rule is advisory (the server still enforces access).
    /// </summary>
    /// <param name="area">The area to evaluate.</param>
    /// <param name="capabilities">The current advisory capability map. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="capabilities"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="area"/> is not a registered area.</exception>
    public static bool IsEnabled(AppArea area, ExplorerCapabilities capabilities)
    {
        ArgumentNullException.ThrowIfNull(capabilities);
        return Describe(area).IsEnabled(capabilities);
    }

    /// <summary>Returns the descriptor for <paramref name="area"/>.</summary>
    /// <param name="area">The area to describe.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="area"/> is not a registered area.</exception>
    public static AppAreaDescriptor Describe(AppArea area)
    {
        foreach (var descriptor in Ordered)
        {
            if (descriptor.Area == area)
            {
                return descriptor;
            }
        }

        throw new ArgumentOutOfRangeException(nameof(area), area, "Unknown application area.");
    }
}
