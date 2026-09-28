namespace Orleans.Lattice.Explorer.Shell.Areas.Backups;

/// <summary>What the page may say about the app that owns a backed-up tree.</summary>
/// <param name="Slug">The app's slug.</param>
/// <param name="DisplayName">The app's presentation name, untrusted text rendered only as text; <see langword="null"/> when it declares none.</param>
/// <param name="RebuildableTrees">The app-local names of the trees it declares rebuildable.</param>
internal sealed record BackupAppInfo(string Slug, string? DisplayName, IReadOnlyList<string> RebuildableTrees)
{
    /// <summary>The name to show for the app: its display name, or its slug.</summary>
    public string Label => string.IsNullOrWhiteSpace(DisplayName) ? Slug : DisplayName;

    /// <summary>Whether the app declares the tree named <paramref name="treeName"/> rebuildable.</summary>
    /// <param name="treeName">The app-local tree name.</param>
    public bool IsRebuildable(string treeName) => RebuildableTrees.Contains(treeName, StringComparer.Ordinal);
}
