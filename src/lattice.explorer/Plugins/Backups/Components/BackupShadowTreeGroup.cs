namespace Orleans.Lattice.Explorer.Backup.Components;

/// <summary>
/// One logical tree's restore shadows, grouped for the capture picker: the
/// logical tree the shadows were restored for, and its shadow trees.
/// <para>
/// The grouping is computed once per tree discovery rather than by a LINQ
/// <c>GroupBy</c> on the render path, so the picker re-renders without
/// re-grouping on every keystroke.
/// </para>
/// </summary>
/// <param name="LogicalTreeId">The logical tree the shadows were restored for.</param>
/// <param name="Shadows">The shadow trees restored for that logical tree.</param>
internal sealed record BackupShadowTreeGroup(
    string LogicalTreeId,
    IReadOnlyList<BackupTreeOption> Shadows);
