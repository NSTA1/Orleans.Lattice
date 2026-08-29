using System.Globalization;

namespace Orleans.Lattice.Explorer.Backup.Components;

/// <summary>
/// One catalogue row as the surface renders it: the display row itself, plus
/// the cell text projected once per page.
/// <para>
/// The projection exists so the catalogue's render path allocates nothing.
/// The underlying <see cref="BackupRow"/> re-derives its scope list and
/// re-formats its timestamps on every read, and the surface reads each of them
/// once per row per render - twice over, since the same row is also read by the
/// card presentation at compact. Projecting once per page turns that into a
/// field read.
/// </para>
/// <para>
/// Only stable facts are baked in. Health is deliberately left out and read
/// live from the panel, because a health refresh must show through without
/// re-projecting the page.
/// </para>
/// </summary>
/// <param name="Row">The display row this projection renders.</param>
/// <param name="DisplayId">The row's display id, used as the surface's diffing key.</param>
/// <param name="Name">The row's name.</param>
/// <param name="Kind">The backup kind, rendered.</param>
/// <param name="Scope">The scope trees the row covers, rendered.</param>
/// <param name="Created">The capture time in the catalogue's <c>yyyy-MM-dd HH:mm:ss</c> UTC form.</param>
/// <param name="CreatedTitle">The capture time in full round-trippable UTC form, shown as a tooltip.</param>
internal sealed record BackupCatalogueRow(
    BackupRow Row,
    string DisplayId,
    string Name,
    string Kind,
    string Scope,
    string Created,
    string CreatedTitle)
{
    /// <summary>Projects <paramref name="row"/> onto its rendered form.</summary>
    /// <param name="row">The display row to project. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="row"/> is <see langword="null"/>.</exception>
    public static BackupCatalogueRow From(BackupRow row)
    {
        ArgumentNullException.ThrowIfNull(row);

        var created = row.CreatedAtUtc.UtcDateTime;
        return new BackupCatalogueRow(
            row,
            row.DisplayId,
            row.Name,
            row.Kind.ToString(),
            ScopeText(row),
            created.ToString(BackupsPanel.CreatedFormat, CultureInfo.InvariantCulture),
            created.ToString("u", CultureInfo.InvariantCulture));
    }

    private static string ScopeText(BackupRow row) =>
        row.IsSet ? string.Join(", ", row.TreeIds) : row.Members[0].Scope.TreeId;
}
