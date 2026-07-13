using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Backup;

/// <summary>
/// Collapses a flat catalog page of <see cref="BackupManifest"/> entries into
/// display rows, folding the per-tree members of a backup set (those sharing a
/// non-null <see cref="BackupManifest.SetId"/>) into a single
/// <see cref="BackupRow"/>. Standalone backups pass through as their own rows.
/// Row order follows the first appearance of each backup or set in the input, so
/// the deterministic catalog ordering is preserved.
/// </summary>
public static class BackupRowGrouping
{
    /// <summary>
    /// Groups <paramref name="manifests"/> into display rows: one row per
    /// standalone backup and one collapsed row per backup set.
    /// </summary>
    /// <param name="manifests">The catalog manifests to group. Must not be <see langword="null"/>.</param>
    /// <returns>The display rows in first-seen order.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="manifests"/> is <see langword="null"/>.</exception>
    public static IReadOnlyList<BackupRow> Group(IReadOnlyList<BackupManifest> manifests)
    {
        ArgumentNullException.ThrowIfNull(manifests);

        var rows = new List<BackupRow>();

        // Set id -> index into rows, so members appended after the first are
        // merged into the row already emitted at the set's first-seen position.
        var setRowIndex = new Dictionary<string, int>(StringComparer.Ordinal);

        foreach (var manifest in manifests)
        {
            if (manifest.SetId is not { } setId)
            {
                rows.Add(StandaloneRow(manifest));
                continue;
            }

            if (setRowIndex.TryGetValue(setId, out var index))
            {
                rows[index] = MergeMember(rows[index], manifest);
                continue;
            }

            setRowIndex[setId] = rows.Count;
            rows.Add(FirstSetRow(setId, manifest));
        }

        return rows;
    }

    private static BackupRow StandaloneRow(BackupManifest manifest) => new()
    {
        SetId = null,
        DisplayId = manifest.Id,
        Name = manifest.Name,
        Kind = manifest.Kind,
        CreatedAtUtc = manifest.CreatedAtUtc,
        Members = new[] { manifest },
    };

    private static BackupRow FirstSetRow(string setId, BackupManifest manifest) => new()
    {
        SetId = setId,
        DisplayId = setId,
        Name = manifest.SetName ?? manifest.Name,
        Kind = manifest.Kind,
        CreatedAtUtc = manifest.CreatedAtUtc,
        Members = new[] { manifest },
    };

    private static BackupRow MergeMember(BackupRow row, BackupManifest manifest)
    {
        var members = new List<BackupManifest>(row.Members) { manifest };
        return row with
        {
            // The set row carries the earliest member capture time.
            CreatedAtUtc = manifest.CreatedAtUtc < row.CreatedAtUtc ? manifest.CreatedAtUtc : row.CreatedAtUtc,
            Members = members,
        };
    }
}
