namespace Orleans.Lattice.Backup;

/// <summary>
/// The <see cref="ILatticeViewProjection"/> that maintains the backup-catalog
/// index materialised view. It lowers each catalog <see cref="MutationKind.Set"/>
/// (a registered <see cref="BackupManifest"/>) into a single compact
/// <see cref="BackupCatalogIndexRow"/> re-keyed by
/// <see cref="BackupCatalogIndexKey"/> so the index scans newest-first with set
/// members contiguous. The listing API reads this index to answer filtered,
/// created-descending, paged catalog queries by pushing the name / kind / scope /
/// created predicates down into the scan.
/// <para>
/// <b>Deletes.</b> The index key is derived from the manifest value (its capture
/// time and set id), which a value-less catalog delete does not carry, so a stale
/// index row cannot be re-keyed and removed from the projection. This is by design:
/// the listing API drops any index row whose backup no longer exists in the
/// authoritative catalog (a liveness check), and because the view is not
/// accumulative a projection-version change rebuilds it from live catalog state,
/// so a deleted backup can never surface through the index.
/// </para>
/// <para>
/// <b>Standalone re-captures are idempotent.</b> A backup id is a content address
/// and a standalone backup's capture time is held immutable by
/// <see cref="BackupManifestRegistration"/> when the id is re-registered, so
/// re-capturing identical content re-keys to the same index key and upserts in
/// place rather than leaving an orphaned duplicate row behind. A row left behind by
/// an earlier index generation (before this became idempotent, or by a set stamp)
/// is healed by the liveness check and the created-descending read's per-page
/// de-duplication, and shed entirely the next time the non-accumulative view
/// rebuilds from live catalog state.
/// </para>
/// </summary>
public sealed class BackupCatalogIndexProjection : ILatticeViewProjection
{
    /// <summary>The stable code-identity version of the index projection logic.</summary>
    public const string Version = "backup-catalog-index-v3";

    private static readonly ILatticeSerializer<BackupManifest> ManifestSerializer =
        JsonLatticeSerializer<BackupManifest>.Default;

    private static readonly ILatticeSerializer<BackupCatalogIndexRow> RowSerializer =
        JsonLatticeSerializer<BackupCatalogIndexRow>.Default;

    /// <inheritdoc />
    public string ProjectionVersion => Version;

    /// <inheritdoc />
    public IEnumerable<ViewWrite> Project(LatticeMutation mutation)
    {
        if (ProjectSet(mutation) is { } write)
        {
            yield return write;
        }
    }

    private static ViewWrite? ProjectSet(in LatticeMutation mutation)
    {
        // Only a manifest registration (a Set carrying value bytes) contributes an
        // index row. Deletes and range deletes yield nothing (see the type remark).
        if (mutation.Kind != MutationKind.Set || mutation.Value is not { Length: > 0 } value)
        {
            return null;
        }

        var manifest = ManifestSerializer.Deserialize(value);
        var row = new BackupCatalogIndexRow
        {
            BackupId = manifest.Id,
            Name = manifest.Name,
            Kind = manifest.Kind,
            TreeId = manifest.Scope.TreeId,
            CreatedAtUtc = manifest.CreatedAtUtc,
            SetId = manifest.SetId,
            SetName = manifest.SetName,
            BaseBackupId = manifest.BaseBackupId,
        };

        // SourceKey is deliberately left unset: the index key is value-derived, so
        // distinct backups never map to one key and the maintainer's re-key
        // collision detector must skip these writes (as the history view does).
        return ViewWrite.Upsert(BackupCatalogIndexKey.Encode(manifest), RowSerializer.Serialize(row), mutation.Timestamp);
    }
}
