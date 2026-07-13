namespace Orleans.Lattice.Backup;

/// <summary>
/// Reconciles a manifest that is being registered against the manifest already
/// stored under the same backup id.
/// <para>
/// A backup id is a content address, so re-capturing identical bytes yields the
/// same id: it is the <i>same</i> immutable backup, not a new one. Its capture
/// time (<see cref="BackupManifest.CreatedAtUtc"/>) is therefore treated as
/// immutable and carried forward from the first registration. Because a standalone
/// backup's catalog-index key orders by that timestamp, holding it stable makes a
/// re-capture an idempotent in-place upsert rather than a re-key that would leave
/// an orphaned duplicate row a value-less delete could not later remove.
/// </para>
/// <para>
/// Set membership (<see cref="BackupManifest.SetId"/> and friends) is deliberately
/// <b>not</b> preserved: the multi-tree set capture legitimately re-registers each
/// member to stamp the shared set identity after the members are captured, and that
/// stamp must win.
/// </para>
/// </summary>
internal static class BackupManifestRegistration
{
    /// <summary>
    /// Returns the manifest to store for <paramref name="incoming"/> given the
    /// <paramref name="existing"/> manifest already registered under the same id
    /// (or <see langword="null"/> when the id is new).
    /// </summary>
    /// <param name="existing">The manifest currently stored under the id, or <see langword="null"/>.</param>
    /// <param name="incoming">The manifest being registered.</param>
    /// <returns>
    /// <paramref name="incoming"/> when the id is new; otherwise <paramref name="incoming"/>
    /// with the immutable capture timestamp preserved from <paramref name="existing"/>.
    /// </returns>
    public static BackupManifest Reconcile(BackupManifest? existing, BackupManifest incoming)
    {
        ArgumentNullException.ThrowIfNull(incoming);

        if (existing is null)
        {
            return incoming;
        }

        return incoming with
        {
            CreatedAtUtc = existing.CreatedAtUtc,
        };
    }
}
