namespace Orleans.Lattice.Backup;

/// <summary>
/// Reconciles a manifest that is being registered against the manifest already
/// stored under the same backup id.
/// <para>
/// A backup id is a content address (<see cref="BackupIdentity"/>), so a capture
/// of identical bytes re-derives an existing id and republishes it, and a catalog
/// rebuild or a set's membership stamp re-registers one: either way it is the
/// <i>same</i> backup, not a new one. Its capture
/// time (<see cref="BackupManifest.CreatedAtUtc"/>) is therefore treated as
/// immutable and carried forward from the first registration. Because a standalone
/// backup's catalog-index key orders by that timestamp, holding it stable makes a
/// re-capture an idempotent in-place upsert rather than a re-key that would leave
/// an orphaned duplicate row a value-less delete could not later remove.
/// </para>
/// <para>
/// The latest capture is recorded instead in
/// <see cref="BackupManifest.LastCapturedAtUtc"/>, the later of the two
/// registrations' ranking times (<see cref="BackupRetentionPlanner.RankTime"/>),
/// which is what retention ranks by: a backup a capture has just republished is
/// never pruned as old.
/// </para>
/// <para>
/// Set membership (<see cref="BackupManifest.SetId"/> and friends) is deliberately
/// <b>not</b> preserved: the incoming manifest is authoritative for every field but
/// the two capture times. A set's membership stamp therefore moves a republished
/// backup into its set, and a plain capture of identical content republishes it
/// unstamped, as in 10.2.0.
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
    /// with the immutable capture timestamp preserved from <paramref name="existing"/>
    /// and the later of the two ranking times as its last capture time.
    /// </returns>
    public static BackupManifest Reconcile(BackupManifest? existing, BackupManifest incoming)
    {
        ArgumentNullException.ThrowIfNull(incoming);

        if (existing is null)
        {
            return incoming;
        }

        var existingRank = BackupRetentionPlanner.RankTime(existing);
        var incomingRank = BackupRetentionPlanner.RankTime(incoming);
        var latest = existingRank >= incomingRank ? existingRank : incomingRank;
        return incoming with
        {
            CreatedAtUtc = existing.CreatedAtUtc,
            LastCapturedAtUtc = latest > existing.CreatedAtUtc ? latest : null,
        };
    }
}
