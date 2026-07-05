namespace Orleans.Lattice.Backup;

/// <summary>
/// The pluggable storage sink a backup is written to and restored from. A sink
/// stores two kinds of content: streamed, content-addressed <b>artifacts</b> (the
/// captured bytes) and the self-describing <b>manifest</b> that describes them.
/// The artifact surface is async-streaming-friendly - both write and read move
/// the payload as an ordered sequence of chunks rather than a single buffered
/// blob - so the capture engine can stream a large tree without materializing it
/// whole.
/// <para>
/// The default in-cluster implementation dogfoods a reserved <c>sys-backup-*</c>
/// tree; a durable provider (for example an Azure append-blob backend) is a later
/// sub-issue that implements this same interface. Artifact ids are expected to be
/// content-addressed (see <see cref="BackupContentHash"/>) so re-writing identical
/// content is idempotent and never produces a duplicate.
/// </para>
/// </summary>
public interface ILatticeBackupSink
{
    /// <summary>
    /// Writes an artifact as an ordered stream of chunks under
    /// <paramref name="artifactId"/>. Re-writing the same id with the same content
    /// is idempotent. When the id is content-addressed, an identical retry is a
    /// no-op that does not duplicate the artifact.
    /// </summary>
    /// <param name="artifactId">The content-addressed artifact id. Must not be <c>null</c> or empty.</param>
    /// <param name="content">The ordered chunk stream of the artifact bytes. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentException"><paramref name="artifactId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="content"/> is <c>null</c>.</exception>
    Task WriteArtifactAsync(
        string artifactId,
        IAsyncEnumerable<ReadOnlyMemory<byte>> content,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads an artifact back as an ordered stream of chunks. Yields nothing when
    /// no artifact with <paramref name="artifactId"/> exists.
    /// </summary>
    /// <param name="artifactId">The artifact id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The ordered chunk stream of the artifact bytes.</returns>
    /// <exception cref="ArgumentException"><paramref name="artifactId"/> is <c>null</c> or empty.</exception>
    IAsyncEnumerable<ReadOnlyMemory<byte>> ReadArtifactAsync(
        string artifactId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes an artifact. Returns <c>true</c> when an artifact was removed,
    /// <c>false</c> when none existed.
    /// </summary>
    /// <param name="artifactId">The artifact id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the delete.</param>
    /// <exception cref="ArgumentException"><paramref name="artifactId"/> is <c>null</c> or empty.</exception>
    Task<bool> DeleteArtifactAsync(string artifactId, CancellationToken cancellationToken = default);

    /// <summary>Enumerates the ids of every artifact held by the sink, in id order.</summary>
    /// <param name="cancellationToken">Cancels the scan.</param>
    IAsyncEnumerable<string> ListArtifactIdsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates or replaces the manifest keyed by its
    /// <see cref="BackupManifest.Id"/>. Writing the same manifest twice is
    /// idempotent.
    /// </summary>
    /// <param name="manifest">The manifest to persist. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentNullException"><paramref name="manifest"/> is <c>null</c>.</exception>
    Task WriteManifestAsync(BackupManifest manifest, CancellationToken cancellationToken = default);

    /// <summary>Reads a manifest by backup id, or <c>null</c> when none exists.</summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    Task<BackupManifest?> ReadManifestAsync(string backupId, CancellationToken cancellationToken = default);

    /// <summary>Enumerates every manifest held by the sink, in backup-id order.</summary>
    /// <param name="cancellationToken">Cancels the scan.</param>
    IAsyncEnumerable<BackupManifest> ListManifestsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a manifest by backup id. Returns <c>true</c> when a manifest was
    /// removed, <c>false</c> when none existed. Does not remove the artifacts the
    /// manifest references.
    /// </summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the delete.</param>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    Task<bool> DeleteManifestAsync(string backupId, CancellationToken cancellationToken = default);
}
