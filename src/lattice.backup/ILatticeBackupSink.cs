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
    /// Whether this sink stores backup payload durably outside the cluster that
    /// captured it, so a backup survives the loss of that cluster. A durable /
    /// external sink (for example the Azure Blob backend or a shared filesystem
    /// backend) reports <see langword="true"/>; the default in-cluster sink, which
    /// dogfoods a reserved tree in the same cluster whose loss the backup is meant
    /// to protect against, reports <see langword="false"/>.
    /// <para>
    /// Periodic backup-health verification is only meaningful against a durable
    /// sink: verifying payload that lives in the same ephemeral cluster proves
    /// nothing about disaster recovery. The health monitor consults this capability
    /// to stay inert on a non-durable sink, and a management UI hides or disables
    /// its health surface accordingly. Prefer this flag over an <c>is</c>-type
    /// check so a new durable sink is covered without a change here.
    /// </para>
    /// </summary>
    bool IsDurable { get; }

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
    /// Cheaply tests whether the sink holds a manifest for <paramref name="backupId"/>,
    /// using a single existence / metadata probe that never downloads payload. This
    /// is the minimum liveness signal a selection surface consults so a catalog row
    /// whose sink manifest is gone is not offered as a base or restore point.
    /// </summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns><c>true</c> when the sink holds the manifest; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    Task<bool> ManifestExistsAsync(string backupId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Probes, read-only and cheaply, whether a backup is fully resolvable from the
    /// sink: whether the manifest is present and whether every artifact it
    /// references exists and is committed. The returned
    /// <see cref="BackupSinkResolution"/> reports the manifest-presence flag and the
    /// ids of any missing artifacts so a caller can both decide resolvability
    /// (<see cref="BackupSinkResolution.IsResolvable"/>) and explain a fault. The
    /// probe checks existence and the committed-metadata flag only - it never
    /// downloads or hashes artifact payload - so it stays cheap enough to run across
    /// a large catalog.
    /// </summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the probe.</param>
    /// <returns>The sink-resolution outcome for the backup.</returns>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    Task<BackupSinkResolution> ProbeAsync(string backupId, CancellationToken cancellationToken = default);

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
