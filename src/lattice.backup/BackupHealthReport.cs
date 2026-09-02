namespace Orleans.Lattice.Backup;

/// <summary>
/// The result of verifying one backup's durable sink payload: the overall
/// <see cref="Status"/>, whether the manifest is present, the ids of any referenced
/// artifacts that are missing or uncommitted, the ids of any artifacts whose stored
/// content no longer matches the hash the manifest recorded at capture time, the
/// wall-clock time the verification ran, and a precise human-readable
/// <see cref="Explanation"/> that names exactly which blob is missing or which hash
/// mismatched so a diagnostics dialog can render the fault without further lookups.
/// <para>
/// This layers hash-consistency verification on top of the cheap presence probe
/// (<see cref="ILatticeBackupSink.ProbeAsync"/>): presence and committed-metadata
/// come from the probe, and every present artifact is additionally downloaded and
/// re-hashed against its <see cref="BackupContentDescriptor.ContentHash"/>. A report
/// is a point-in-time snapshot and is persisted per backup so the monitor and the
/// UI share one verification result.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupHealthReport)]
[Immutable]
public sealed record BackupHealthReport
{
    /// <summary>Initializes a new <see cref="BackupHealthReport"/>.</summary>
    /// <param name="backupId">The verified backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="status">The overall health status.</param>
    /// <param name="manifestPresent">Whether the backup's manifest is present in the sink.</param>
    /// <param name="missingArtifactIds">
    /// The ids of referenced artifacts that are missing from the sink (absent, or
    /// present but not committed). Must not be <c>null</c>.
    /// </param>
    /// <param name="hashMismatchArtifactIds">
    /// The ids of artifacts whose stored content no longer matches the manifest's
    /// recorded hash. Must not be <c>null</c>.
    /// </param>
    /// <param name="checkedAtUtc">The wall-clock time the verification ran.</param>
    /// <param name="explanation">
    /// A precise, human-readable description of the outcome, naming the specific
    /// missing or mismatched artifacts. Must not be <c>null</c>.
    /// </param>
    /// <param name="peerVisibility">
    /// Whether the sink holding this backup is demonstrably shared with every peer
    /// cluster, for a backup of a replicated tree. Defaults to
    /// <see cref="BackupSinkSharingStatus.NotApplicable"/>, which is correct for a
    /// non-replicated tree, a single-cluster deployment, and every report captured
    /// before the cross-cluster sharing probe existed.
    /// </param>
    /// <param name="peerUnconfirmedClusterIds">
    /// The peer clusters that could not read this cluster's backup sink. <c>null</c>
    /// is normalised to an empty list.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException">A required reference argument is <c>null</c>.</exception>
    public BackupHealthReport(
        string backupId,
        BackupHealthStatus status,
        bool manifestPresent,
        IReadOnlyList<string> missingArtifactIds,
        IReadOnlyList<string> hashMismatchArtifactIds,
        DateTimeOffset checkedAtUtc,
        string explanation,
        BackupSinkSharingStatus peerVisibility = BackupSinkSharingStatus.NotApplicable,
        IReadOnlyList<string>? peerUnconfirmedClusterIds = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        ArgumentNullException.ThrowIfNull(missingArtifactIds);
        ArgumentNullException.ThrowIfNull(hashMismatchArtifactIds);
        ArgumentNullException.ThrowIfNull(explanation);
        BackupId = backupId;
        Status = status;
        ManifestPresent = manifestPresent;
        MissingArtifactIds = missingArtifactIds;
        HashMismatchArtifactIds = hashMismatchArtifactIds;
        CheckedAtUtc = checkedAtUtc;
        Explanation = explanation;
        PeerVisibility = peerVisibility;
        PeerUnconfirmedClusterIds = peerUnconfirmedClusterIds ?? [];
    }

    /// <summary>The verified backup id.</summary>
    [Id(0)]
    public string BackupId { get; init; }

    /// <summary>The overall health status of the backup's sink payload.</summary>
    [Id(1)]
    public BackupHealthStatus Status { get; init; }

    /// <summary>Whether the backup's manifest is present in the sink.</summary>
    [Id(2)]
    public bool ManifestPresent { get; init; }

    /// <summary>
    /// The ids of referenced artifacts that are missing from the sink (absent, or
    /// present but not committed). Empty when the manifest is absent or every
    /// artifact is present and committed.
    /// </summary>
    [Id(3)]
    public IReadOnlyList<string> MissingArtifactIds { get; init; }

    /// <summary>
    /// The ids of artifacts whose stored content no longer hashes to the digest the
    /// manifest recorded at capture time. Empty when every present artifact's
    /// content still matches its recorded hash.
    /// </summary>
    [Id(4)]
    public IReadOnlyList<string> HashMismatchArtifactIds { get; init; }

    /// <summary>The wall-clock time the verification that produced this report ran.</summary>
    [Id(5)]
    public DateTimeOffset CheckedAtUtc { get; init; }

    /// <summary>
    /// A precise, human-readable description of the verification outcome, naming the
    /// specific missing or mismatched artifacts, suitable for rendering directly in
    /// a diagnostics dialog.
    /// </summary>
    [Id(6)]
    public string Explanation { get; init; }

    /// <summary>
    /// Whether the sink holding this backup is demonstrably shared with every peer
    /// cluster. Only meaningful for a backup of a replicated tree: a coordinated
    /// restore resolves the same manifest chain from every cluster's own sink, so a
    /// backup that is locally intact but invisible to a peer is not a usable restore
    /// point for the replication set. <see cref="BackupSinkSharingStatus.NotApplicable"/>
    /// for a non-replicated tree, a single-cluster deployment, or a report captured
    /// before the cross-cluster sharing probe existed.
    /// </summary>
    [Id(7)]
    public BackupSinkSharingStatus PeerVisibility { get; init; }

    /// <summary>
    /// The peer clusters that could not be confirmed to read this cluster's backup
    /// sink. Empty unless <see cref="PeerVisibility"/> is
    /// <see cref="BackupSinkSharingStatus.NotShared"/> or
    /// <see cref="BackupSinkSharingStatus.Unverified"/>.
    /// </summary>
    [Id(8)]
    public IReadOnlyList<string> PeerUnconfirmedClusterIds { get; init; }

    /// <summary><see langword="true"/> when <see cref="Status"/> is <see cref="BackupHealthStatus.Healthy"/>.</summary>
    public bool IsHealthy => Status == BackupHealthStatus.Healthy;
}
