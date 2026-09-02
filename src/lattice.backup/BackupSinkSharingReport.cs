namespace Orleans.Lattice.Backup;

/// <summary>
/// The outcome of one cross-cluster backup-sink sharing probe: whether the sink
/// this cluster captures into is demonstrably the same store every peer reads
/// from, which peers could not be confirmed, and a human-readable explanation
/// naming the remediation. A report is a point-in-time snapshot; the periodic
/// backup-health sweep refreshes it.
/// <para>
/// <see cref="UnconfirmedPeerClusterIds"/> is the actionable payload: it lists
/// exactly the peers whose marker this cluster could not read back from its own
/// sink. When <see cref="Status"/> is
/// <see cref="BackupSinkSharingStatus.NotShared"/> those peers are known to be up
/// (they answered the saga control channel), so the sink is positively refuted;
/// when it is <see cref="BackupSinkSharingStatus.Unverified"/> they were simply
/// not reachable and the verdict is undecided.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSinkSharingReport)]
[Immutable]
public sealed record BackupSinkSharingReport
{
    /// <summary>Initializes a new <see cref="BackupSinkSharingReport"/>.</summary>
    /// <param name="status">The probe verdict.</param>
    /// <param name="clusterId">
    /// The local cluster's stable id, as attested by the marker this cluster
    /// wrote. Must not be <c>null</c>.
    /// </param>
    /// <param name="peerCount">The number of peer clusters the probe considered. Must not be negative.</param>
    /// <param name="unconfirmedPeerClusterIds">
    /// The peers whose sink marker could not be read back from this cluster's
    /// sink. Must not be <c>null</c>.
    /// </param>
    /// <param name="probedAtUtc">The wall-clock time the probe ran.</param>
    /// <param name="explanation">
    /// A precise, human-readable description of the verdict, naming the
    /// unconfirmed peers and the remediation. Must not be <c>null</c>.
    /// </param>
    /// <exception cref="ArgumentNullException">A required reference argument is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="peerCount"/> is negative.</exception>
    public BackupSinkSharingReport(
        BackupSinkSharingStatus status,
        string clusterId,
        int peerCount,
        IReadOnlyList<string> unconfirmedPeerClusterIds,
        DateTimeOffset probedAtUtc,
        string explanation)
    {
        ArgumentNullException.ThrowIfNull(clusterId);
        ArgumentNullException.ThrowIfNull(unconfirmedPeerClusterIds);
        ArgumentNullException.ThrowIfNull(explanation);
        ArgumentOutOfRangeException.ThrowIfNegative(peerCount);
        Status = status;
        ClusterId = clusterId;
        PeerCount = peerCount;
        UnconfirmedPeerClusterIds = unconfirmedPeerClusterIds;
        ProbedAtUtc = probedAtUtc;
        Explanation = explanation;
    }

    /// <summary>The probe verdict for the local cluster's configured sink.</summary>
    [Id(0)]
    public BackupSinkSharingStatus Status { get; init; }

    /// <summary>The local cluster's stable id, as attested by the marker it wrote.</summary>
    [Id(1)]
    public string ClusterId { get; init; }

    /// <summary>The number of peer clusters the probe considered. Zero means no probe was needed.</summary>
    [Id(2)]
    public int PeerCount { get; init; }

    /// <summary>
    /// The peers whose sink marker could not be read back from this cluster's
    /// sink. Empty when every peer was confirmed, or when the probe did not apply.
    /// </summary>
    [Id(3)]
    public IReadOnlyList<string> UnconfirmedPeerClusterIds { get; init; }

    /// <summary>The wall-clock time the probe that produced this report ran.</summary>
    [Id(4)]
    public DateTimeOffset ProbedAtUtc { get; init; }

    /// <summary>
    /// A precise, human-readable description of the verdict, naming the
    /// unconfirmed peers and the remediation, suitable for rendering directly in
    /// a diagnostics dialog or a startup log line.
    /// </summary>
    [Id(5)]
    public string Explanation { get; init; }

    /// <summary>
    /// <see langword="true"/> when the probe positively refuted the sink
    /// (<see cref="Status"/> is <see cref="BackupSinkSharingStatus.NotShared"/>):
    /// a reachable peer cannot read what this cluster wrote, so a coordinated
    /// restore of a replicated tree would abort.
    /// </summary>
    public bool IsRefuted => Status == BackupSinkSharingStatus.NotShared;
}
