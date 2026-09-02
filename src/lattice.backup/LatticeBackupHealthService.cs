using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupHealthService"/>. Verifies a backup in two
/// stages: it reuses <see cref="ILatticeBackupSink.ProbeAsync"/> for the cheap
/// presence signal (is the manifest present, and which referenced artifacts are
/// missing or uncommitted), then downloads every <i>present</i> artifact and
/// re-hashes its streamed content against the
/// <see cref="BackupContentDescriptor.ContentHash"/> the manifest recorded at
/// capture time, so silent bit-rot or an out-of-band edit is caught in addition to
/// a deletion. Missing artifacts are not downloaded (the probe already classified
/// them), so the extra cost over the probe is one streamed hash per present
/// artifact.
/// <para>
/// For a backup of a <b>replicated</b> tree the local verdict is not the whole
/// story: a coordinated restore resolves the same manifest chain from every
/// cluster's own sink, so a backup that is locally intact but sits in a sink no
/// peer can read is not a usable restore point. The service therefore reads the
/// most recent cross-cluster verdict from <see cref="IBackupSinkSharingProbe"/> -
/// a cached value, never fresh I/O, because sink sharing is a slow-moving
/// deployment fact refreshed once per health sweep - and downgrades an otherwise
/// healthy replicated-tree backup to <see cref="BackupHealthStatus.Warning"/> with
/// a reason naming the peers.
/// </para>
/// </summary>
internal sealed class LatticeBackupHealthService(
    ILatticeBackupSink sink,
    IBackupSinkSharingProbe? sharingProbe = null,
    IReplicatedTreeMembership? membership = null) : ILatticeBackupHealthService
{
    private readonly ILatticeBackupSink _sink = sink ?? throw new ArgumentNullException(nameof(sink));

    /// <inheritdoc />
    public async Task<BackupHealthReport> VerifyAsync(string backupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);

        var checkedAt = DateTimeOffset.UtcNow;

        var resolution = await _sink.ProbeAsync(backupId, cancellationToken).ConfigureAwait(false);
        if (!resolution.ManifestPresent)
        {
            return new BackupHealthReport(
                backupId,
                BackupHealthStatus.Missing,
                manifestPresent: false,
                missingArtifactIds: Array.Empty<string>(),
                hashMismatchArtifactIds: Array.Empty<string>(),
                checkedAt,
                $"The backup manifest '{backupId}' is absent from the durable sink; the backup cannot be resolved or restored.");
        }

        var manifest = await _sink.ReadManifestAsync(backupId, cancellationToken).ConfigureAwait(false);
        if (manifest is null)
        {
            // Raced with a concurrent delete between the probe and the manifest
            // read: treat it as missing rather than crash.
            return new BackupHealthReport(
                backupId,
                BackupHealthStatus.Missing,
                manifestPresent: false,
                missingArtifactIds: Array.Empty<string>(),
                hashMismatchArtifactIds: Array.Empty<string>(),
                checkedAt,
                $"The backup manifest '{backupId}' disappeared from the durable sink during verification; the backup cannot be resolved.");
        }

        var missing = resolution.MissingArtifactIds;
        var missingSet = new HashSet<string>(missing, StringComparer.Ordinal);

        // Hash-verify only the artifacts the probe reported as present; a missing
        // artifact is already classified and downloading it would be pointless.
        var mismatches = new List<string>();
        var verifiedHashes = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var descriptor in manifest.ContentDescriptors)
        {
            if (missingSet.Contains(descriptor.ArtifactId) || verifiedHashes.ContainsKey(descriptor.ArtifactId))
            {
                continue;
            }

            var actualHash = await ComputeArtifactHashAsync(descriptor.ArtifactId, cancellationToken).ConfigureAwait(false);
            verifiedHashes[descriptor.ArtifactId] = actualHash;
            if (!string.Equals(actualHash, descriptor.ContentHash, StringComparison.Ordinal))
            {
                mismatches.Add(descriptor.ArtifactId);
            }
        }

        var status = missing.Count == 0 && mismatches.Count == 0
            ? BackupHealthStatus.Healthy
            : BackupHealthStatus.Warning;

        // A replicated tree's backup is only a restore point if every peer can read
        // the sink holding it. Fold the last cross-cluster verdict in: a positively
        // refuted sink downgrades an otherwise healthy backup to Warning so the
        // misconfiguration is visible in the Explorer HEALTH column long before
        // anyone attempts a coordinated restore.
        var sharing = ResolveSharing(manifest.Scope.TreeId);
        if (sharing is { Status: BackupSinkSharingStatus.NotShared } && status == BackupHealthStatus.Healthy)
        {
            status = BackupHealthStatus.Warning;
        }

        var explanation = BuildExplanation(backupId, status, missing, mismatches, sharing);
        return new BackupHealthReport(
            backupId,
            status,
            manifestPresent: true,
            missingArtifactIds: missing,
            hashMismatchArtifactIds: mismatches,
            checkedAt,
            explanation,
            peerVisibility: sharing?.Status ?? BackupSinkSharingStatus.NotApplicable,
            peerUnconfirmedClusterIds: sharing?.UnconfirmedPeerClusterIds);
    }

    /// <summary>
    /// Resolves the cached cross-cluster sharing verdict for a backup of
    /// <paramref name="treeId"/>, or <see langword="null"/> when the verdict does
    /// not apply. Returns <see langword="null"/> for a non-replicated tree, when the
    /// replication package is not wired, when the probe has never run, and when the
    /// probe reported <see cref="BackupSinkSharingStatus.NotApplicable"/> - so a
    /// single-cluster deployment's health reports are byte-for-byte what they were
    /// before the probe existed.
    /// </summary>
    private BackupSinkSharingReport? ResolveSharing(string treeId)
    {
        if (sharingProbe is null || membership is null || !membership.IsReplicated(treeId))
        {
            return null;
        }

        var report = sharingProbe.LastReport;
        return report is null || report.Status == BackupSinkSharingStatus.NotApplicable
            ? null
            : report;
    }

    private async Task<string> ComputeArtifactHashAsync(string artifactId, CancellationToken cancellationToken)
    {
        // Hash the artifact's streamed chunks incrementally so a large artifact is
        // never buffered whole; the digest matches BackupContentHash.Compute, which
        // the capture path used to derive the recorded content hash.
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        await foreach (var chunk in _sink.ReadArtifactAsync(artifactId, cancellationToken).ConfigureAwait(false))
        {
            hasher.AppendData(chunk.Span);
        }

        return Convert.ToHexStringLower(hasher.GetHashAndReset());
    }

    private static string BuildExplanation(
        string backupId,
        BackupHealthStatus status,
        IReadOnlyList<string> missing,
        IReadOnlyList<string> mismatches,
        BackupSinkSharingReport? sharing)
    {
        var peerFault = sharing is { Status: BackupSinkSharingStatus.NotShared };
        if (status == BackupHealthStatus.Healthy)
        {
            var healthy = $"Backup '{backupId}' is healthy: its manifest and every referenced "
                + "artifact are present, committed, and hash-verified against the manifest.";
            return sharing is { Status: BackupSinkSharingStatus.Unverified }
                ? healthy + " " + DescribeSharing(sharing)
                : healthy;
        }

        var builder = new StringBuilder();
        builder.Append(
            missing.Count == 0 && mismatches.Count == 0
                ? $"Backup '{backupId}' is intact locally but is not a usable restore point for the replication set. "
                : $"Backup '{backupId}' is not fully resolvable. ");
        if (missing.Count > 0)
        {
            builder.Append($"Missing or uncommitted artifact(s): {string.Join(", ", missing)}. ");
        }

        if (mismatches.Count > 0)
        {
            builder.Append(
                $"Artifact(s) whose stored content no longer matches the manifest's recorded hash: {string.Join(", ", mismatches)}. ");
        }

        if (sharing is not null && sharing.Status != BackupSinkSharingStatus.Shared)
        {
            builder.Append(DescribeSharing(sharing)).Append(' ');
        }

        builder.Append(
            peerFault
                ? "A coordinated restore of this replicated tree would abort until every cluster is pointed at the same backup sink."
                : "Do not rely on this backup as a restore point until the fault is investigated.");
        return builder.ToString();
    }

    /// <summary>
    /// Renders the cross-cluster sharing verdict as one sentence naming the peers
    /// that could not be confirmed, so the Explorer HEALTH column's reason states
    /// the remediation rather than merely that something is wrong.
    /// </summary>
    private static string DescribeSharing(BackupSinkSharingReport sharing)
    {
        var peers = sharing.UnconfirmedPeerClusterIds.Count > 0
            ? string.Join(", ", sharing.UnconfirmedPeerClusterIds)
            : "(none reported)";
        return sharing.Status == BackupSinkSharingStatus.NotShared
            ? $"The backup sink is NOT shared with peer cluster(s) {peers}, which are running but cannot see this cluster's sink marker."
            : $"Backup sink sharing with peer cluster(s) {peers} is unconfirmed because they were not reachable.";
    }
}
