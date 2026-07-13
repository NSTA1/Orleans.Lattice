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
/// </summary>
internal sealed class LatticeBackupHealthService(ILatticeBackupSink sink) : ILatticeBackupHealthService
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

        var explanation = BuildExplanation(backupId, status, missing, mismatches);
        return new BackupHealthReport(
            backupId,
            status,
            manifestPresent: true,
            missingArtifactIds: missing,
            hashMismatchArtifactIds: mismatches,
            checkedAt,
            explanation);
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
        IReadOnlyList<string> mismatches)
    {
        if (status == BackupHealthStatus.Healthy)
        {
            return $"Backup '{backupId}' is healthy: its manifest and every referenced "
                + "artifact are present, committed, and hash-verified against the manifest.";
        }

        var builder = new StringBuilder();
        builder.Append($"Backup '{backupId}' is not fully resolvable. ");
        if (missing.Count > 0)
        {
            builder.Append($"Missing or uncommitted artifact(s): {string.Join(", ", missing)}. ");
        }

        if (mismatches.Count > 0)
        {
            builder.Append(
                $"Artifact(s) whose stored content no longer matches the manifest's recorded hash: {string.Join(", ", mismatches)}. ");
        }

        builder.Append("Do not rely on this backup as a restore point until the fault is investigated.");
        return builder.ToString();
    }
}
