using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="ILatticeBackupIncrementalCaptureService"/> used until a
/// cluster wires a dedicated differential-capture engine. It reuses the landed
/// full-capture engine to snapshot the scope, then re-registers the resulting
/// manifest as an <see cref="BackupKind.Incremental"/> keyed by a distinct
/// derived id and carrying the requested
/// <see cref="LatticeBackupIncrementalCaptureRequest.BaseBackupId"/>. The
/// interim full manifest the capture registered is removed so the catalog holds
/// exactly one manifest per captured backup.
/// <para>
/// This is a chain-shape-correct stand-in: the increment records a real base and
/// participates in retention chain-integrity exactly like a true differential,
/// but its payload is a full snapshot rather than a delta. It is a
/// <c>TryAddSingleton</c> default, so a real incremental engine registered by a
/// later feature transparently replaces it. This is the reconciliation point for
/// the incremental-capture sub-issue.
/// </para>
/// </summary>
internal sealed class BaselineIncrementalBackupCaptureService(
    ILatticeBackupCaptureService fullCapture,
    ILatticeBackupSink sink,
    ILatticeBackupCatalogStore catalog,
    ILogger<BaselineIncrementalBackupCaptureService> logger)
    : ILatticeBackupIncrementalCaptureService
{
    /// <inheritdoc />
    public async Task<LatticeBackupCaptureResult> CaptureIncrementalAsync(
        LatticeBackupIncrementalCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Snapshot the scope through the landed full-capture engine. This writes
        // the artifact plus an interim full manifest to the sink and registers it
        // in the catalog under its content-addressed id.
        var full = await fullCapture
            .CaptureAsync(new LatticeBackupCaptureRequest(request.Name, request.Scope, request.PageSize), cancellationToken)
            .ConfigureAwait(false);

        var incrementalId = DeriveIncrementalId(full.BackupId, request.BaseBackupId);
        var incremental = new BackupManifest(
            id: incrementalId,
            name: full.Manifest.Name,
            createdAtUtc: full.Manifest.CreatedAtUtc,
            kind: BackupKind.Incremental,
            scope: full.Manifest.Scope,
            consistencyCut: full.Manifest.ConsistencyCut,
            topology: full.Manifest.Topology,
            structuralDigest: full.Manifest.StructuralDigest,
            keyDescriptors: full.Manifest.KeyDescriptors,
            contentDescriptors: full.Manifest.ContentDescriptors,
            provenance: full.Manifest.Provenance,
            baseBackupId: request.BaseBackupId,
            compressionDictionary: full.Manifest.CompressionDictionary);

        // Register the incremental manifest, then remove the interim full manifest
        // the capture wrote under its own id. The shared artifact is content-keyed
        // and referenced by the incremental manifest, so it is left in place. The
        // interim manifest is only removed when its id is genuinely a throwaway:
        // never when it collides with the increment's own id or with the base
        // backup, so an unchanged-payload increment can never delete its own base.
        await sink.WriteManifestAsync(incremental, cancellationToken).ConfigureAwait(false);
        await catalog.RegisterAsync(incremental, cancellationToken).ConfigureAwait(false);

        if (!string.Equals(incrementalId, full.BackupId, StringComparison.Ordinal)
            && !string.Equals(full.BackupId, request.BaseBackupId, StringComparison.Ordinal))
        {
            await catalog.RemoveAsync(full.BackupId, cancellationToken).ConfigureAwait(false);
            await sink.DeleteManifestAsync(full.BackupId, cancellationToken).ConfigureAwait(false);
        }

        logger.LogInformation(
            "Captured baseline-stand-in incremental backup {IncrementalId} of scope {Scope} layered on base {BaseId}.",
            incrementalId, BackupScopeKey.For(request.Scope), request.BaseBackupId);

        return new LatticeBackupCaptureResult(incrementalId, incremental);
    }

    // A distinct, separator-free, content-derived id so the increment never
    // collides with its base even when their payloads are byte-identical.
    private static string DeriveIncrementalId(string fullBackupId, string baseBackupId)
    {
        var bytes = Encoding.UTF8.GetBytes(string.Concat(fullBackupId, "\u0000", baseBackupId, "\u0000incremental"));
        return Convert.ToHexStringLower(SHA256.HashData(bytes));
    }
}
