using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// Builds sample backup domain objects for the explorer backup tests, so the
/// fake client can return well-formed results without every test spelling out a
/// full manifest.
/// </summary>
internal static class SampleBackup
{
    /// <summary>Builds a minimal, well-formed <see cref="BackupManifest"/>.</summary>
    public static BackupManifest Manifest(
        string id = "backup-1",
        BackupKind kind = BackupKind.Full,
        string treeId = "orders",
        string? setId = null,
        string? setName = null,
        DateTimeOffset? createdAtUtc = null)
    {
        var scope = BackupScopeSelector.WholeTree(treeId);
        return new BackupManifest(
            id: id,
            name: "nightly",
            createdAtUtc: createdAtUtc ?? DateTimeOffset.UnixEpoch,
            kind: kind,
            scope: scope,
            consistencyCut: new BackupConsistencyCut(42, 100),
            topology: new BackupTopologySnapshot(2, 4096, new[] { "d0", "d1" }),
            structuralDigest: "digest-root",
            keyDescriptors: new[] { new BackupKeyDescriptor("order-1", BackupKeyMergeMode.Crdt, "replica-a") },
            contentDescriptors: new[] { new BackupContentDescriptor("artifact-1", "abc123", 12, 1, scope) },
            provenance: new[] { new BackupOriginProvenance("replica-a", 42) },
            baseBackupId: kind == BackupKind.Incremental ? "base-1" : null)
        {
            SetId = setId,
            SetName = setName,
        };
    }

    /// <summary>Builds a well-formed <see cref="LatticeRestoreResult"/>.</summary>
    public static LatticeRestoreResult RestoreResult(string backupId, string targetTreeId, long entriesApplied) =>
        new(
            backupId: backupId,
            targetTreeId: targetTreeId,
            mode: LatticeRestoreMode.InPlace,
            operationId: "op-1",
            manifestChain: new[] { backupId },
            entriesApplied: entriesApplied);

    /// <summary>Builds a well-formed <see cref="LatticeBackupSetCaptureResult"/> over the given member ids.</summary>
    /// <param name="setId">
    /// The set id to report, or <c>null</c> for a single-member set (which stamps
    /// no membership and so carries no id).
    /// </param>
    /// <param name="memberIds">The member backup ids.</param>
    public static LatticeBackupSetCaptureResult SetResult(string? setId, params string[] memberIds)
    {
        var members = memberIds.Select(id => new LatticeBackupCaptureResult(id, Manifest(id))).ToList();
        var setManifest = new BackupSetManifest(
            setId: setId,
            name: "nightly-set",
            createdAtUtc: DateTimeOffset.UnixEpoch,
            crossTreeConsistent: false,
            fence: null,
            memberBackupIds: memberIds);
        return new LatticeBackupSetCaptureResult(setManifest, members);
    }
}
