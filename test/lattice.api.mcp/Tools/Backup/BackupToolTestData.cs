using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Builders for the backup domain models the backup MCP tool tests project onto
/// their DTOs. Kept in one place so a manifest can be minted with the handful of
/// fields the tools actually surface without repeating the manifest's long
/// constructor at every call site.
/// </summary>
internal static class BackupToolTestData
{
    /// <summary>Mints a manifest with the given identity, scope, and artifact count.</summary>
    public static BackupManifest Manifest(
        string id,
        string name,
        BackupScopeSelector scope,
        BackupKind kind = BackupKind.Full,
        string? baseBackupId = null,
        int artifactCount = 1,
        string? capturingClusterId = null)
    {
        var contentDescriptors = new BackupContentDescriptor[artifactCount];
        for (var i = 0; i < artifactCount; i++)
        {
            contentDescriptors[i] = new BackupContentDescriptor($"artifact-{i}", $"hash-{i}", 12, 1, scope);
        }

        return new BackupManifest(
            id: id,
            name: name,
            createdAtUtc: DateTimeOffset.UnixEpoch,
            kind: kind,
            scope: scope,
            consistencyCut: new BackupConsistencyCut(42, 100),
            topology: new BackupTopologySnapshot(2, 4096, new[] { "d0", "d1" }),
            structuralDigest: "digest-root",
            keyDescriptors: new[] { new BackupKeyDescriptor("order-1", BackupKeyMergeMode.Crdt, "replica-a") },
            contentDescriptors: contentDescriptors,
            provenance: new[] { new BackupOriginProvenance("replica-a", 42) },
            baseBackupId: baseBackupId,
            capturingClusterId: capturingClusterId);
    }
}
