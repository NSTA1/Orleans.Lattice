namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the server-streaming export-artifact RPC: the owning
/// <see cref="BackupId"/> and the <see cref="ArtifactId"/> to stream back.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.ArtifactExportRequest)]
[Immutable]
public sealed record ArtifactExportRequest
{
    /// <summary>The owning backup id.</summary>
    [Id(0)] public required string BackupId { get; init; }

    /// <summary>The artifact id to export.</summary>
    [Id(1)] public required string ArtifactId { get; init; }
}
