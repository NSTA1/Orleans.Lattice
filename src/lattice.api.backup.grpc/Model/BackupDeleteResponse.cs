namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the delete-backup RPC. <see cref="Deleted"/> is
/// <see langword="true"/> when a backup was removed, <see langword="false"/>
/// when nothing was deleted: none with the requested id existed, or it was
/// listed again while the delete ran and kept. After <see langword="false"/>,
/// re-read the backup and delete it again if it is still listed.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupDeleteResponse)]
[Immutable]
public sealed record BackupDeleteResponse
{
    /// <summary>Whether a backup was deleted.</summary>
    [Id(0)] public bool Deleted { get; init; }
}
