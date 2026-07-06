namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the delete-backup RPC. <see cref="Deleted"/> is
/// <see langword="true"/> when a backup was removed, <see langword="false"/>
/// when none with the requested id existed.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupDeleteResponse)]
[Immutable]
public sealed record BackupDeleteResponse
{
    /// <summary>Whether a backup was deleted.</summary>
    [Id(0)] public bool Deleted { get; init; }
}
