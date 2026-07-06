namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the server-streaming <c>StreamBackups</c> RPC. Carries no
/// fields: the whole catalog the caller may read is drained, in backup-id order,
/// with bounded memory.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupStreamRequest)]
[Immutable]
public sealed record BackupStreamRequest;
