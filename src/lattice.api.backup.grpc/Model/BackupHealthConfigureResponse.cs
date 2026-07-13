namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response acknowledging a configure-backup-health RPC. The configuration
/// write either succeeds (returning this empty acknowledgement) or faults with a
/// gRPC status, so the response carries no payload.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupHealthConfigureResponse)]
[Immutable]
public sealed record BackupHealthConfigureResponse
{
}
