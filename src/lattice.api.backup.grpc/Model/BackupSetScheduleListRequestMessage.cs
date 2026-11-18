namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the list-backup-set-schedules RPC. Carries no fields: the
/// listing is scoped to the caller's tenant and authorization.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupSetScheduleListRequestMessage)]
[Immutable]
public sealed record BackupSetScheduleListRequestMessage;
