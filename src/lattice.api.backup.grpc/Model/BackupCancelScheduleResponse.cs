namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the cancel-schedule RPC. Carries no fields: removing a
/// schedule is a void, idempotent operation, so an empty acknowledgement signals success.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupCancelScheduleResponse)]
[Immutable]
public sealed record BackupCancelScheduleResponse;
