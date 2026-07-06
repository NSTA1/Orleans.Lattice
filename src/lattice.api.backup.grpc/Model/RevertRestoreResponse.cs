namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the revert-restore RPC. Carries no fields: the revert is a
/// void, idempotent operation, so an empty acknowledgement signals success.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.RevertRestoreResponse)]
[Immutable]
public sealed record RevertRestoreResponse;
