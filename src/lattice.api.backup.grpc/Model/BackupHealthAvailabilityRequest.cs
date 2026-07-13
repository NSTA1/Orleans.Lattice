namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the health-monitoring-availability RPC. It carries no
/// payload: availability is a deployment-wide capability that depends only on
/// whether the configured backup sink is durable.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupHealthAvailabilityRequest)]
[Immutable]
public sealed record BackupHealthAvailabilityRequest
{
}
