namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire response for the health-monitoring-availability RPC: whether periodic
/// backup-health monitoring applies on this deployment (true only when the
/// configured sink is durable and external).
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupHealthAvailabilityResponse)]
[Immutable]
public sealed record BackupHealthAvailabilityResponse
{
    /// <summary>Whether backup-health monitoring is available (the sink is durable).</summary>
    [Id(0)] public bool Available { get; init; }
}
