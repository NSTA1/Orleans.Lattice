using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc;

/// <summary>
/// Wire request for the capability-probe RPC: the scope whose backup / restore
/// capabilities to evaluate for the calling credential. The response reuses the
/// transport-agnostic <see cref="Orleans.Lattice.Api.Backup.BackupScopeCapabilities"/>
/// result directly.
/// </summary>
[GenerateSerializer]
[Alias(GrpcBackupTypeAliases.BackupCapabilityProbeRequest)]
[Immutable]
public sealed record BackupCapabilityProbeRequest
{
    /// <summary>The scope to probe. Must be non-<c>null</c> on a well-formed request.</summary>
    [Id(0)] public required BackupScopeSelector Scope { get; init; }
}
