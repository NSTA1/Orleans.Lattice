namespace Orleans.Lattice.Api.Replication.Grpc;

/// <summary>
/// Wire request for the <c>GetReplicationConfig</c> RPC. Carries no fields: the
/// report is scoped server-side to the trees the authenticated caller is
/// authorized to manage.
/// </summary>
[GenerateSerializer]
[Alias(GrpcReplicationTypeAliases.ReplicationGetConfigRequest)]
[Immutable]
public sealed record ReplicationGetConfigRequest;
