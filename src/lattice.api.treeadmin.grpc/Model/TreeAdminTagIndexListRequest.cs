namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request carrying no parameters, used by the tree-administration control-API
/// cluster-wide tag-index listing RPC. It addresses no single tree (the listing spans
/// every tag index on the cluster), so it carries no tree id and no index name.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminTagIndexListRequest)]
[Immutable]
public sealed record TreeAdminTagIndexListRequest;
