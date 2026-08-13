namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request carrying no parameters, used by the tree-administration control-API
/// cluster-wide runtime materialised-view listing RPC. It addresses no single tree
/// (the listing spans every runtime-registered view on the cluster), so it carries
/// no tree id and no view name.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminViewListRequest)]
[Immutable]
public sealed record TreeAdminViewListRequest;
