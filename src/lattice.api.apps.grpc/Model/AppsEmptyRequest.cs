namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Empty request for catalog and capability RPCs.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsEmptyRequest), Immutable]
public sealed record AppsEmptyRequest;
