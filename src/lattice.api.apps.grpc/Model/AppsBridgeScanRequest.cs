namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Requests one page of keys under a prefix of an app-owned tree through the app bridge.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsBridgeScanRequest), Immutable]
public sealed record AppsBridgeScanRequest
{
    /// <summary>The app, install revision and app-local tree.</summary>
    [Id(0)] public required AppBridgeTarget Target { get; init; }
    /// <summary>The key prefix; empty scans the whole tree.</summary>
    [Id(1)] public required string Prefix { get; init; }
    /// <summary>The requested page size.</summary>
    [Id(2)] public int PageSize { get; init; }
    /// <summary>The opaque continuation from the previous page, or null to start.</summary>
    [Id(3)] public string? Continuation { get; init; }
}
