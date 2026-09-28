namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>Reports whether an app bridge delete removed a live value.</summary>
[GenerateSerializer, Alias(GrpcAppsTypeAliases.AppsBridgeDeleteResponse), Immutable]
public sealed record AppsBridgeDeleteResponse
{
    /// <summary>True when a live value was deleted; false when the key had none.</summary>
    [Id(0)] public bool Deleted { get; init; }
}
