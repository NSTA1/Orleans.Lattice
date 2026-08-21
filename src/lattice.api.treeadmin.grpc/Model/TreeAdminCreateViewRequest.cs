namespace Orleans.Lattice.Api.TreeAdmin.Grpc;

/// <summary>
/// Wire request for creating a provider-backed runtime materialised view. The
/// payload is opaque to the transport and is never echoed in a response.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTreeAdminTypeAliases.TreeAdminCreateViewRequest)]
[Immutable]
public sealed record TreeAdminCreateViewRequest
{
    /// <summary>The logical materialised-view name.</summary>
    [Id(0)] public required string ViewName { get; init; }

    /// <summary>The directly writable source tree id.</summary>
    [Id(1)] public required string SourceTreeId { get; init; }

    /// <summary>The host-registered runtime projection provider key.</summary>
    [Id(2)] public required string ProviderKey { get; init; }

    /// <summary>The bounded opaque provider payload.</summary>
    [Id(3)] public byte[] Payload { get; init; } = [];
}
