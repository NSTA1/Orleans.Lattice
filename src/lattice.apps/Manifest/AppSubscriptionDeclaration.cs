namespace Orleans.Lattice.Apps;

/// <summary>A named change-feed subscription whose runtime handler is supplied separately by the host.</summary>
[GenerateSerializer, Alias(AppsTypeAliases.AppSubscriptionDeclaration), Immutable]
public sealed record AppSubscriptionDeclaration
{
    /// <summary>Unique subscription name used to resolve a host-registered handler.</summary>
    [Id(0)] public required string Name { get; init; }

    /// <summary>Source app's local tree name.</summary>
    [Id(1)] public required string Tree { get; init; }

    /// <summary>Source app, or null for this app; cross-app observation requires consent.</summary>
    [Id(2)] public AppSlug? App { get; init; }

    /// <summary>Optional non-empty key prefix limiting observed changes.</summary>
    [Id(3)] public string? KeyPrefix { get; init; }
}
