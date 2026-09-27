namespace Orleans.Lattice.Api.Apps;

/// <summary>A requested change-feed subscription using an app-local tree name, never a composed physical id.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AppSubscriptionDescriptor), Immutable]
public sealed record AppSubscriptionDescriptor
{
    /// <summary>The declared subscription name.</summary>
    [Id(0)] public required string Name { get; init; }
    /// <summary>The source app's local tree name.</summary>
    [Id(1)] public required string Tree { get; init; }
    /// <summary>The source app slug; null means the described app.</summary>
    [Id(2)] public string? App { get; init; }
    /// <summary>An optional key prefix limiting observed changes.</summary>
    [Id(3)] public string? KeyPrefix { get; init; }
}
