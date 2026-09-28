namespace Orleans.Lattice.Api.Apps;

/// <summary>A request for one page of the apps the configured sources make available.</summary>
[GenerateSerializer, Alias(ApiAppsTypeAliases.AvailableAppQuery), Immutable]
public sealed record AvailableAppQuery
{
    /// <summary>The page size used when a query does not set one.</summary>
    public const int DefaultPageSize = 50;
    /// <summary>The largest page size an implementation serves; larger requests are clamped to it.</summary>
    public const int MaxPageSize = 200;

    /// <summary>The key of the one source to list, or null to list every source.</summary>
    [Id(0)] public string? SourceKey { get; init; }
    /// <summary>An optional text filter, honoured only by sources that support search.</summary>
    [Id(1)] public string? Text { get; init; }
    /// <summary>Which apps to return relative to the active tenant's installations.</summary>
    [Id(2)] public AvailableAppFilter Filter { get; init; } = AvailableAppFilter.All;
    /// <summary>The requested page size; implementations clamp it to [1, <see cref="MaxPageSize"/>].</summary>
    [Id(3)] public int PageSize { get; init; } = DefaultPageSize;
    /// <summary>The opaque continuation from the previous page, or null to start.</summary>
    [Id(4)] public string? Continuation { get; init; }
}
