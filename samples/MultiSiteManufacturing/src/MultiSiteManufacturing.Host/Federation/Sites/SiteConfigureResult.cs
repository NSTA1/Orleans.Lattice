using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Result of <see cref="IProcessSiteGrain.ConfigureAsync"/>: the new
/// effective config plus any facts the grain released in response (for
/// instance, everything queued during a pause that the update cleared).
/// </summary>
[GenerateSerializer, Immutable]
public sealed record SiteConfigureResult
{
    /// <summary>Effective configuration after the update.</summary>
    [Id(0)] public required SiteConfig Config { get; init; }

    /// <summary>Facts drained from the grain's pending queue that the caller must now fan out.</summary>
    [Id(1)] public required IReadOnlyList<Fact> Drained { get; init; }
}
