namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Chaos configuration for a single process site. All defaults are
/// "nominal" - no pause, no delay, no reorder - so a freshly activated
/// grain passes traffic through unchanged.
/// </summary>
[GenerateSerializer, Immutable]
public readonly record struct SiteConfig
{
    /// <summary>Nominal configuration: no chaos applied.</summary>
    public static SiteConfig Nominal => default;

    /// <summary>When true, incoming facts are queued in the grain and not forwarded.</summary>
    [Id(0)] public bool IsPaused { get; init; }

    /// <summary>Artificial latency added before a fact is forwarded to the backends.</summary>
    [Id(1)] public int DelayMs { get; init; }

    /// <summary>
    /// When true, the grain accumulates admitted facts in a small
    /// reorder buffer and flushes them in shuffled order once the
    /// buffer fills (window size <see cref="ReorderWindowSize"/>).
    /// Models cross-site out-of-order arrival.
    /// </summary>
    [Id(2)] public bool ReorderEnabled { get; init; }

    /// <summary>Number of facts buffered before a reorder flush fires.</summary>
    public const int ReorderWindowSize = 4;
}
