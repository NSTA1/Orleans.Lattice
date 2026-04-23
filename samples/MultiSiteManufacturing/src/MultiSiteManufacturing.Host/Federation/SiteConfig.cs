using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Federation;

/// <summary>
/// Chaos configuration for a single process site. All defaults are
/// "nominal" — no pause, no delay, no reorder — so a freshly activated
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
    /// Models cross-site out-of-order arrival (plan §4.3 Tier 3).
    /// </summary>
    [Id(2)] public bool ReorderEnabled { get; init; }

    /// <summary>Number of facts buffered before a reorder flush fires.</summary>
    public const int ReorderWindowSize = 4;
}

/// <summary>
/// Observable snapshot of a site's chaos configuration plus its live
/// counters. Published by <see cref="ISiteRegistryGrain"/> to the gRPC
/// <c>WatchSites</c> feed and the Blazor chaos fly-out.
/// </summary>
[GenerateSerializer, Immutable]
public readonly record struct SiteState
{
    /// <summary>Site identity.</summary>
    [Id(0)] public ProcessSite Site { get; init; }

    /// <summary>Current chaos configuration.</summary>
    [Id(1)] public SiteConfig Config { get; init; }

    /// <summary>Number of facts currently held in the grain's pending queue.</summary>
    [Id(2)] public int PendingCount { get; init; }

    /// <summary>Total number of facts admitted (forwarded or held) by the grain since it activated.</summary>
    [Id(3)] public long AdmittedCount { get; init; }
}

/// <summary>
/// Verdict from <see cref="IProcessSiteGrain.AdmitAsync"/> describing how
/// the router should handle the fact.
/// </summary>
[GenerateSerializer, Immutable]
public readonly record struct SiteAdmission
{
    /// <summary>Pass-through admission: forward immediately, no delay.</summary>
    public static SiteAdmission Pass => new() { Forward = true, DelayMs = 0 };

    /// <summary>Hold admission: the grain has enqueued the fact; do not forward now.</summary>
    public static SiteAdmission Hold => new() { Forward = false, DelayMs = 0 };

    /// <summary>Delayed pass-through: forward after waiting <see cref="DelayMs"/>.</summary>
    public static SiteAdmission Delayed(int delayMs) => new() { Forward = true, DelayMs = delayMs };

    /// <summary>
    /// Hold admission that also returns a shuffled batch of previously
    /// buffered facts for the router to release. Produced when a reorder
    /// window fills (plan §4.3 Tier 3).
    /// </summary>
    public static SiteAdmission ReorderFlush(IReadOnlyList<Fact> drained) =>
        new() { Forward = false, DelayMs = 0, ShuffledDrain = drained };

    /// <summary>True if the router should forward the fact to the backends.</summary>
    [Id(0)] public bool Forward { get; init; }

    /// <summary>Artificial latency to apply before forwarding (ignored when <see cref="Forward"/> is false).</summary>
    [Id(1)] public int DelayMs { get; init; }

    /// <summary>
    /// When non-null, the router must release these facts to every
    /// backend in the order given. Only populated on a reorder flush;
    /// <see cref="Pass"/> / <see cref="Hold"/> / <see cref="Delayed"/>
    /// all leave this null.
    /// </summary>
    [Id(2)] public IReadOnlyList<Fact>? ShuffledDrain { get; init; }
}

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

/// <summary>Canned chaos presets exposed in the UI fly-out (plan §7.2).</summary>
public enum ChaosPreset
{
    /// <summary>Resets every site to <see cref="SiteConfig.Nominal"/>.</summary>
    ClearAll,

    /// <summary>Pauses Stuttgart CMM Lab + Toulouse NDT Lab and adds a 4s delay.</summary>
    TransoceanicBackhaulOutage,

    /// <summary>Adds an 8s delay at Nagoya Heat Treatment.</summary>
    CustomsHold,

    /// <summary>Pauses Cincinnati MRB entirely.</summary>
    MrbWeekend,

    /// <summary>
    /// Applies a 10% transient-failure rate and 50–250 ms jitter to the
    /// <c>lattice</c> backend only. Drives baseline-vs-lattice divergence
    /// without touching site chaos (plan §7.2).
    /// </summary>
    LatticeStorageFlakes,

    /// <summary>
    /// Opens a 300 ms reorder window on the <c>baseline</c> backend
    /// only — incoming writes are buffered and flushed in shuffled
    /// order. Combined with the <b>Race</b> row action, this is the
    /// canonical way to force baseline into an arrival-order fold that
    /// disagrees with lattice's HLC-order fold, producing a red row in
    /// the divergence feed.
    /// </summary>
    BaselineReorderStorm,
}
