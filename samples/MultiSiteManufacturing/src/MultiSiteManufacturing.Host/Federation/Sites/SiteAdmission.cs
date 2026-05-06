using MultiSiteManufacturing.Host.Domain;

namespace MultiSiteManufacturing.Host.Federation;

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
    /// window fills.
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
