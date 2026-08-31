using Orleans.Concurrency;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// How far a grain index's background backfill has progressed, in the terms an
/// operator asks about: how much has been done, how much there is to do, and
/// where the crawl currently sits.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="Processed"/> is always available, because the crawl counts what it
/// takes from the key source. <see cref="Total"/> and
/// <see cref="PercentComplete"/> are best effort: they are populated only when
/// the application's <see cref="IGrainKeySource"/> can bound its population
/// through
/// <see cref="IGrainKeySource.TryGetApproximateCountAsync(CancellationToken)"/>.
/// A source that cannot is fully supported - the progress is then a processed
/// count rather than a percentage, which is honest rather than a fabricated
/// denominator.
/// </para>
/// <para>
/// A completed crawl reports 100 percent whether or not a bound was available:
/// having exhausted the key source is the one progress fact that never needs an
/// estimate.
/// </para>
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexProgress)]
public sealed class GrainIndexProgress
{
    /// <summary>Initialises a progress report.</summary>
    /// <param name="processed">Keys the crawl has taken from its key source.</param>
    /// <param name="total">The bounded population size, or <c>null</c> when unknown.</param>
    /// <param name="percentComplete">The percentage covered, or <c>null</c> when unknown.</param>
    /// <param name="lastProcessedKey">The last key the crawl visited, or <c>null</c>.</param>
    /// <param name="lastError">Why the last pass failed, or <c>null</c>.</param>
    public GrainIndexProgress(
        long processed,
        long? total,
        double? percentComplete,
        string? lastProcessedKey,
        string? lastError)
    {
        Processed = processed;
        Total = total;
        PercentComplete = percentComplete;
        LastProcessedKey = lastProcessedKey;
        LastError = lastError;
    }

    /// <summary>The number of keys the crawl has taken from its key source.</summary>
    [Id(0)]
    public long Processed { get; }

    /// <summary>
    /// The best-effort size of the population the crawl has to cover, or
    /// <c>null</c> when the key source cannot bound it.
    /// </summary>
    [Id(1)]
    public long? Total { get; }

    /// <summary>
    /// How far through its population the crawl has reached, from 0 to 100, or
    /// <c>null</c> when that is not knowable.
    /// </summary>
    [Id(2)]
    public double? PercentComplete { get; }

    /// <summary>
    /// The last key the crawl visited, or <c>null</c> when it has visited none.
    /// This is the crawl's resume point, so it is also where a paused crawl will
    /// pick up.
    /// </summary>
    [Id(3)]
    public string? LastProcessedKey { get; }

    /// <summary>
    /// Why the crawl's last pass failed, or <c>null</c>. Retained across a
    /// resume, so the reason a crawl stalled survives somebody restarting it.
    /// </summary>
    [Id(4)]
    public string? LastError { get; }

    /// <summary>
    /// The progress of a crawl that has never run: nothing processed, nothing
    /// known, nowhere reached.
    /// </summary>
    public static GrainIndexProgress None { get; } = new(0, null, null, null, null);
}
