namespace Orleans.Lattice.GrainIndex.Tests.Backfill;

/// <summary>
/// An <see cref="IGrainIndexBackfillActivator"/> that records the keys it was
/// asked to onboard instead of addressing a grain, so the crawl's decisions are
/// observable without a cluster.
/// </summary>
internal sealed class RecordingBackfillActivator : IGrainIndexBackfillActivator
{
    /// <summary>The keys onboarded, in the order the crawl visited them.</summary>
    internal List<string> Activated { get; } = [];

    /// <summary>Keys whose activation throws, so a partial-failure pass is testable.</summary>
    internal HashSet<string> Failing { get; } = new(StringComparer.Ordinal);

    /// <summary>The definition the last call was given.</summary>
    internal IGrainIndexDefinition? LastDefinition { get; private set; }

    /// <inheritdoc />
    public Task ActivateAsync(
        IGrainIndexDefinition definition,
        string grainKey,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(grainKey);

        LastDefinition = definition;

        if (Failing.Contains(grainKey))
            return Task.FromException(new InvalidOperationException($"grain '{grainKey}' is unreachable"));

        Activated.Add(grainKey);
        return Task.CompletedTask;
    }
}
