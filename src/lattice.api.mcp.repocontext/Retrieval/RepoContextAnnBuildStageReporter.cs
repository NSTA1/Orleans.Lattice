using System.Diagnostics.Metrics;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Publishes where an approximate-index build slice spent its time, by stage.
/// <para>
/// <b>Why this exists.</b> The build already reports how many slices ran, whether
/// each advanced, and how many vectors the index holds. None of that says WHERE a
/// slice's time went, so a slice yielding 2% of its batch cap looked identical
/// whether it was bound by a slow source, by key assignment, or by the index. That
/// attribution had to be made by reading source rather than from telemetry, which
/// is the gap this closes: the retrieval path has had
/// <c>repocontext.retrieval.stage.duration</c> for exactly this purpose and the
/// build path had no equivalent.
/// </para>
/// <para>
/// <b>It lives here and not in <c>Orleans.Lattice.Vector</c>.</b> That package
/// declares no meter and no instruments at all, and adding one would put a metrics
/// class in a library whose consumers own their own meters. It exposes
/// <see cref="IVectorIndexBuildObserver"/> instead, and this type adapts it onto
/// the meter the rest of the repocontext surface already publishes on.
/// </para>
/// </summary>
internal sealed class RepoContextAnnBuildStageReporter : IVectorIndexBuildObserver, IDisposable
{
    /// <summary>The histogram of per-stage build-slice seconds.</summary>
    internal const string StageDurationInstrumentName = "repocontext.ann.build.stage.duration";

    /// <summary>The counter of source items consumed by build slices.</summary>
    internal const string SliceItemsInstrumentName = "repocontext.ann.build.slice.items";

    /// <summary>The tag key carrying the stage partition.</summary>
    internal const string StageTagKey = "stage";

    /// <summary>Awaiting the source enumerator for the next item.</summary>
    internal const string StageSourceWait = "source_wait";

    /// <summary>Assigning identifiers to index keys, durable reservations included.</summary>
    internal const string StageKeyAssign = "key_assign";

    /// <summary>Inserting vectors into the in-memory index.</summary>
    internal const string StageIndexUpsert = "index_upsert";

    /// <summary>Making the slice's buffered key-map records durable.</summary>
    internal const string StageKeyFlush = "key_flush";

    // Declared above the instruments it constructs, and both instruments are built
    // from this field, so reordering throws at type-initialisation rather than
    // publishing an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Histogram<double> _stageDuration;
    private readonly Counter<long> _sliceItems;

    /// <summary>Creates the reporter and publishes its instruments.</summary>
    public RepoContextAnnBuildStageReporter()
    {
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _stageDuration = _meter.CreateHistogram<double>(
            StageDurationInstrumentName,
            unit: "s",
            description:
                "Seconds one stage of an approximate-index build slice took, accumulated across the items of "
                + "that slice and tagged by stage: 'source_wait' - awaiting the source enumerator, which "
                + "streams over grain calls; 'key_assign' - mapping identifiers to index keys, including a "
                + "durable block reservation when one is due; 'index_upsert' - the in-memory insert; and "
                + "'key_flush' - the one batched write that makes the slice's key-map records durable. The "
                + "four sum to slightly less than the slice's elapsed time, the remainder being loop "
                + "bookkeeping that is deliberately not apportioned. Deliberately NOT zero-primed: priming a "
                + "histogram fabricates a zero-valued sample, which reads as a real measurement of an "
                + "instantaneous stage and destroys the distribution the instrument exists to report.");
        _sliceItems = _meter.CreateCounter<long>(
            SliceItemsInstrumentName,
            unit: "{item}",
            description:
                "Source items consumed by approximate-index build slices. The denominator that makes the "
                + "stage histogram above interpretable, and the only way to obtain vectors-per-slice from "
                + "metrics alone: dividing the cumulative 'ann.vectorsIndexed' by the process-scoped slice "
                + "counter is INVALID, because the former is inherited across a restart whenever the index "
                + "was restored from durable state while the latter resets at the deploy boundary.");
        _sliceItems.Add(0, LatticeTenantLabel.Platform);
    }

    /// <summary>
    /// The meter these instruments are published on. Exposed so a fixture can pass
    /// it to <c>MeterListening.StartForMeter</c>, which takes the meter as a
    /// parameter and so cannot reintroduce the re-entrant publication hazard.
    /// </summary>
    internal Meter Meter => _meter;

    /// <inheritdoc />
    public void OnSliceCompleted(in VectorIndexBuildSliceTimings timings)
    {
        Record(StageSourceWait, timings.SourceWait);
        Record(StageKeyAssign, timings.KeyAssign);
        Record(StageIndexUpsert, timings.IndexUpsert);
        Record(StageKeyFlush, timings.KeyFlush);

        if (timings.Consumed > 0)
        {
            _sliceItems.Add(timings.Consumed, LatticeTenantLabel.Platform);
        }
    }

    // One emission site per arm, each naming its tag constant literally, rather
    // than a computed tag: the priming gate resolves an instrument's tag domain
    // from its emission sites, and a computed tag leaves that domain ambiguous.
    // The stage is a parameter here because this instrument is deliberately
    // unprimed, so there is no priming to render ineffective.
    private void Record(string stage, TimeSpan elapsed)
    {
        // Clamped rather than dropped. A negative reading can only come from a
        // clock irregularity, and once summed it is indistinguishable from a
        // genuine measurement, so it must not reach the sum.
        var seconds = elapsed > TimeSpan.Zero ? elapsed.TotalSeconds : 0d;
        _stageDuration.Record(
            seconds,
            new KeyValuePair<string, object?>(StageTagKey, stage),
            LatticeTenantLabel.Platform);
    }

    /// <inheritdoc />
    public void Dispose() => _meter.Dispose();
}
