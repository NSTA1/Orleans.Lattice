namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// Where the wall-clock time of one ingest slice went, split by stage.
/// <para>
/// <b>Why this exists.</b> A build slice is bounded by
/// <see cref="DurableVectorIndexOptions.IngestSliceBudget"/> and by
/// <see cref="DurableVectorIndexOptions.IngestBatchSize"/>, and the counters
/// already published say how many vectors a slice moved and whether it was
/// deadlined. None of them says WHERE the slice spent its time, so a slice that
/// yields 2% of its batch cap is indistinguishable from one bound by a slow
/// source, by a slow key assignment, or by the index itself. That attribution
/// previously had to be made by reading the source rather than from telemetry.
/// </para>
/// <para>
/// Durations are accumulated across the items of a single slice, so they sum to
/// slightly less than the slice's elapsed time - the remainder is the loop's own
/// bookkeeping and is deliberately not apportioned.
/// </para>
/// </summary>
/// <param name="SourceWait">Time awaiting the source enumerator for items.</param>
/// <param name="KeyAssign">Time assigning identifiers to index keys, reservations included.</param>
/// <param name="IndexUpsert">Time inserting vectors into the in-memory index.</param>
/// <param name="KeyFlush">Time making the slice's buffered key-map records durable.</param>
/// <param name="Consumed">How many items the slice consumed.</param>
public readonly record struct VectorIndexBuildSliceTimings(
    TimeSpan SourceWait,
    TimeSpan KeyAssign,
    TimeSpan IndexUpsert,
    TimeSpan KeyFlush,
    int Consumed);
