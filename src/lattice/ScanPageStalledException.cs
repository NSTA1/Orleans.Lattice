using Orleans.Serialization.Cloning;

namespace Orleans.Lattice;

/// <summary>
/// Thrown by <c>ShardRootGrain</c> when a single range-scan page fill exceeds
/// the per-tree
/// <see cref="Orleans.Lattice.LatticeOptions.MaxScanPageStallDuration"/>
/// ceiling (default 30 seconds) measured end to end from the first statement of
/// the grain call.
/// <para>
/// <see cref="Orleans.Lattice.LatticeOptions.MaxScanPageDuration"/> is the
/// primary, cooperative bound, but it can only be sampled <em>between</em> leaf
/// reads, so it cannot bound a single await that is slow or never returns. When
/// that happens the page fill keeps holding its deliberately non-reentrant
/// shard root and every other request to that shard queues behind it - 576
/// seconds against a 5 second budget in the incident behind issue 2002. This
/// exception is what the hard ceiling raises instead: the call stops waiting,
/// the shard is released, and the queue drains.
/// </para>
/// <para>
/// <b>The exception is retriable.</b> A page fill is a pure read of a key
/// range (<see cref="Phase"/> names how far it got), so nothing is
/// half-applied; the caller re-issues the same request from its last
/// continuation token. The abandoned call's own eventual completion is
/// harmlessly unobserved - Orleans runs its stray continuation on the
/// activation's single-threaded scheduler, so it interleaves between turns
/// rather than racing them.
/// </para>
/// <para>
/// Derives from <see cref="System.TimeoutException"/> so existing catch
/// handlers that match on <see cref="System.TimeoutException"/> continue to
/// work; the typed slots carry the per-occurrence attribution that makes the
/// next occurrence self-diagnosing rather than a bare duration.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ScanPageStalled)]
public sealed class ScanPageStalledException : TimeoutException
{
    /// <summary>
    /// Initialises a new instance with no diagnostic context. Provided to
    /// satisfy the framework's exception construction contract; production
    /// throw sites use the message + inner-exception overload.
    /// </summary>
    public ScanPageStalledException() { }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message.
    /// </summary>
    public ScanPageStalledException(string message) : base(message) { }

    /// <summary>
    /// Initialises a new instance with the specified diagnostic message and
    /// wrapped inner exception (typically the underlying
    /// <see cref="System.OperationCanceledException"/> raised when the ceiling
    /// fired).
    /// </summary>
    public ScanPageStalledException(string message, Exception innerException)
        : base(message, innerException) { }

    /// <summary>
    /// The tree whose shard-root page fill was abandoned.
    /// </summary>
    [Id(0)] public string TreeId { get; set; } = string.Empty;

    /// <summary>
    /// The physical shard index whose page fill was abandoned.
    /// </summary>
    [Id(1)] public int ShardIndex { get; set; }

    /// <summary>
    /// The grain method that stalled, for example
    /// <c>GetSortedEntriesBatchAsync</c>.
    /// </summary>
    [Id(2)] public string Operation { get; set; } = string.Empty;

    /// <summary>
    /// How far the page fill had got when the ceiling fired: <c>prologue</c>
    /// (preparing the shard for the operation), <c>descent</c> (traversing to
    /// the start leaf), or <c>leaf-walk</c> (reading the leaf chain).
    /// <para>
    /// This is the field that makes a recurrence self-diagnosing.
    /// <c>MaxScanPageDuration</c> alone cannot distinguish "the prologue never
    /// returned" from "one leaf read never returned", and those have entirely
    /// different causes; <see cref="LeavesVisited"/> disambiguates further by
    /// naming which leaf read was in flight.
    /// </para>
    /// </summary>
    [Id(3)] public string Phase { get; set; } = string.Empty;

    /// <summary>
    /// Leaves the walk had completed when the ceiling fired. Zero in the
    /// prologue and descent phases; in the leaf-walk phase it identifies the
    /// in-flight leaf read as the next one after this count.
    /// </summary>
    [Id(4)] public int LeavesVisited { get; set; }

    /// <summary>
    /// The ceiling that fired, expressed in seconds for wire-format stability
    /// across hosts whose <see cref="System.TimeSpan"/> serialisation might
    /// differ.
    /// </summary>
    [Id(5)] public double TimeoutSeconds { get; set; }
}

/// <summary>
/// Same-silo deep-copier for <see cref="ScanPageStalledException"/>. Orleans deep-copies a grain result
/// across an in-process (co-located) boundary instead of serialising it, and the
/// generated copier for a <c>[GenerateSerializer]</c> exception deriving from a BCL
/// exception subclass requests a copier for that base type, which Orleans does not
/// provide - so a same-silo throw would fail with an opaque <c>KeyNotFoundException</c>
/// ("Could not find a base type copier for ...") and mask the real, actionable fault.
/// An exception is immutable once constructed, so returning the same instance is a
/// correct deep copy and keeps the typed exception intact (the cross-silo serialise
/// path is unaffected).
/// </summary>
[RegisterCopier]
internal sealed class ScanPageStalledExceptionCopier : IDeepCopier<ScanPageStalledException>
{
    /// <inheritdoc />
    public ScanPageStalledException DeepCopy(ScanPageStalledException input, CopyContext context) => input;
}
