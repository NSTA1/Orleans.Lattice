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
/// <b>Since issue 2585 the ceiling only throws when it caught the walk holding
/// nothing.</b> A page fill whose sortable rows had already accumulated banks
/// them as an ordinary short page (<c>HasMore = true</c>, no
/// <c>ResumeFromKey</c>) instead of faulting, so the work is not discarded and
/// the caller's next request starts past it. This exception therefore names the
/// strictly narrower case where the ceiling fired before any row was read, or
/// on a path (counts, deletes, diagnostics) whose result carries no
/// continuation to bank into. That is why a repeated stall used to be a
/// livelock - every attempt re-walked and re-discarded the same leaves - and no
/// longer is.
/// </para>
/// <para>
/// The typed slots carry the per-occurrence attribution that makes the
/// next occurrence self-diagnosing rather than a bare duration.
/// </para>
/// <para>
/// It derives from <see cref="System.TimeoutException"/> for backwards
/// compatibility, but that inheritance is a hazard rather than a convenience: a
/// broad <c>catch (TimeoutException)</c> that simply retries reproduces exactly
/// the livelock described above, because it discards the banked continuation
/// and re-walks the same leaves. This type therefore implements
/// <see cref="ILatticeDomainFault"/>, so a broad handler declines it with
/// <c>catch (TimeoutException ex) when (ex is not ILatticeDomainFault)</c>.
/// Catching this type by name, and resuming from the banked continuation,
/// remains the correct handling and is unaffected.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ScanPageStalled)]
public sealed class ScanPageStalledException : TimeoutException, ILatticeDomainFault
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

    /// <summary>
    /// The leaf whose read was still outstanding when the ceiling fired, as
    /// its grain identity (for example
    /// <c>bplusleaf/7b16d935344e4206bc6e0d161f52ff6b</c>), or
    /// <see langword="null"/> when no read was outstanding (the prologue and
    /// descent phases, and a ceiling that fired between two leaf reads).
    /// <para>
    /// <see cref="LeavesVisited"/> says <em>which</em> read stalled by
    /// position; this says which leaf that position was. Position alone cannot
    /// be joined to anything else recorded about that leaf, and a stall at
    /// zero leaves has several candidate causes that are only distinguishable
    /// by what the named leaf was doing - so without it a recurrence is
    /// attributable to a shard but not to a cause (issue 2278).
    /// </para>
    /// </summary>
    [Id(6)] public string? LeafInFlight { get; set; }

    /// <summary>
    /// How many <em>consecutive</em> ceiling fires this shard root has now seen
    /// that completed no leaf and named this same <see cref="LeafInFlight"/>,
    /// counting this one. Zero when the fire made progress, named no leaf, or
    /// named a different leaf from the previous fire (issue #3016).
    /// <para>
    /// <see cref="LeavesVisited"/> says a single attempt read nothing.
    /// <b>This says the attempts are not making each other any more likely to
    /// succeed</b>, which is the only quantity that separates a tree that is
    /// busy from one that cannot converge. Both present identically per
    /// attempt: the field exists because a caller reading one exception cannot
    /// otherwise tell a leaf replaying a long WAL window from cold - which
    /// recovers - from one that has failed the same read 307 times running,
    /// which does not.
    /// </para>
    /// </summary>
    [Id(7)] public int ConsecutiveZeroProgressStalls { get; set; }

    /// <summary>
    /// Whether <see cref="LeafInFlight"/> has been classified <b>unreadable</b>
    /// by this shard root: it has now missed the ceiling on enough consecutive
    /// zero-progress attempts that retrying it unchanged cannot be expected to
    /// behave differently, so the stranded-leaf recovery was applied before
    /// this exception was raised (issue #3016).
    /// <para>
    /// <see langword="false"/> is the ordinary reading for a stall and means
    /// only "not yet": a slow leaf is expected to recover, and the first fires
    /// against one are indistinguishable from the first fires against a wedged
    /// one. <see langword="true"/> is a statement about the <em>sequence</em>
    /// and is always actionable - it says a plain retry has already been tried
    /// and has already failed to differ.
    /// </para>
    /// </summary>
    [Id(8)] public bool LeafStranded { get; set; }

    /// <summary>
    /// How many times this shard root has applied the stranded-leaf recovery to
    /// <see cref="LeafInFlight"/>, counting this occurrence, across <b>every
    /// activation of the shard root</b> and not merely the current one
    /// (issue #3016). Zero when the fire did not reach the classification.
    /// <para>
    /// <see cref="ConsecutiveZeroProgressStalls"/> and
    /// <see cref="LeafStranded"/> together say that retrying <em>the read this
    /// activation is parked on</em> has been tried and does not differ, and the
    /// remedy for that is to stop waiting on it. This field answers the
    /// question that remedy raises and nothing else can: <b>did dropping it
    /// help?</b>
    /// </para>
    /// <para>
    /// One means the recovery has just been applied for the first time; the
    /// next attempt will issue a genuinely fresh read and may well succeed.
    /// <b>Greater than one means it already did that and the leaf still did not
    /// answer</b>, so the fault is inside the leaf activation rather than in the
    /// shard root's coalescing, and no number of further scan attempts will
    /// converge. That distinction cannot be drawn from within one activation,
    /// because a freshly activated shard root holds no coalesced reads and its
    /// eviction is a no-op - so its stall looks identical to a first-ever
    /// stall no matter how long the leaf has been unreadable.
    /// </para>
    /// </summary>
    [Id(9)] public int StrandedRecoveryApplications { get; set; }
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
