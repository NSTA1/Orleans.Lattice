namespace Orleans.Lattice;

/// <summary>
/// A point-in-time observation of a single tree's cross-tree atomic-write
/// delegation state, returned by
/// <see cref="Orleans.Lattice.BPlusTree.ITxRegistryGrain.ObserveCrossTreeInFlightAsync"/>.
/// Consumed by the cross-tree-consistent backup fence: the fence drains every
/// in-flight cross-tree saga touching the backup set before capturing, then
/// re-observes to confirm no new cross-tree saga registered during the capture
/// window.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.CrossTreeInFlightObservation)]
public readonly record struct CrossTreeInFlightObservation
{
    /// <summary>Initializes a new <see cref="CrossTreeInFlightObservation"/>.</summary>
    /// <param name="inFlightCount">
    /// The number of cross-tree atomic sagas that still delegate their
    /// commit/abort decision on this tree (their coordinator has not yet reached
    /// a terminal verdict). Must not be negative.
    /// </param>
    /// <param name="registrationEpoch">
    /// A monotonically non-decreasing counter bumped once each time a distinct
    /// cross-tree saga first registers a delegation on this tree. Comparing the
    /// epoch before and after a capture window detects a cross-tree saga that
    /// both registered and completed inside the window. Must not be negative.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="inFlightCount"/> or <paramref name="registrationEpoch"/> is negative.</exception>
    public CrossTreeInFlightObservation(int inFlightCount, long registrationEpoch)
        : this(inFlightCount, registrationEpoch, unresolvableCount: 0)
    {
    }

    /// <summary>
    /// Initializes a new <see cref="CrossTreeInFlightObservation"/> that also
    /// reports how much of <paramref name="inFlightCount"/> is unreachable
    /// rather than pending.
    /// </summary>
    /// <param name="inFlightCount">
    /// The number of cross-tree atomic sagas that still delegate their
    /// commit/abort decision on this tree. Must not be negative.
    /// </param>
    /// <param name="registrationEpoch">
    /// A monotonically non-decreasing counter bumped once each time a distinct
    /// cross-tree saga first registers a delegation on this tree. Must not be
    /// negative.
    /// </param>
    /// <param name="unresolvableCount">
    /// The subset of <paramref name="inFlightCount"/> whose coordinator could
    /// not be reached during this observation. Must not be negative.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">Any argument is negative.</exception>
    public CrossTreeInFlightObservation(int inFlightCount, long registrationEpoch, int unresolvableCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(inFlightCount);
        ArgumentOutOfRangeException.ThrowIfNegative(registrationEpoch);
        ArgumentOutOfRangeException.ThrowIfNegative(unresolvableCount);
        InFlightCount = inFlightCount;
        RegistrationEpoch = registrationEpoch;
        UnresolvableCount = unresolvableCount;
    }

    /// <summary>
    /// The number of cross-tree atomic sagas still delegating a decision on this
    /// tree at observation time. Zero once every cross-tree saga touching the
    /// tree has drained to a terminal verdict.
    /// <para>
    /// This count includes any saga whose coordinator could not be reached, which
    /// is the correct conservative accounting - an unreachable coordinator is not
    /// evidence of a decision - but it means a non-zero count is not by itself
    /// evidence that sagas are progressing. Read <see cref="UnresolvableCount"/>
    /// alongside it to tell the two apart.
    /// </para>
    /// </summary>
    [Id(0)]
    public int InFlightCount { get; init; }

    /// <summary>
    /// The monotonically non-decreasing count of distinct cross-tree sagas that
    /// have ever registered a delegation on this tree. A change across a capture
    /// window signals that a new cross-tree saga touched the tree during it.
    /// </summary>
    [Id(1)]
    public long RegistrationEpoch { get; init; }

    /// <summary>
    /// How many of the <see cref="InFlightCount"/> sagas were counted because
    /// their coordinator could not be reached, rather than because the
    /// coordinator answered that it is still preparing. A persistently non-zero
    /// value is a connectivity fault, not pipelining: the observation cannot
    /// distinguish a decided saga from an undecided one for these entries, so a
    /// fence that waits for the in-flight count to reach zero will never
    /// converge until reachability is restored.
    /// <para>
    /// Zero on an observation produced by a node that predates this field, which
    /// is indistinguishable from a genuinely reachable observation. Treat it as
    /// a signal to act on when non-zero rather than as proof of health when zero.
    /// </para>
    /// </summary>
    [Id(2)]
    public int UnresolvableCount { get; init; }
}
