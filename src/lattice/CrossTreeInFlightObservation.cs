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
    {
        ArgumentOutOfRangeException.ThrowIfNegative(inFlightCount);
        ArgumentOutOfRangeException.ThrowIfNegative(registrationEpoch);
        InFlightCount = inFlightCount;
        RegistrationEpoch = registrationEpoch;
    }

    /// <summary>
    /// The number of cross-tree atomic sagas still delegating a decision on this
    /// tree at observation time. Zero once every cross-tree saga touching the
    /// tree has drained to a terminal verdict.
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
}
