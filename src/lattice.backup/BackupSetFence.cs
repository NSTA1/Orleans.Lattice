namespace Orleans.Lattice.Backup;

/// <summary>
/// The causal fence a cross-tree-consistent backup set was captured behind. The
/// fence is selected after every in-flight cross-tree atomic saga touching the
/// set has drained to a terminal decision; the members are then captured one tree
/// at a time, and the attempt is kept only when no cross-tree saga registered on
/// the set during that capture window and none is in flight when it closes
/// (otherwise it is retried behind a fresh fence). No cross-tree batch therefore
/// straddles the capture: for each such batch either every member backup includes
/// it, or none does. The set is not a single point-in-time cut - each member
/// carries its own per-tree cut, taken in turn inside the window.
/// <para>
/// The fence carries the local wall-clock time it was selected at plus the
/// observed coordination cost - how many in-flight cross-tree sagas the fence
/// waited to drain and how long that wait took - so the set manifest is
/// self-describing about the coordination it paid for.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSetFence)]
[Immutable]
public sealed record BackupSetFence
{
    /// <summary>Initializes a new <see cref="BackupSetFence"/>.</summary>
    /// <param name="hlcTimestamp">The local UTC wall-clock time, in ticks, at which the fence was selected. Must not be negative.</param>
    /// <param name="drainedInFlightCount">The number of in-flight cross-tree sagas the fence waited to drain. Must not be negative.</param>
    /// <param name="drainWaitMilliseconds">The total wall-clock time, in milliseconds, spent waiting for in-flight cross-tree sagas to drain. Must not be negative.</param>
    /// <param name="attempts">The number of fence attempts made before a stable capture window was observed (at least 1). Must be positive.</param>
    /// <exception cref="ArgumentOutOfRangeException">Any argument is out of range.</exception>
    public BackupSetFence(
        long hlcTimestamp,
        int drainedInFlightCount,
        double drainWaitMilliseconds,
        int attempts)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(hlcTimestamp);
        ArgumentOutOfRangeException.ThrowIfNegative(drainedInFlightCount);
        ArgumentOutOfRangeException.ThrowIfNegative(drainWaitMilliseconds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(attempts);
        HlcTimestamp = hlcTimestamp;
        DrainedInFlightCount = drainedInFlightCount;
        DrainWaitMilliseconds = drainWaitMilliseconds;
        Attempts = attempts;
    }

    /// <summary>
    /// The local UTC wall-clock time, in ticks, at which the fence was selected: after
    /// the drain and before the members were captured. A descriptive marker read from
    /// the capturing silo's clock rather than a hybrid-logical-clock value; the member
    /// snapshots are taken after it, not at it.
    /// </summary>
    [Id(0)]
    public long HlcTimestamp { get; init; }

    /// <summary>The number of in-flight cross-tree sagas the fence waited to drain.</summary>
    [Id(1)]
    public int DrainedInFlightCount { get; init; }

    /// <summary>The total wall-clock time, in milliseconds, spent draining in-flight cross-tree sagas.</summary>
    [Id(2)]
    public double DrainWaitMilliseconds { get; init; }

    /// <summary>The number of fence attempts made before a stable capture window was observed.</summary>
    [Id(3)]
    public int Attempts { get; init; }
}
