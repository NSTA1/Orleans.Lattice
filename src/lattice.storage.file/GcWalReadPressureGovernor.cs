namespace Orleans.Lattice.Storage.File;

/// <summary>
/// The production <see cref="IWalReadPressureGovernor"/>: narrows the read
/// budget from the garbage collector's own view of heap occupancy.
/// <para>
/// <b>Why occupancy and not headroom.</b> A budget proportional to remaining
/// headroom looks prudent and is not: at 94.7% of a 12 GiB cap the headroom
/// is still ~640 MiB, so any sane fraction of it is megabytes, and hundreds
/// of leaves replay concurrently. Concurrency is the multiplier that defeats
/// a headroom-proportional bound, so the response has to be sharply
/// nonlinear in the <i>fraction</i> occupied: unchanged while there is room
/// to work in, and collapsed to a floor by the time the heap is the thing
/// that is failing.
/// </para>
/// <para>
/// <b>Why a floor and not zero.</b> Refusing to read at all would be stable
/// but dead: a replay that reads nothing never advances a checkpoint, and a
/// checkpoint that never advances is exactly the pin that froze the log in
/// the first place. The floor is chosen so that a page of it, plus the
/// chunked payload buffers used to decode it, is affordable out of the
/// headroom that remains even at the observed 94.7% - which is what lets the
/// loop drain rather than stall.
/// </para>
/// </summary>
internal sealed class GcWalReadPressureGovernor : IWalReadPressureGovernor
{
    /// <summary>Shared, stateless instance.</summary>
    internal static readonly GcWalReadPressureGovernor Instance = new();

    /// <summary>
    /// Occupancy at or below which the configured ceiling is used unchanged.
    /// Below this the heap is not the constraint and narrowing would only
    /// slow recovery down.
    /// </summary>
    internal const double RelaxedOccupancy = 0.70d;

    /// <summary>
    /// Occupancy at or above which <see cref="MinimumBudgetBytes"/> is used.
    /// Set below the runtime's own high-memory-load threshold so the budget
    /// has already collapsed by the time allocations begin to fail, rather
    /// than collapsing in response to the first failure.
    /// </summary>
    internal const double CriticalOccupancy = 0.90d;

    /// <summary>
    /// The floor the budget collapses to at <see cref="CriticalOccupancy"/>.
    /// Sixty-four kibibytes: large enough that a replay still makes real
    /// forward progress per page, small enough that hundreds of concurrent
    /// leaf activations fit in the headroom left at critical occupancy.
    /// </summary>
    internal const long MinimumBudgetBytes = 64L * 1024L;

    /// <summary>
    /// The pure, allocation-free core of the narrowing rule, separated from
    /// the GC so it can be asserted exactly rather than approximately.
    /// </summary>
    /// <param name="configuredMaxBytes">The configured ceiling.</param>
    /// <param name="totalAvailableBytes">Total bytes available to the
    /// process, as the GC sees it. Zero or negative means "no signal", and
    /// the configured ceiling is returned unchanged.</param>
    /// <param name="memoryLoadBytes">Bytes currently in use.</param>
    internal static long NarrowCore(long configuredMaxBytes, long totalAvailableBytes, long memoryLoadBytes)
    {
        if (configuredMaxBytes < 1L)
        {
            configuredMaxBytes = 1L;
        }

        // No usable signal: never invent pressure. A host with no container
        // limit and no heap hard limit reports zero here, and silently
        // shrinking its reads would be a pure regression.
        if (totalAvailableBytes <= 0L || memoryLoadBytes < 0L)
        {
            return configuredMaxBytes;
        }

        var occupancy = (double)memoryLoadBytes / totalAvailableBytes;
        if (occupancy <= RelaxedOccupancy)
        {
            return configuredMaxBytes;
        }

        var floor = MinimumBudgetBytes < configuredMaxBytes ? MinimumBudgetBytes : configuredMaxBytes;
        if (occupancy >= CriticalOccupancy)
        {
            return floor;
        }

        var travelled = (occupancy - RelaxedOccupancy) / (CriticalOccupancy - RelaxedOccupancy);
        var narrowed = configuredMaxBytes - (long)(travelled * (configuredMaxBytes - floor));
        if (narrowed < floor)
        {
            narrowed = floor;
        }

        return narrowed > configuredMaxBytes ? configuredMaxBytes : narrowed;
    }

    /// <inheritdoc />
    public long NarrowBudget(long configuredMaxBytes)
    {
        // GCMemoryInfo is a struct read from the last collection, so this is
        // a field read and not a collection: the one thing the budget must
        // not do is allocate in order to decide how little to allocate.
        var info = GC.GetGCMemoryInfo();
        return NarrowCore(configuredMaxBytes, info.TotalAvailableMemoryBytes, info.MemoryLoadBytes);
    }

    /// <inheritdoc />
    public byte[] Allocate(int byteCount) => new byte[byteCount];
}
