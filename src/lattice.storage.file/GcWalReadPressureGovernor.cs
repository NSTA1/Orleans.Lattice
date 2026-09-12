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
    /// <para>
    /// One mebibyte, and the size is a deliberate trade rather than the most
    /// conservative value available. Narrowing harder is not obviously safer,
    /// because a leaf does not become trimmable by reading the log - it
    /// becomes trimmable by registering a cursor, and it registers only once
    /// it has applied an entry inside its own key range. A page carrying few
    /// entries is a page likely to carry none of this leaf's, so every
    /// halving of the floor multiplies the number of pages a leaf must read
    /// before its first registration. The floor therefore trades directly
    /// against time to recovery, and the two costs are asymmetric: an
    /// over-wide read fails, narrows, and retries inside one activation,
    /// whereas an over-narrow read costs whole extra activations. The floor
    /// should be the largest value whose failure is cheap, not the largest
    /// value that never fails.
    /// </para>
    /// <para>
    /// What makes a megabyte cheap is that this is a budget, not an
    /// allocation size. The page is staged as pooled fixed-size chunks, so
    /// the largest single contiguous request stays at the chunk size whatever
    /// this floor is set to. Raising the floor raises the number of pooled
    /// chunks in flight, never the size of the block the allocator must find
    /// - which is the quantity that actually fails on a fragmented,
    /// nearly-full heap. The concurrency arithmetic that made the configured
    /// ceiling unaffordable does not bite here either: a ceiling paid once
    /// per in-flight read, with replay permits multiplied by partitions
    /// putting on the order of a hundred reads in flight, asks for gigabytes
    /// at 16 MiB and tens of mebibytes at this floor.
    /// </para>
    /// <para>
    /// A megabyte is also the natural stopping point rather than an arbitrary
    /// one. At representative entry sizes it admits about as many entries as
    /// the replay loop's own per-slice entry cap allows, so beyond this the
    /// entry cap binds first and further widening buys nothing.
    /// </para>
    /// </summary>
    internal const long MinimumBudgetBytes = 1024L * 1024L;

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
