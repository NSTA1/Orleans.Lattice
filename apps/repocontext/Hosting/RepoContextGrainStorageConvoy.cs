namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Counts the grain-storage operations in flight against the SQLite grain store,
/// so a lock failure can be read against the width of the write convoy it
/// happened inside.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2431. SQLite serialises writers, so the hypothesis for the lock storm
/// is a convoy of writers wide enough that the ones at the back exhaust the busy
/// window. That hypothesis names a quantity - how many writers were queued when
/// the lock failed - which nothing measured. This type is that measurement: every
/// write and clear enters before it reaches the provider and exits when the
/// provider returns, whatever the outcome.
/// </para>
/// <para>
/// Writes and clears are counted together because both need the database write
/// lock and so both queue in the same convoy. Reads are counted separately: in
/// <c>WAL</c> journal mode a reader does not take the write lock, so folding reads
/// in would widen the apparent convoy with operations that are not in it.
/// </para>
/// </remarks>
public sealed class RepoContextGrainStorageConvoy
{
    private long _writesInFlight;
    private long _readsInFlight;
    private long _peakWritesInFlight;

    /// <summary>Writes and clears currently in flight.</summary>
    public long WritesInFlight => Interlocked.Read(ref _writesInFlight);

    /// <summary>Reads currently in flight.</summary>
    public long ReadsInFlight => Interlocked.Read(ref _readsInFlight);

    /// <summary>The most writes and clears ever in flight at once since this process started.</summary>
    public long PeakWritesInFlight => Interlocked.Read(ref _peakWritesInFlight);

    /// <summary>
    /// Records that an operation has started and returns the width of its convoy,
    /// counting itself.
    /// </summary>
    /// <param name="operation">The operation starting.</param>
    /// <returns>
    /// For a write or clear, the writes and clears in flight including this one; for
    /// a read, the reads in flight including this one.
    /// </returns>
    public long Enter(RepoContextGrainStorageOperation operation)
    {
        if (operation == RepoContextGrainStorageOperation.Read)
        {
            return Interlocked.Increment(ref _readsInFlight);
        }

        var width = Interlocked.Increment(ref _writesInFlight);
        RaisePeak(width);
        return width;
    }

    /// <summary>Records that an operation previously passed to <see cref="Enter"/> has finished.</summary>
    /// <param name="operation">The operation finishing.</param>
    public void Exit(RepoContextGrainStorageOperation operation)
    {
        if (operation == RepoContextGrainStorageOperation.Read)
        {
            Interlocked.Decrement(ref _readsInFlight);
        }
        else
        {
            Interlocked.Decrement(ref _writesInFlight);
        }
    }

    private void RaisePeak(long width)
    {
        var peak = Interlocked.Read(ref _peakWritesInFlight);
        while (width > peak)
        {
            var observed = Interlocked.CompareExchange(ref _peakWritesInFlight, width, peak);
            if (observed == peak)
            {
                return;
            }

            peak = observed;
        }
    }
}
