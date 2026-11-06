using System.Diagnostics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The process-wide storage-pressure signal the WAL replay permit governor reads
/// (issue #5877): how long replay slice reads take, and how often the storage
/// provider had to retry a throttled or failed request.
/// </summary>
/// <remarks>
/// <para>
/// Both halves are monotonic, lock-free tallies, read by
/// <see cref="ReplayStoragePressureGovernor"/> as deltas between windows. Slice
/// latency is kept in fixed power-of-two millisecond buckets, so recording a read
/// is a few interlocked increments and allocates nothing.
/// </para>
/// <para>
/// Retries are reported by the storage provider itself, through
/// <see cref="NoteStorageRetry"/>, rather than read back off the
/// <c>orleans.lattice.provider.retry.attempts</c> counter: only a storage provider
/// calls it, so the signal is provider-scoped by construction, and no
/// <see cref="System.Diagnostics.Metrics.MeterListener"/> runs in production.
/// </para>
/// </remarks>
internal static class ReplayStoragePressure
{
    /// <summary>Latency buckets: bucket <c>i</c> holds reads under <c>2^i</c> ms; the last is unbounded.</summary>
    internal const int LatencyBuckets = 20;

    private static readonly long[] SliceLatency = new long[LatencyBuckets];
    private static long _sliceReads;
    private static long _throttleRetries;

    /// <summary>
    /// Records one completed replay slice read of <paramref name="elapsedTicks"/>
    /// <see cref="Stopwatch"/> ticks.
    /// </summary>
    internal static void RecordSliceRead(long elapsedTicks)
    {
        var ms = elapsedTicks <= 0 ? 0 : elapsedTicks * 1000 / Stopwatch.Frequency;
        var bucket = ms <= 0 ? 0 : Math.Min(LatencyBuckets - 1, 64 - long.LeadingZeroCount(ms));
        Interlocked.Increment(ref SliceLatency[bucket]);
        Interlocked.Increment(ref _sliceReads);
    }

    /// <summary>
    /// Reports one storage request retry the provider made, with the HTTP status
    /// that caused it (0 for a transport failure). Only throttling and
    /// server-side unavailability count as pressure: 408, 429, 500, 502, 503 and
    /// 504. Counts every retry the provider makes on this process, not only
    /// those on replay reads: throttling anywhere on the account is pressure
    /// the replay reads share.
    /// </summary>
    internal static void NoteStorageRetry(int status)
    {
        if (status is 408 or 429 or 500 or 502 or 503 or 504)
            Interlocked.Increment(ref _throttleRetries);
    }

    /// <summary>Reads the cumulative tallies.</summary>
    internal static ReplayStoragePressureSample Read()
    {
        var buckets = new long[LatencyBuckets];
        for (var i = 0; i < LatencyBuckets; i++)
            buckets[i] = Volatile.Read(ref SliceLatency[i]);
        return new ReplayStoragePressureSample(
            Volatile.Read(ref _sliceReads), Volatile.Read(ref _throttleRetries), buckets);
    }

    /// <summary>Test-only: clears every tally.</summary>
    internal static void ResetForTest()
    {
        for (var i = 0; i < LatencyBuckets; i++)
            Volatile.Write(ref SliceLatency[i], 0);
        Volatile.Write(ref _sliceReads, 0);
        Volatile.Write(ref _throttleRetries, 0);
    }
}

/// <summary>Cumulative storage-pressure tallies at one instant.</summary>
/// <param name="SliceReads">Replay slice reads completed.</param>
/// <param name="ThrottleRetries">Throttle and unavailability retries reported by the storage provider.</param>
/// <param name="LatencyBuckets">Slice reads per power-of-two millisecond bucket.</param>
internal readonly record struct ReplayStoragePressureSample(long SliceReads, long ThrottleRetries, long[] LatencyBuckets)
{
    /// <summary>
    /// The upper bound, in milliseconds, of the bucket holding the 99th percentile
    /// of the reads between <paramref name="earlier"/> and this sample, or
    /// <see langword="null"/> when no read completed between them.
    /// </summary>
    internal long? P99MillisecondsSince(ReplayStoragePressureSample earlier)
    {
        var total = SliceReads - earlier.SliceReads;
        if (total <= 0)
            return null;

        var rank = (long)Math.Ceiling(total * 0.99);
        var seen = 0L;
        for (var i = 0; i < LatencyBuckets.Length; i++)
        {
            seen += LatencyBuckets[i] - (earlier.LatencyBuckets?[i] ?? 0);
            if (seen >= rank)
                return 1L << i;
        }

        return 1L << (LatencyBuckets.Length - 1);
    }
}
