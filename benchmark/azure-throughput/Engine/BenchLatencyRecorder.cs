using System.Diagnostics;

namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// Lock-free client-side latency histogram for the read workloads, used by the
/// tree-size benchmark to measure read latency as the producer observes it.
/// </summary>
/// <remarks>
/// <para>
/// Each sample lands in a logarithmic bucket <c>floor(16 * log2(us + 1))</c>,
/// so a bucket spans about 4.4% of its value and a quantile read back from it
/// is accurate to that resolution. Three histograms are kept: the current
/// report window (drained by <see cref="DrainWindow"/>), the whole run, and
/// the steady state, which counts only samples recorded after
/// <see cref="MarkSteady"/>.
/// </para>
/// <para>
/// Only a successful call is recorded. A call retried after a transient fault
/// is recorded once, timed from its last attempt.
/// </para>
/// </remarks>
public sealed class BenchLatencyRecorder
{
    private const int BucketsPerOctave = 16;
    private const int BucketCount = 40 * BucketsPerOctave;

    private readonly long[] window = new long[BucketCount];
    private readonly long[] total = new long[BucketCount];
    private readonly long[] steady = new long[BucketCount];
    private volatile bool isSteady;

    /// <summary>Starts recording into the steady-state histogram as well.</summary>
    public void MarkSteady() => isSteady = true;

    /// <summary>Records one call that started at the <see cref="Stopwatch"/> timestamp <paramref name="startTimestamp"/>.</summary>
    public void RecordSince(long startTimestamp) =>
        RecordMicros((Stopwatch.GetTimestamp() - startTimestamp) * 1_000_000.0 / Stopwatch.Frequency);

    /// <summary>Records one call that took <paramref name="micros"/> microseconds.</summary>
    public void RecordMicros(double micros)
    {
        var bucket = BucketFor(micros);
        Interlocked.Increment(ref window[bucket]);
        Interlocked.Increment(ref total[bucket]);
        if (isSteady) Interlocked.Increment(ref steady[bucket]);
    }

    /// <summary>Summarises and resets the current report window.</summary>
    public LatencySummary DrainWindow()
    {
        var copy = new long[BucketCount];
        for (var i = 0; i < BucketCount; i++) copy[i] = Interlocked.Exchange(ref window[i], 0);
        return Summarise(copy);
    }

    /// <summary>Summarises every sample recorded this run.</summary>
    public LatencySummary Total() => Summarise(Snapshot(total));

    /// <summary>Summarises the samples recorded after <see cref="MarkSteady"/>.</summary>
    public LatencySummary Steady() => Summarise(Snapshot(steady));

    internal static int BucketFor(double micros)
    {
        if (!(micros > 0)) return 0;
        var bucket = (int)(BucketsPerOctave * Math.Log2(micros + 1));
        return Math.Clamp(bucket, 0, BucketCount - 1);
    }

    // The upper edge of a bucket, so a quantile is never under-reported.
    internal static double UpperMicros(int bucket) => Math.Pow(2, (bucket + 1) / (double)BucketsPerOctave) - 1;

    private static long[] Snapshot(long[] source)
    {
        var copy = new long[BucketCount];
        for (var i = 0; i < BucketCount; i++) copy[i] = Interlocked.Read(ref source[i]);
        return copy;
    }

    private static LatencySummary Summarise(long[] buckets)
    {
        long count = 0;
        var max = -1;
        for (var i = 0; i < buckets.Length; i++)
        {
            if (buckets[i] == 0) continue;
            count += buckets[i];
            max = i;
        }
        if (count == 0) return new LatencySummary(0, 0, 0, 0, 0);
        return new LatencySummary(
            count,
            Quantile(buckets, count, 0.50) / 1000.0,
            Quantile(buckets, count, 0.95) / 1000.0,
            Quantile(buckets, count, 0.99) / 1000.0,
            UpperMicros(max) / 1000.0);
    }

    private static double Quantile(long[] buckets, long count, double q)
    {
        var rank = (long)Math.Ceiling(q * count);
        long seen = 0;
        for (var i = 0; i < buckets.Length; i++)
        {
            seen += buckets[i];
            if (seen >= rank) return UpperMicros(i);
        }
        return UpperMicros(buckets.Length - 1);
    }
}

/// <summary>A latency histogram summary; every figure but <see cref="Count"/> is in milliseconds.</summary>
/// <param name="Count">Samples summarised.</param>
/// <param name="P50Ms">Median.</param>
/// <param name="P95Ms">95th percentile.</param>
/// <param name="P99Ms">99th percentile.</param>
/// <param name="MaxMs">Largest sample's bucket upper edge.</param>
public readonly record struct LatencySummary(long Count, double P50Ms, double P95Ms, double P99Ms, double MaxMs)
{
    /// <summary>Formats the summary as the <c>count= p50= p95= p99= max=</c> tokens the report parses.</summary>
    public string Format() =>
        FormattableString.Invariant($"count={Count} p50Ms={P50Ms:0.000} p95Ms={P95Ms:0.000} p99Ms={P99Ms:0.000} maxMs={MaxMs:0.000}");
}
