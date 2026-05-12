using System.Collections.Concurrent;

namespace Orleans.Lattice.Benchmark.Microbench.Profiling;

/// <summary>
/// Pure, thread-safe aggregation primitive for the EventPipe-driven profiler.
/// Receives per-event observations (attributed managed-method names, allocation
/// byte-counts, sample counts) and produces a sorted <see cref="ProfileReport"/>
/// at the end of the profile window.
/// </summary>
/// <remarks>
/// This type has no dependency on EventPipe / TraceEvent so it can be unit
/// tested independently of the live event pump.
/// </remarks>
public sealed class ProfileAggregator
{
    private readonly ConcurrentDictionary<MethodKey, long> _allocBytes = new();
    private readonly ConcurrentDictionary<MethodKey, long> _samples = new();
    private long _totalAllocBytes;
    private long _totalSamples;

    /// <summary>
    /// Total allocation bytes observed so far across all methods.
    /// </summary>
    public long TotalAllocBytes => Interlocked.Read(ref _totalAllocBytes);

    /// <summary>
    /// Total CPU samples observed so far across all methods.
    /// </summary>
    public long TotalSamples => Interlocked.Read(ref _totalSamples);

    /// <summary>
    /// Records an allocation event attributed to <paramref name="method"/>.
    /// Null or whitespace method names are normalised to <c>[unknown]</c>;
    /// non-positive byte counts are ignored.
    /// </summary>
    /// <param name="method">Fully-qualified managed method name (or null when symbolication failed).</param>
    /// <param name="module">Owning assembly's simple name (may be null/empty).</param>
    /// <param name="bytes">Bytes allocated by this event. Must be positive; non-positive values are ignored.</param>
    public void RecordAllocation(string? method, string? module, long bytes)
    {
        if (bytes <= 0)
        {
            return;
        }
        var key = new MethodKey(Normalise(method), module ?? string.Empty);
        // The TArg overload lets both factories be static, so we don't allocate
        // a per-call closure to carry `bytes` into the update factory. This
        // matters because this method is invoked once per GCSampledObjectAllocation
        // event - potentially tens of thousands of times per second under load.
        _allocBytes.AddOrUpdate(
            key,
            static (_, b) => b,
            static (_, current, b) => current + b,
            bytes);
        Interlocked.Add(ref _totalAllocBytes, bytes);
    }

    /// <summary>
    /// Records a single CPU sample attributed to <paramref name="method"/>.
    /// Null or whitespace method names are normalised to <c>[unknown]</c>.
    /// </summary>
    /// <param name="method">Fully-qualified managed method name (or null when symbolication failed).</param>
    /// <param name="module">Owning assembly's simple name (may be null/empty).</param>
    public void RecordSample(string? method, string? module)
    {
        var key = new MethodKey(Normalise(method), module ?? string.Empty);
        _samples.AddOrUpdate(key, 1L, static (_, current) => current + 1);
        Interlocked.Increment(ref _totalSamples);
    }

    /// <summary>
    /// Snapshots the current aggregation state into a <see cref="ProfileReport"/>.
    /// The two top-N lists are independently sorted descending by their primary
    /// metric (bytes for allocators, samples for CPU). Percentages are rounded
    /// to one decimal place.
    /// </summary>
    /// <param name="runId">Mirrors <c>BENCH_RUN_ID</c>.</param>
    /// <param name="gitSha">Mirrors <c>BENCH_GIT_SHA</c>.</param>
    /// <param name="mode">Profile mode the producing session used.</param>
    /// <param name="duration">Wall-clock duration of the profile window.</param>
    /// <param name="topN">Maximum number of rows to emit per top-list. Must be positive.</param>
    public ProfileReport Build(
        string runId,
        string gitSha,
        ProfileMode mode,
        TimeSpan duration,
        int topN)
    {
        ArgumentNullException.ThrowIfNull(runId);
        ArgumentNullException.ThrowIfNull(gitSha);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(topN);

        var totalAlloc = TotalAllocBytes;
        var totalSamples = TotalSamples;

        var topAlloc = _allocBytes
            .Select(kvp => BuildRow(kvp.Key, kvp.Value, 0, totalAlloc, totalSamples))
            .OrderByDescending(static r => r.AllocB)
            .ThenBy(static r => r.Method, StringComparer.Ordinal)
            .Take(topN)
            .ToList();

        var topCpu = _samples
            .Select(kvp => BuildRow(kvp.Key, 0, kvp.Value, totalAlloc, totalSamples))
            .OrderByDescending(static r => r.Samples)
            .ThenBy(static r => r.Method, StringComparer.Ordinal)
            .Take(topN)
            .ToList();

        return new ProfileReport(
            RunId: runId,
            GitSha: gitSha,
            CapturedAt: DateTime.UtcNow,
            Mode: mode,
            DurationMs: (long)duration.TotalMilliseconds,
            TotalAllocationsB: totalAlloc,
            TotalCpuSamples: totalSamples,
            TopAllocators: topAlloc,
            TopCpu: topCpu);
    }

    private static ProfileFrameRow BuildRow(
        MethodKey key,
        long allocBytes,
        long samples,
        long totalAlloc,
        long totalSamples)
    {
        var allocPct = totalAlloc > 0 && allocBytes > 0
            ? Math.Round((double)allocBytes * 100d / totalAlloc, 1, MidpointRounding.AwayFromZero)
            : 0d;
        var samplesPct = totalSamples > 0 && samples > 0
            ? Math.Round((double)samples * 100d / totalSamples, 1, MidpointRounding.AwayFromZero)
            : 0d;
        return new ProfileFrameRow(key.Method, key.Module, allocBytes, allocPct, samples, samplesPct);
    }

    private static string Normalise(string? method) =>
        string.IsNullOrWhiteSpace(method) ? "[unknown]" : method;

    /// <summary>
    /// Composite (method, module) accumulation key. Module is included so two
    /// methods that share an identifier across assemblies (e.g.
    /// <c>System.Linq.Enumerable.Select</c> vs a user override) are
    /// distinguishable in the top-list.
    /// </summary>
    private readonly record struct MethodKey(string Method, string Module);
}
