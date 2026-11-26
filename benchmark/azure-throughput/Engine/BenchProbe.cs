using System.Diagnostics;
using Orleans.Lattice;

namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// The producer's read-only correctness probe (#6166), selected by
/// <c>BENCH_PROBE=verify-all</c>. It re-reads every seeded key of an existing tree
/// with <see cref="ILattice.GetManyAsync"/>, re-reads each key the batch read misses
/// with <see cref="ILattice.GetAsync"/>, logs every absent key with its seed offset
/// (capped) and every chunk that lost one, then logs the tree's entry count and the
/// orphaned-leaf survey. It writes nothing, so it can run against a parked tree.
/// <c>BENCH_PROBE_STEPS</c> selects and orders the steps and <c>BENCH_PROBE_RANGE</c> narrows the keys
/// the verify step reads.
/// </summary>
internal static class BenchProbe
{
    /// <summary>The environment variable that selects a probe.</summary>
    public const string EnvironmentVariable = "BENCH_PROBE";

    /// <summary>The probe that verifies every seeded key.</summary>
    public const string VerifyAll = "verify-all";

    /// <summary>How many absent or mismatched keys are logged one per line.</summary>
    public const int MaxLoggedKeys = 2000;

    /// <summary>How many attempts a transiently failing read gets.</summary>
    private const int MaxAttempts = 8;

    /// <summary>Whether <paramref name="raw"/> selects <see cref="VerifyAll"/>.</summary>
    public static bool IsVerifyAll(string? raw) =>
        string.Equals(raw?.Trim(), VerifyAll, StringComparison.OrdinalIgnoreCase);

    /// <summary>The probe's totals.</summary>
    /// <param name="Verified">Keys read back with the seeded payload.</param>
    /// <param name="Mismatch">Keys read back with another payload.</param>
    /// <param name="Missing">Keys the batch read did not return.</param>
    /// <param name="PointMissing">Of <paramref name="Missing"/>, keys the point read did not return either.</param>
    /// <param name="ChunksWithLoss">Seed chunks with at least one missing or mismatched key.</param>
    public sealed record Totals(long Verified, long Mismatch, long Missing, long PointMissing, int ChunksWithLoss);

    /// <summary>
    /// Verifies keys <c>[<paramref name="start"/>, <paramref name="count"/>)</c> in chunks of
    /// <paramref name="chunkSize"/>, aligned with the seed's, so a chunk's offset
    /// names the bulk append that wrote it.
    /// </summary>
    public static async Task<Totals> VerifyAllAsync(
        ILattice lattice,
        int count,
        int chunkSize,
        int parallelism,
        Action<string> log,
        CancellationToken ct,
        int maxLoggedKeys = MaxLoggedKeys,
        int start = 0)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfNegative(start);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkSize);
        long verified = 0, mismatch = 0, missing = 0, pointMissing = 0, done = 0;
        var chunksWithLoss = 0;
        var logged = 0;
        var gate = new object();
        var sw = Stopwatch.StartNew();
        var firstChunk = start / chunkSize;
        var offsets = Enumerable.Range(firstChunk, Math.Max(0, (count + chunkSize - 1) / chunkSize - firstChunk)).Select(c => c * chunkSize);
        await Parallel.ForEachAsync(
            offsets,
            new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, parallelism), CancellationToken = ct },
            async (offset, token) =>
            {
                var from = Math.Max(offset, start);
                var length = Math.Max(0, Math.Min(offset + chunkSize, count) - from);
                var keys = new List<string>(length);
                for (var j = 0; j < length; j++) keys.Add(BenchPreseed.KeyFor(from + j));
                var found = await WithRetryAsync(() => lattice.GetManyAsync(keys, token), log, token).ConfigureAwait(false);
                long chunkVerified = 0, chunkMismatch = 0, chunkMissing = 0, chunkPointMissing = 0;
                for (var j = 0; j < length; j++)
                {
                    var index = from + j;
                    var expected = BenchPreseed.PayloadFor(index);
                    string? verdict = null;
                    if (!found.TryGetValue(keys[j], out var value))
                    {
                        chunkMissing++;
                        var point = await WithRetryAsync(() => lattice.GetAsync(keys[j], token), log, token).ConfigureAwait(false);
                        verdict = point is null ? "absent" : point.AsSpan().SequenceEqual(expected) ? "point-present" : "point-mismatch";
                        if (point is null) chunkPointMissing++;
                    }
                    else if (!value.AsSpan().SequenceEqual(expected))
                    {
                        chunkMismatch++;
                        verdict = "mismatch";
                    }
                    else
                    {
                        chunkVerified++;
                    }

                    if (verdict is not null && Interlocked.Increment(ref logged) <= maxLoggedKeys)
                    {
                        log($"[probe] key offset={index} chunk={offset} key={keys[j]} verdict={verdict}");
                    }
                }

                Interlocked.Add(ref verified, chunkVerified);
                Interlocked.Add(ref mismatch, chunkMismatch);
                Interlocked.Add(ref missing, chunkMissing);
                Interlocked.Add(ref pointMissing, chunkPointMissing);
                if (chunkMissing > 0 || chunkMismatch > 0)
                {
                    Interlocked.Increment(ref chunksWithLoss);
                    log($"[probe] chunk offset={offset} length={length} missing={chunkMissing} pointMissing={chunkPointMissing} mismatch={chunkMismatch}");
                }

                var total = Interlocked.Add(ref done, length);
                if (total / 1_048_576 != (total - length) / 1_048_576)
                {
                    lock (gate)
                    {
                        log($"[probe] progress read={total}/{count - start} missing={Interlocked.Read(ref missing)} elapsedMs={sw.Elapsed.TotalMilliseconds:F0}");
                    }
                }
            }).ConfigureAwait(false);

        var totals = new Totals(verified, mismatch, missing, pointMissing, chunksWithLoss);
        log($"[probe] verify-all range={start}:{count} verified={totals.Verified} mismatch={totals.Mismatch} missing={totals.Missing} pointMissing={totals.PointMissing} chunksWithLoss={totals.ChunksWithLoss} elapsedMs={sw.Elapsed.TotalMilliseconds:F0}");
        return totals;
    }

    /// <summary>The probe steps <c>BENCH_PROBE_STEPS</c> may name, in the order they run by default.</summary>
    public static readonly IReadOnlyList<string> DefaultSteps = ["verify", "count", "orphans"];

    /// <summary>
    /// Parses <c>BENCH_PROBE_STEPS</c>: a comma-separated list of <c>verify</c>, <c>count</c> and
    /// <c>orphans</c>, run in the order given; unset runs <see cref="DefaultSteps"/>.
    /// </summary>
    public static IReadOnlyList<string> ParseSteps(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return DefaultSteps;
        var steps = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(static s => s.ToLowerInvariant())
            .ToArray();
        foreach (var step in steps)
        {
            if (!DefaultSteps.Contains(step))
                throw new ArgumentException($"BENCH_PROBE_STEPS entry '{step}' is not one of {string.Join(", ", DefaultSteps)}.");
        }

        return steps;
    }

    /// <summary>
    /// Parses <c>BENCH_PROBE_RANGE</c> (<c>start:end</c>, end exclusive) into the key indexes
    /// the verify step reads, clamped to <c>[0, <paramref name="count"/>)</c>; unset is the
    /// whole seed.
    /// </summary>
    public static (int Start, int End) ParseRange(string? raw, int count)
    {
        if (string.IsNullOrWhiteSpace(raw)) return (0, count);
        var parts = raw.Split(':', StringSplitOptions.TrimEntries);
        if (parts.Length != 2
            || !int.TryParse(parts[0], System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var start)
            || !int.TryParse(parts[1], System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var end)
            || end <= start)
        {
            throw new ArgumentException($"BENCH_PROBE_RANGE '{raw}' is not start:end with end > start.");
        }

        return (Math.Min(start, count), Math.Min(end, count));
    }

    /// <summary>
    /// Logs the tree's entry count. One attempt only: the count walks every leaf, so a
    /// retry after a timeout repeats the whole walk.
    /// </summary>
    public static async Task CountAsync(ILattice lattice, Action<string> log, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(log);
        var sw = Stopwatch.StartNew();
        try
        {
            var entries = await lattice.CountAsync(ct).ConfigureAwait(false);
            log($"[probe] count entries={entries} elapsedMs={sw.Elapsed.TotalMilliseconds:F0}");
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            log($"[probe] count failed after elapsedMs={sw.Elapsed.TotalMilliseconds:F0}: {ex.GetType().Name}: {ex.Message}");
        }
    }

    /// <summary>
    /// Drives the read-only orphaned-leaf survey to completion, logging every finding and gap
    /// as its batch returns and a progress line per batch.
    /// </summary>
    public static async Task SurveyOrphansAsync(ILattice lattice, Action<string> log, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(log);
        var sw = Stopwatch.StartNew();
        string? resume = null;
        int leaves = 0, orphans = 0, gaps = 0, batches = 0;
        try
        {
            do
            {
                var report = await WithRetryAsync(() => lattice.SurveyOrphanedLeavesAsync(resume, ct), log, ct).ConfigureAwait(false);
                batches++;
                leaves += report.LeavesWalked;
                foreach (var finding in report.Findings ?? [])
                {
                    orphans++;
                    log($"[probe] orphan shard={finding.ShardIndex} leaf={finding.LeafId} low={finding.LowKeyInclusive ?? "-"} high={finding.HighKeyExclusive ?? "-"} keys={finding.KeyCount} surveyMissing={finding.SurveyMissingKeyCount?.ToString() ?? "-"} disposition={finding.Disposition}");
                }

                foreach (var gap in report.Gaps ?? [])
                {
                    gaps++;
                    log($"[probe] orphan-gap shard={gap.ShardIndex} reason={gap.Reason} leaf={gap.LeafId ?? "-"} keyHint={gap.KeyHint ?? "-"}");
                }

                resume = report.ResumeFrom;
                log($"[probe] orphan-survey-progress batch={batches} leavesWalked={leaves} orphans={orphans} gaps={gaps} complete={(resume is null ? "true" : "false")} elapsedMs={sw.Elapsed.TotalMilliseconds:F0}");
            }
            while (resume is not null);

            log($"[probe] orphan-survey leavesWalked={leaves} orphans={orphans} gaps={gaps}");
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            log($"[probe] orphan-survey failed after leavesWalked={leaves}: {ex.GetType().Name}: {ex.Message}");
        }
    }
    private static async Task<T> WithRetryAsync<T>(Func<Task<T>> read, Action<string> log, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await read().ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < MaxAttempts && !ct.IsCancellationRequested)
            {
                log($"[probe] transient ({ex.GetType().Name}) attempt={attempt}/{MaxAttempts}: {ex.Message}");
                await Task.Delay(Math.Min(1000 * attempt, 5000), ct).ConfigureAwait(false);
            }
        }
    }
}
