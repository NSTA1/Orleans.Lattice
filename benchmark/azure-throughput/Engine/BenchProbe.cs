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
    /// Verifies keys <c>[0, <paramref name="count"/>)</c> in chunks of
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
        int maxLoggedKeys = MaxLoggedKeys)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkSize);
        long verified = 0, mismatch = 0, missing = 0, pointMissing = 0, done = 0;
        var chunksWithLoss = 0;
        var logged = 0;
        var gate = new object();
        var sw = Stopwatch.StartNew();
        var offsets = Enumerable.Range(0, (count + chunkSize - 1) / chunkSize).Select(c => c * chunkSize);
        await Parallel.ForEachAsync(
            offsets,
            new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, parallelism), CancellationToken = ct },
            async (offset, token) =>
            {
                var length = Math.Min(chunkSize, count - offset);
                var keys = new List<string>(length);
                for (var j = 0; j < length; j++) keys.Add(BenchPreseed.KeyFor(offset + j));
                var found = await WithRetryAsync(() => lattice.GetManyAsync(keys, token), log, token).ConfigureAwait(false);
                long chunkVerified = 0, chunkMismatch = 0, chunkMissing = 0, chunkPointMissing = 0;
                for (var j = 0; j < length; j++)
                {
                    var index = offset + j;
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
                        log($"[probe] progress read={total}/{count} missing={Interlocked.Read(ref missing)} elapsedMs={sw.Elapsed.TotalMilliseconds:F0}");
                    }
                }
            }).ConfigureAwait(false);

        var totals = new Totals(verified, mismatch, missing, pointMissing, chunksWithLoss);
        log($"[probe] verify-all keys={count} verified={totals.Verified} mismatch={totals.Mismatch} missing={totals.Missing} pointMissing={totals.PointMissing} chunksWithLoss={totals.ChunksWithLoss} elapsedMs={sw.Elapsed.TotalMilliseconds:F0}");
        return totals;
    }

    /// <summary>Logs the tree's entry count and drives the orphaned-leaf survey to completion; both read only.</summary>
    public static async Task ReportTreeAsync(ILattice lattice, Action<string> log, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(log);
        try
        {
            var entries = await WithRetryAsync(() => lattice.CountAsync(ct), log, ct).ConfigureAwait(false);
            log($"[probe] count entries={entries}");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            log($"[probe] count failed: {ex.GetType().Name}: {ex.Message}");
        }

        string? resume = null;
        int leaves = 0, orphans = 0, gaps = 0;
        try
        {
            do
            {
                var report = await WithRetryAsync(() => lattice.SurveyOrphanedLeavesAsync(resume, ct), log, ct).ConfigureAwait(false);
                leaves += report.LeavesWalked;
                foreach (var finding in report.Findings ?? [])
                {
                    orphans++;
                    log($"[probe] orphan shard={finding.ShardIndex} leaf={finding.LeafId} low={finding.LowKeyInclusive ?? "-"} high={finding.HighKeyExclusive ?? "-"} keys={finding.KeyCount} surveyMissing={finding.SurveyMissingKeyCount?.ToString() ?? "-"} disposition={finding.Disposition}");
                }

                foreach (var gap in report.Gaps ?? [])
                {
                    gaps++;
                    log($"[probe] orphan-gap shard={gap.ShardIndex} reason={gap.Reason} leaf={gap.LeafId ?? "-"}");
                }

                resume = report.ResumeFrom;
            }
            while (resume is not null);

            log($"[probe] orphan-survey leavesWalked={leaves} orphans={orphans} gaps={gaps}");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
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
