using System.Diagnostics;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Tracks how many full scan passes each background worker in a
/// "scan while the tree splits underneath you" integration test has completed,
/// and lets the test wait on that progress instead of sleeping for a guessed
/// interval.
/// <para>
/// A bare <c>Task.Delay</c> only proves that wall-clock time elapsed in the test
/// thread. On a loaded or starved build agent the scan workers may not have run
/// at all in that window, so the split can be driven to completion before a
/// single scan is in flight and the test silently stops covering the overlap it
/// exists to cover - it still passes, but it no longer tests anything. Waiting
/// on the workers' own pass counters is a real happens-after barrier: N recorded
/// passes prove the worker genuinely completed N scans. It is also faster in the
/// common case, because it returns as soon as the evidence exists rather than
/// always burning the full fixed delay.
/// </para>
/// </summary>
/// <param name="workerCount">Number of concurrent scan workers being tracked.</param>
internal sealed class ConcurrentScanProgress(int workerCount)
{
    private const int PollIntervalMs = 5;

    private readonly int[] _passes = new int[workerCount];

    /// <summary>Total scan passes completed across every worker.</summary>
    public int TotalPasses
    {
        get
        {
            var total = 0;
            for (var i = 0; i < workerCount; i++) total += Volatile.Read(ref _passes[i]);
            return total;
        }
    }

    /// <summary>Records that <paramref name="worker"/> completed one full scan pass.</summary>
    public void RecordPass(int worker = 0) => Interlocked.Increment(ref _passes[worker]);

    /// <summary>Takes a point-in-time copy of the per-worker pass counts.</summary>
    public int[] Snapshot()
    {
        var snapshot = new int[workerCount];
        for (var i = 0; i < workerCount; i++) snapshot[i] = Volatile.Read(ref _passes[i]);
        return snapshot;
    }

    /// <summary>
    /// Waits until every worker has completed at least one full pass. Because a
    /// worker immediately begins its next pass after recording one, this also
    /// establishes that a scan is in flight when the caller proceeds.
    /// </summary>
    public Task WaitForOnePassEachAsync(string phase, int timeoutMs = 60_000)
        => WaitForFurtherPassEachAsync(new int[workerCount], phase, timeoutMs);

    /// <summary>
    /// Waits until every worker has completed at least one further pass beyond
    /// <paramref name="baseline"/> (a prior <see cref="Snapshot"/>), failing the
    /// test with the observed counts if that does not happen within
    /// <paramref name="timeoutMs"/>.
    /// </summary>
    public async Task WaitForFurtherPassEachAsync(int[] baseline, string phase, int timeoutMs = 60_000)
    {
        ArgumentNullException.ThrowIfNull(baseline);

        var elapsed = Stopwatch.StartNew();
        while (true)
        {
            var current = Snapshot();
            var pending = Enumerable.Range(0, workerCount).Where(i => current[i] <= baseline[i]).ToArray();
            if (pending.Length == 0) return;

            if (elapsed.ElapsedMilliseconds >= timeoutMs)
            {
                Assert.Fail(
                    $"Timed out after {timeoutMs} ms waiting for every scan worker to complete a pass ({phase}). " +
                    $"Workers still short: [{string.Join(", ", pending)}]. " +
                    $"Baseline: [{string.Join(", ", baseline)}], observed: [{string.Join(", ", current)}].");
            }

            await Task.Delay(PollIntervalMs);
        }
    }
}
