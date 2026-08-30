using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// The deterministic half of the fan-out-reduction workload: an exact round-trip
/// census of the three sites the change batched, measured rather than argued.
/// </summary>
/// <remarks>
/// Round-trip count is the figure the change targets, and unlike latency it is
/// exact: it does not vary with host, core count, or scheduler luck, so it needs
/// no statistical treatment and no BenchmarkDotNet job. This pass runs each site's
/// baseline and shipped shape once per parameter point against the counting read
/// surface and reports the caller-visible hops each actually made, plus the total
/// hops the batched arm causes anywhere (its one facade crossing plus the
/// store-internal concurrent wave).
/// </remarks>
internal static class FanOutRoundTripReport
{
    private static readonly byte[] Row = [1];

    /// <summary>One measured row of the census.</summary>
    internal sealed record CensusRow(
        string Site,
        string Shape,
        int Width,
        int Groups,
        int BaselineRoundTrips,
        int BatchedRoundTrips,
        int BatchedTotalHops)
    {
        /// <summary>The batched caller-visible cost as a fraction of the baseline cost.</summary>
        public double Ratio => BaselineRoundTrips == 0 ? 1 : BatchedRoundTrips / (double)BaselineRoundTrips;

        /// <summary>Caller-visible round-trips removed.</summary>
        public int Saved => BaselineRoundTrips - BatchedRoundTrips;
    }

    // Sweep axes: T sibling tags per AND query over G candidates; N keys per
    // atomic step; F inverse shards per view materialisation.
    private static readonly int[] TagSweep = [2, 4, 8];
    private static readonly int[] KeySweep = [8, 32, 128];
    private static readonly int[] SlotSweep = [4, 16, 64];
    private const int AndCandidates = 100;

    /// <summary>Runs the census and returns one row per (site, parameter point).</summary>
    public static async Task<List<CensusRow>> MeasureAsync()
    {
        var rows = new List<CensusRow>();

        foreach (var tags in TagSweep)
        {
            rows.Add(await MeasureAndAsync(tags, AndCandidates).ConfigureAwait(false));
        }

        foreach (var keys in KeySweep)
        {
            rows.Add(await MeasureAtomicAsync(keys).ConfigureAwait(false));
        }

        foreach (var slots in SlotSweep)
        {
            rows.Add(await MeasureViewAsync(slots).ConfigureAwait(false));
        }

        return rows;
    }

    private static async Task<CensusRow> MeasureAndAsync(int tags, int candidates)
    {
        var seed = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var candidateProbeKeys = new List<IReadOnlyList<string>>(candidates);
        for (var c = 0; c < candidates; c++)
        {
            var probe = new List<string>(tags - 1);
            for (var t = 1; t < tags; t++)
            {
                var key = string.Create(CultureInfo.InvariantCulture, $"tag{t}\0tree\0k{c}");
                seed[key] = Row;
                probe.Add(key);
            }

            candidateProbeKeys.Add(probe);
        }

        var store = new FanOutReadSurface(seed);

        store.ResetCounters();
        await FanOutShapes.TagIndexAndBaselineAsync(store, candidateProbeKeys).ConfigureAwait(false);
        var baseline = store.RoundTrips;

        store.ResetCounters();
        await FanOutShapes.TagIndexAndBatchedAsync(store, candidateProbeKeys).ConfigureAwait(false);
        var batched = store.RoundTrips;
        var total = store.RoundTrips + store.FanOutReads;

        return new CensusRow("tag-index AND", $"{tags} tags x {candidates} keys", tags, candidates, baseline, batched, total);
    }

    private static async Task<CensusRow> MeasureAtomicAsync(int keyCount)
    {
        var seed = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var keys = new List<string>(keyCount);
        for (var i = 0; i < keyCount; i++)
        {
            var key = string.Create(CultureInfo.InvariantCulture, $"k{i}");
            seed[key] = Row;
            keys.Add(key);
        }

        var store = new FanOutReadSurface(seed);

        store.ResetCounters();
        await FanOutShapes.AtomicPreImageBaselineAsync(store, keys).ConfigureAwait(false);
        var baseline = store.RoundTrips;

        store.ResetCounters();
        await FanOutShapes.AtomicPreImageBatchedAsync(store, keys).ConfigureAwait(false);
        var batched = store.RoundTrips;
        var total = store.RoundTrips + store.FanOutReads;

        return new CensusRow("atomic pre-image", $"{keyCount} keys", keyCount, 1, baseline, batched, total);
    }

    private static async Task<CensusRow> MeasureViewAsync(int slots)
    {
        var seed = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var slotKeys = new List<string>(slots);
        for (var s = 0; s < slots; s++)
        {
            var key = string.Create(CultureInfo.InvariantCulture, $"g\0inv\0{s}");
            seed[key] = Row;
            slotKeys.Add(key);
        }

        var store = new FanOutReadSurface(seed);

        store.ResetCounters();
        await FanOutShapes.ViewInverseBaselineAsync(store, slotKeys).ConfigureAwait(false);
        var baseline = store.RoundTrips;

        store.ResetCounters();
        await FanOutShapes.ViewInverseBatchedAsync(store, slotKeys).ConfigureAwait(false);
        var batched = store.RoundTrips;
        var total = store.RoundTrips + store.FanOutReads;

        return new CensusRow("view inverse", $"{slots} shards", slots, 1, baseline, batched, total);
    }

    /// <summary>Renders the census as a fixed-width console table.</summary>
    public static string Render(IReadOnlyList<CensusRow> rows)
    {
        var sb = new StringBuilder();
        sb.AppendLine();
        sb.AppendLine("[fanout] caller-visible read round-trips before/after batching (exact, deterministic)");
        sb.AppendLine();
        sb.AppendLine("  site              shape                  baseline    batched   saved  ratio  all-hops");
        sb.AppendLine("  ----------------- --------------------- ---------  ---------  ------  -----  --------");
        foreach (var r in rows)
        {
            sb.AppendLine(string.Create(
                CultureInfo.InvariantCulture,
                $"  {r.Site,-17} {r.Shape,-21} {r.BaselineRoundTrips,9}  {r.BatchedRoundTrips,9}  {r.Saved,6}  {r.Ratio,5:F2}  {r.BatchedTotalHops,8}"));
        }

        sb.AppendLine();
        sb.AppendLine("  baseline = one sequential awaited read per sibling tag / key / shard. batched = one");
        sb.AppendLine("  multi-get per candidate (AND) or per operation (atomic, view). all-hops adds the");
        sb.AppendLine("  store-internal per-key reads the batched arm issues as one concurrent wave: those");
        sb.AppendLine("  reads did not vanish, they moved behind one facade crossing and cost one read");
        sb.AppendLine("  latency instead of N. Sequential await depth is what collapses.");
        sb.AppendLine();
        return sb.ToString();
    }

    /// <summary>Writes the census next to the harness results as <c>fanout-roundtrips.json</c>.</summary>
    public static void Write(IReadOnlyList<CensusRow> rows, string resultsJsonPath)
    {
        var directory = Path.GetDirectoryName(resultsJsonPath);
        if (string.IsNullOrEmpty(directory))
        {
            return;
        }

        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "fanout-roundtrips.json");
        File.WriteAllText(path, JsonSerializer.Serialize(rows, JsonOptions));
        Console.WriteLine($"[fanout] round-trip census -> {path}");
    }

    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };
}
