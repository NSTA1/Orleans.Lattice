using System.Text;
using System.Text.Json;

namespace Orleans.Lattice.Benchmark.Microbench.Profiling;

/// <summary>
/// One row in <see cref="ProfileReport.TopAllocators"/> or
/// <see cref="ProfileReport.TopCpu"/>: a single attributed managed method with
/// its aggregate allocation / sample contribution.
/// </summary>
/// <param name="Method">
/// Fully-qualified managed method name (e.g.
/// <c>Orleans.Lattice.BPlusTree.Grains.LatticeGrain.SetAsync</c>). Falls back
/// to <c>[unknown]</c> when stack symbolication failed.
/// </param>
/// <param name="Module">
/// Owning assembly's simple name (e.g. <c>Orleans.Lattice</c>). Empty when
/// the module could not be resolved.
/// </param>
/// <param name="AllocB">
/// Bytes attributed to <see cref="Method"/> across the profile window. Zero
/// when the row originates from the CPU-sample aggregation.
/// </param>
/// <param name="AllocPct">
/// <see cref="AllocB"/> as a percentage of <see cref="ProfileReport.TotalAllocationsB"/>,
/// rounded to one decimal place.
/// </param>
/// <param name="Samples">
/// CPU samples attributed to <see cref="Method"/>. Zero when the row
/// originates from the allocation aggregation.
/// </param>
/// <param name="SamplesPct">
/// <see cref="Samples"/> as a percentage of <see cref="ProfileReport.TotalCpuSamples"/>,
/// rounded to one decimal place.
/// </param>
public readonly record struct ProfileFrameRow(
    string Method,
    string Module,
    long AllocB,
    double AllocPct,
    long Samples,
    double SamplesPct);

/// <summary>
/// Aggregated EventPipe-profile output for a single microbench run.
/// Written to <c>profile.json</c> next to the harness <c>results.json</c>
/// whenever <see cref="ProfilerOptions.IsEnabled"/> is <see langword="true"/>.
/// </summary>
/// <param name="RunId">Microbench run identifier (mirrors <c>BENCH_RUN_ID</c>).</param>
/// <param name="GitSha">Source-tree git SHA (mirrors <c>BENCH_GIT_SHA</c>).</param>
/// <param name="CapturedAt">UTC timestamp the report was produced.</param>
/// <param name="Mode">Resolved <see cref="ProfileMode"/> the run used.</param>
/// <param name="DurationMs">Wall-clock duration of the profile window, in milliseconds.</param>
/// <param name="TotalAllocationsB">Sum of attributed allocations across all frames.</param>
/// <param name="TotalCpuSamples">Sum of attributed CPU samples across all frames.</param>
/// <param name="TopAllocators">Top-N allocators by <see cref="ProfileFrameRow.AllocB"/>, descending.</param>
/// <param name="TopCpu">Top-N CPU samples by <see cref="ProfileFrameRow.Samples"/>, descending.</param>
public sealed record ProfileReport(
    string RunId,
    string GitSha,
    DateTime CapturedAt,
    ProfileMode Mode,
    long DurationMs,
    long TotalAllocationsB,
    long TotalCpuSamples,
    IReadOnlyList<ProfileFrameRow> TopAllocators,
    IReadOnlyList<ProfileFrameRow> TopCpu)
{
    /// <summary>
    /// Serialises this report to UTF-8 JSON without a byte-order mark and with
    /// indented output, mirroring <c>HarnessJsonExporter</c>'s on-disk
    /// convention. Creates parent directories as needed.
    /// </summary>
    /// <param name="path">Absolute or workspace-relative path for the JSON file.</param>
    public void WriteJson(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["run_id"] = RunId,
            ["git_sha"] = GitSha,
            ["captured_at"] = CapturedAt.ToString("o", System.Globalization.CultureInfo.InvariantCulture),
            ["mode"] = Mode.ToString().ToLowerInvariant(),
            ["duration_ms"] = DurationMs,
            ["total_allocations_b"] = TotalAllocationsB,
            ["total_cpu_samples"] = TotalCpuSamples,
            ["top_allocators"] = TopAllocators.Select(FrameToDictionary).ToList(),
            ["top_cpu"] = TopCpu.Select(FrameToDictionary).ToList(),
        };

        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
        {
            Directory.CreateDirectory(dir);
        }
        var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(path, json, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
    }

    private static Dictionary<string, object?> FrameToDictionary(ProfileFrameRow row) =>
        new(StringComparer.Ordinal)
        {
            ["method"] = row.Method,
            ["module"] = row.Module,
            ["alloc_b"] = row.AllocB,
            ["alloc_pct"] = row.AllocPct,
            ["samples"] = row.Samples,
            ["samples_pct"] = row.SamplesPct,
        };
}
