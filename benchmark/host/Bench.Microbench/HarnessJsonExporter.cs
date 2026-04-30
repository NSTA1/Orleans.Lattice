using System.Globalization;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Reports;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// BenchmarkDotNet <see cref="IExporter"/> that converts the BDN summary into
/// a harness-shaped <c>results.json</c> compatible with <c>benchmark.ps1</c>'s
/// Compare flow and the cockpit history dashboard.
/// <para>
/// One stat tile per workload becomes a flat <c>microbench_&lt;workload&gt;_&lt;stat&gt;</c>
/// key; statistics are computed from the raw workload-iteration measurements
/// directly (rather than relying on a particular BDN version's
/// <c>Statistics.Percentiles</c> shape) so the harness keeps working across
/// BDN upgrades.
/// </para>
/// </summary>
internal sealed class HarnessJsonExporter : IExporter
{
    private readonly string _outputPath;

    public HarnessJsonExporter(string outputPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(outputPath);
        _outputPath = outputPath;
    }

    public string Name => nameof(HarnessJsonExporter);

    public void ExportToLog(Summary summary, ILogger logger)
    {
        // The harness file is the durable artefact; nothing useful goes to BDN's log.
    }

    public IEnumerable<string> ExportToFiles(Summary summary, ILogger consoleLogger)
    {
        ArgumentNullException.ThrowIfNull(summary);

        var metrics = new Dictionary<string, object?>(StringComparer.Ordinal);

        foreach (var report in summary.Reports)
        {
            var slug = ToSlug(report.BenchmarkCase.Descriptor.WorkloadMethod.Name);

            // Pull the workload-only measurements (skip Pilot/Warmup/etc.) and
            // normalise to nanoseconds-per-operation.
            var samples = ExtractWorkloadNsPerOp(report);
            if (samples.Length == 0)
            {
                metrics[$"microbench_{slug}_mean_ns"] = null;
                metrics[$"microbench_{slug}_p50_ns"] = null;
                metrics[$"microbench_{slug}_p95_ns"] = null;
                metrics[$"microbench_{slug}_p99_ns"] = null;
                metrics[$"microbench_{slug}_per_second"] = null;
                metrics[$"microbench_{slug}_alloc_b"] = null;
                continue;
            }

            Array.Sort(samples);
            var mean = Mean(samples);
            var p50 = Percentile(samples, 50);
            var p95 = Percentile(samples, 95);
            var p99 = Percentile(samples, 99);

            metrics[$"microbench_{slug}_mean_ns"] = Round(mean, 1);
            metrics[$"microbench_{slug}_p50_ns"] = Round(p50, 1);
            metrics[$"microbench_{slug}_p95_ns"] = Round(p95, 1);
            metrics[$"microbench_{slug}_p99_ns"] = Round(p99, 1);
            metrics[$"microbench_{slug}_per_second"] = mean > 0 ? Round(1_000_000_000d / mean, 0) : (object?)null;

            // Allocated-bytes-per-op comes from the MemoryDiagnoser; expose it as
            // a single scalar so the cockpit can sparkline-trend it.
            var allocated = report.Metrics.TryGetValue("Allocated Memory", out var allocMetric)
                ? allocMetric.Value
                : (double?)null;
            metrics[$"microbench_{slug}_alloc_b"] = allocated.HasValue ? Round(allocated.Value, 0) : (object?)null;
        }

        var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["scenario"] = Environment.GetEnvironmentVariable("BENCH_SCENARIO") ?? "microbench",
            ["run_id"] = Environment.GetEnvironmentVariable("BENCH_RUN_ID") ?? DateTime.UtcNow.ToString("yyyy-MM-ddTHH-mm-ssZ", CultureInfo.InvariantCulture),
            ["git_sha"] = Environment.GetEnvironmentVariable("BENCH_GIT_SHA"),
            ["started"] = Environment.GetEnvironmentVariable("BENCH_STARTED") ?? DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
            ["ended"] = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
            ["duration_s"] = ParseIntOrNull(Environment.GetEnvironmentVariable("BENCH_DURATION_S")),
            ["config"] = CollectScenarioEnv(),
            ["metrics"] = metrics,
            ["fleetStats"] = new Dictionary<string, object?>(StringComparer.Ordinal),
        };

        Directory.CreateDirectory(Path.GetDirectoryName(_outputPath)!);
        var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText(_outputPath, json, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
        consoleLogger.WriteLine($"[harness] wrote {_outputPath} ({metrics.Count} metric keys)");
        yield return _outputPath;
    }

    /// <summary>Extracts workload-only measurements as nanoseconds per operation.</summary>
    private static double[] ExtractWorkloadNsPerOp(BenchmarkReport report)
    {
        var list = new List<double>(report.AllMeasurements.Count);
        foreach (var m in report.AllMeasurements)
        {
            if (m.IterationMode == IterationMode.Workload && m.Operations > 0)
            {
                list.Add(m.Nanoseconds / m.Operations);
            }
        }
        return [.. list];
    }

    private static double Mean(double[] sorted)
    {
        var sum = 0d;
        for (var i = 0; i < sorted.Length; i++)
        {
            sum += sorted[i];
        }
        return sum / sorted.Length;
    }

    /// <summary>Linear-interpolation percentile over a pre-sorted ascending sample.</summary>
    private static double Percentile(double[] sorted, double percentile)
    {
        if (sorted.Length == 1)
        {
            return sorted[0];
        }
        var rank = (percentile / 100d) * (sorted.Length - 1);
        var lo = (int)Math.Floor(rank);
        var hi = (int)Math.Ceiling(rank);
        if (lo == hi)
        {
            return sorted[lo];
        }
        var frac = rank - lo;
        return (sorted[lo] * (1 - frac)) + (sorted[hi] * frac);
    }

    private static double Round(double value, int digits) =>
        Math.Round(value, digits, MidpointRounding.AwayFromZero);

    private static int? ParseIntOrNull(string? raw) =>
        int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : (int?)null;

    /// <summary>Capture every <c>BENCH_*</c> env var so the run's config is auditable from the JSON alone.</summary>
    private static Dictionary<string, string?> CollectScenarioEnv()
    {
        var bag = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (System.Collections.DictionaryEntry e in Environment.GetEnvironmentVariables())
        {
            var key = e.Key as string;
            if (key is null || !key.StartsWith("BENCH_", StringComparison.Ordinal))
            {
                continue;
            }
            bag[key] = e.Value as string;
        }
        return bag;
    }

    /// <summary>
    /// Lower-snake-case slug for a method name. Example: <c>"PointRead"</c> &rarr;
    /// <c>"point_read"</c>; <c>"Mixed_70R_30W"</c> &rarr; <c>"mixed_70r_30w"</c>.
    /// </summary>
    private static string ToSlug(string methodName)
    {
        var sb = new StringBuilder(methodName.Length + 4);
        for (var i = 0; i < methodName.Length; i++)
        {
            var c = methodName[i];
            if (c == '_')
            {
                sb.Append('_');
                continue;
            }
            if (char.IsUpper(c) && i > 0 && methodName[i - 1] != '_' && !char.IsUpper(methodName[i - 1]))
            {
                sb.Append('_');
            }
            sb.Append(char.ToLowerInvariant(c));
        }
        return sb.ToString();
    }
}
