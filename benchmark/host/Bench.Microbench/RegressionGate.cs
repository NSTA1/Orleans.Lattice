using System.Globalization;
using System.Text.Json;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Pure-function regression detector that compares a freshly-produced
/// <c>results.json</c> against a pinned baseline (e.g. <c>baseline-v3.4.0.json</c>)
/// and flags every gated metric whose move exceeds a tolerance band in the
/// "bad" direction.
/// <para>
/// Direction is inferred from the metric-key suffix produced by
/// <see cref="HarnessJsonExporter"/>:
/// <list type="bullet">
///   <item><description><c>_mean_ns</c>, <c>_p50_ns</c>, <c>_p95_ns</c>, <c>_p99_ns</c>, <c>_alloc_b</c> - lower-is-better;
///     a positive percentage delta is a regression.</description></item>
///   <item><description><c>_per_second</c> - higher-is-better; a negative percentage delta is a regression.</description></item>
/// </list>
/// </para>
/// <para>
/// The gate is intentionally narrow by default - only the metrics enumerated
/// in <see cref="DefaultGatedMetrics"/> are evaluated, because broad-based
/// gating against the full microbench surface produces too much false-positive
/// noise (microbench jitter is real). The defaults cover the F-055 read-path
/// acceptance clause: <c>GetAsync</c> / <c>KeysAsync</c> show no measurable
/// latency regression versus the v3.4.0 baseline when no saga is in flight.
/// </para>
/// </summary>
internal static class RegressionGate
{
    /// <summary>
    /// The metric keys gated by default. These are the F-055 read-path acceptance
    /// metrics; additional keys can be supplied per-call via the <c>gatedMetrics</c>
    /// argument on <see cref="Compare(string, string, double, IReadOnlyCollection{string}?)"/>.
    /// </summary>
    public static readonly IReadOnlyCollection<string> DefaultGatedMetrics =
    [
        "microbench_point_read_p99_ns",
        "microbench_point_read_atomic_tree_idle_p99_ns",
        "microbench_key_scan_page_over4_shards_p99_ns",
    ];

    /// <summary>
    /// Compares <paramref name="currentPath"/> against <paramref name="baselinePath"/>
    /// for each metric in <paramref name="gatedMetrics"/> (or
    /// <see cref="DefaultGatedMetrics"/> if null/empty) and returns a
    /// <see cref="RegressionReport"/> enumerating every violation that exceeds
    /// <paramref name="tolerancePct"/> in the bad direction.
    /// </summary>
    /// <param name="baselinePath">Path to the pinned baseline <c>results.json</c>.</param>
    /// <param name="currentPath">Path to the current run's <c>results.json</c>.</param>
    /// <param name="tolerancePct">
    /// Tolerance band in percent. A move greater than this in the bad direction
    /// is a regression; a smaller move (in either direction) is "noise".
    /// </param>
    /// <param name="gatedMetrics">
    /// Optional override of the gated metric keys. When null or empty,
    /// <see cref="DefaultGatedMetrics"/> is used.
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown when either path is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="tolerancePct"/> is negative.</exception>
    public static RegressionReport Compare(
        string baselinePath,
        string currentPath,
        double tolerancePct,
        IReadOnlyCollection<string>? gatedMetrics = null)
    {
        ArgumentNullException.ThrowIfNull(baselinePath);
        ArgumentNullException.ThrowIfNull(currentPath);
        ArgumentOutOfRangeException.ThrowIfNegative(tolerancePct);

        var keys = (gatedMetrics is { Count: > 0 } ? gatedMetrics : DefaultGatedMetrics).ToArray();
        var baseline = LoadMetrics(baselinePath);
        var current = LoadMetrics(currentPath);

        var rows = new List<MetricComparison>(keys.Length);
        var violations = new List<MetricComparison>();

        foreach (var key in keys)
        {
            var hasBaseline = baseline.TryGetValue(key, out var baselineValue);
            var hasCurrent = current.TryGetValue(key, out var currentValue);

            // Missing-on-either-side means the gate has no signal - record it as
            // an "unknown" row so the caller can see what was skipped, but it does
            // not count as a regression. The most common case is the first run
            // against a baseline that pre-dates the metric.
            if (!hasBaseline || !hasCurrent)
            {
                rows.Add(new MetricComparison(
                    key,
                    hasBaseline ? baselineValue : null,
                    hasCurrent ? currentValue : null,
                    null,
                    IsHigherBetter(key),
                    false,
                    "missing"));
                continue;
            }

            var direction = IsHigherBetter(key);
            var deltaPct = ((currentValue - baselineValue) / baselineValue) * 100d;
            // For lower-is-better metrics, a positive delta is bad (got slower).
            // For higher-is-better metrics, a negative delta is bad (got slower throughput).
            var badPct = direction ? -deltaPct : deltaPct;
            var isRegression = badPct > tolerancePct;
            var row = new MetricComparison(key, baselineValue, currentValue, deltaPct, direction, isRegression, isRegression ? "regression" : "ok");
            rows.Add(row);
            if (isRegression)
            {
                violations.Add(row);
            }
        }

        return new RegressionReport(baselinePath, currentPath, tolerancePct, rows, violations);
    }

    private static bool IsHigherBetter(string metricKey) =>
        metricKey.EndsWith("_per_second", StringComparison.Ordinal);

    private static Dictionary<string, double> LoadMetrics(string path)
    {
        var bag = new Dictionary<string, double>(StringComparer.Ordinal);
        if (!File.Exists(path))
        {
            return bag;
        }
        using var stream = File.OpenRead(path);
        using var doc = JsonDocument.Parse(stream);
        if (!doc.RootElement.TryGetProperty("metrics", out var metrics) || metrics.ValueKind != JsonValueKind.Object)
        {
            return bag;
        }
        foreach (var prop in metrics.EnumerateObject())
        {
            if (prop.Value.ValueKind == JsonValueKind.Number && prop.Value.TryGetDouble(out var v))
            {
                bag[prop.Name] = v;
            }
        }
        return bag;
    }

    /// <summary>
    /// Renders a one-row-per-metric console summary suitable for CI logs.
    /// </summary>
    public static string Render(RegressionReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        var sb = new System.Text.StringBuilder();
        sb.AppendLine($"[regression-gate] baseline={report.BaselinePath}");
        sb.AppendLine($"[regression-gate] current ={report.CurrentPath}");
        sb.AppendLine($"[regression-gate] tolerance={report.TolerancePct:0.##}%");
        foreach (var row in report.Rows)
        {
            var arrow = row.IsHigherBetter ? "↑better" : "↓better";
            var deltaStr = row.DeltaPct.HasValue
                ? row.DeltaPct.Value.ToString("+0.##;-0.##;0", CultureInfo.InvariantCulture) + "%"
                : "n/a";
            sb.AppendLine($"  [{row.Status,-11}] {row.Key,-60} {arrow}  baseline={Format(row.BaselineValue)}  current={Format(row.CurrentValue)}  delta={deltaStr}");
        }
        sb.AppendLine($"[regression-gate] {report.Violations.Count} violation(s) of {report.Rows.Count} gated metric(s).");
        return sb.ToString();
    }

    private static string Format(double? v) =>
        v is null ? "n/a" : v.Value.ToString("0.##", CultureInfo.InvariantCulture);
}

/// <summary>
/// Result of <see cref="RegressionGate.Compare(string, string, double, IReadOnlyCollection{string}?)"/>.
/// </summary>
/// <param name="BaselinePath">Path of the baseline file consumed.</param>
/// <param name="CurrentPath">Path of the current-run file consumed.</param>
/// <param name="TolerancePct">Tolerance band the comparison was evaluated under.</param>
/// <param name="Rows">One row per gated metric - including rows skipped because the metric was missing on either side.</param>
/// <param name="Violations">Subset of <paramref name="Rows"/> whose status is "regression".</param>
internal sealed record RegressionReport(
    string BaselinePath,
    string CurrentPath,
    double TolerancePct,
    IReadOnlyList<MetricComparison> Rows,
    IReadOnlyList<MetricComparison> Violations);

/// <summary>
/// One row of a <see cref="RegressionReport"/> - a single metric's baseline vs current values plus the verdict.
/// </summary>
/// <param name="Key">The gated metric key (e.g. <c>microbench_point_read_p99_ns</c>).</param>
/// <param name="BaselineValue">Baseline value, or null if the metric was missing in the baseline file.</param>
/// <param name="CurrentValue">Current-run value, or null if the metric was missing in the current file.</param>
/// <param name="DeltaPct">Percentage change current vs baseline, or null if either side was missing.</param>
/// <param name="IsHigherBetter">True for throughput-style metrics (suffix <c>_per_second</c>); false for latency/allocation metrics.</param>
/// <param name="IsRegression">True when the move exceeds the tolerance band in the "bad" direction.</param>
/// <param name="Status">Stable status string: <c>ok</c>, <c>regression</c>, or <c>missing</c>.</param>
internal readonly record struct MetricComparison(
    string Key,
    double? BaselineValue,
    double? CurrentValue,
    double? DeltaPct,
    bool IsHigherBetter,
    bool IsRegression,
    string Status);
