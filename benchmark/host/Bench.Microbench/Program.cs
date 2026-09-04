using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Running;
using Orleans.Lattice.Benchmark.Microbench;

// CLI:  --results <path>   write the harness-shaped results.json here
//       --filter  <glob>   pass-through to BDN's --filter (multiple comma-separated globs)
//       --baseline <path>  optional baseline results.json for regression-gate comparison
//                          (defaults to baseline-v3.4.0.json next to the assembly when
//                          BENCH_REGRESSION_GATE_ENABLED=true)
//       --tolerance <pct>  regression tolerance band in percent (default: 10)
//
// Env-var fallbacks (so benchmark.ps1 doesn't have to assemble a CLI):
//   BENCH_RESULTS_PATH         → --results
//   BENCH_MICROBENCH_WORKLOADS → --filter (comma list of method names; '*' wildcards apply)
//   BENCH_BASELINE_PATH        → --baseline
//   BENCH_REGRESSION_TOLERANCE → --tolerance
//   BENCH_REGRESSION_GATE_ENABLED=true enables the gate even when no --baseline is set
//                                       (resolves to the bundled baseline-v3.4.0.json)

string? resultsPath = null;
string? filterRaw = null;
string? baselinePath = null;
string? toleranceRaw = null;

for (var i = 0; i < args.Length; i++)
{
    switch (args[i])
    {
        case "--results":
        case "-r":
            resultsPath = args[++i];
            break;
        case "--filter":
        case "-f":
            filterRaw = args[++i];
            break;
        case "--baseline":
            baselinePath = args[++i];
            break;
        case "--tolerance":
            toleranceRaw = args[++i];
            break;
        default:
            // Unknown args are forwarded to BDN's argument parser later. Keeping
            // the simple parse loop limited to the harness-specific switches.
            break;
    }
}

resultsPath ??= Environment.GetEnvironmentVariable("BENCH_RESULTS_PATH");
if (string.IsNullOrWhiteSpace(resultsPath))
{
    var runId = DateTime.UtcNow.ToString("yyyy-MM-ddTHH-mm-ssZ");
    // Default fallback: write into the canonical benchmark/.run/microbench
    // tree that benchmark.ps1 (and .gitignore) both target. Resolved
    // relative to the benchmark project root (walking up from the
    // executing assembly) rather than the current working directory, so
    // ad-hoc `dotnet run --project ...` invocations from the repo root
    // don't drop a sibling `.run/` directory outside the gitignored area.
    var projectRoot = LocateBenchmarkRoot()
        ?? Path.GetFullPath("benchmark");
    resultsPath = Path.Combine(projectRoot, ".run", "microbench", runId, "results.json");
}
resultsPath = Path.GetFullPath(resultsPath);

filterRaw ??= Environment.GetEnvironmentVariable("BENCH_MICROBENCH_WORKLOADS");

Console.WriteLine($"[microbench] results -> {resultsPath}");
if (!string.IsNullOrWhiteSpace(filterRaw))
{
    Console.WriteLine($"[microbench] filter  -> {filterRaw}");
}

var config = (IConfig)new HarnessConfig(resultsPath);

// Opt-in alternate suite: the producer-side commit-observer microbench
// (ReplicationCommitObserverBenchmarks) runs only when explicitly selected
// via BENCH_MICROBENCH_SUITE=observer (or --suite observer). The default
// path is unchanged so CI / the trend dashboard keep running the main
// LatticeMicroBenchmarks suite.
//
// Recognised suites: observer, authdecision, hotpath, hashalloc, rowcodec, ordedup, mergefold, catalog, fanout, crosstree, alloctrims, viewdrain, aggiter, viewmaint, queryproj, readpathtrims, readpathpresize, draintrims, fusiontrims.
var suite = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_SUITE");
for (var i = 0; i < args.Length - 1; i++)
{
    if (args[i] == "--suite") { suite = args[i + 1]; break; }
}

if (string.Equals(suite, "observer", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> observer (ReplicationCommitObserverBenchmarks)");
    var observerSummary = BenchmarkRunner.Run<ReplicationCommitObserverBenchmarks>(config);
    return observerSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (!string.IsNullOrWhiteSpace(filterRaw))
{
    // Map a comma-separated list of method names to a BDN GlobFilter so we can
    // run a subset (e.g. "PointRead,Mixed_70R_30W").
    var globs = filterRaw
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(name => $"*{name}*")
        .ToArray();
    config = config.AddFilter(new GlobFilter(globs));
}

if (string.Equals(suite, "authdecision", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> authdecision (AuthDecisionBenchmarks)");
    var authSummary = BenchmarkRunner.Run<AuthDecisionBenchmarks>(config);
    return authSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "hotpath", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> hotpath (HotPathAllocationBenchmarks)");
    var hotpathSummary = BenchmarkRunner.Run<HotPathAllocationBenchmarks>(config);
    return hotpathSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "hashalloc", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> hashalloc (HashingAllocationBenchmarks)");
    var hashAllocSummary = BenchmarkRunner.Run<HashingAllocationBenchmarks>(config);
    return hashAllocSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "rowcodec", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> rowcodec (RowCodecBenchmarks)");
    var rowCodecSummary = BenchmarkRunner.Run<RowCodecBenchmarks>(config);
    return rowCodecSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "ordedup", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> ordedup (OrCrdtReconcileBenchmarks)");
    var orDedupSummary = BenchmarkRunner.Run<OrCrdtReconcileBenchmarks>(config);
    return orDedupSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "mergefold", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> mergefold (CrdtMergeFoldBenchmarks)");
    var mergeFoldSummary = BenchmarkRunner.Run<CrdtMergeFoldBenchmarks>(config);
    return mergeFoldSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "fanout", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> fanout (FanOutReductionBenchmarks)");

    // The round-trip census runs first and unconditionally: it is exact and
    // deterministic (no host, core-count, or scheduler dependence), so it needs
    // no BenchmarkDotNet job, and it is the figure the batching change actually
    // targets. Set BENCH_FANOUT_ROUNDTRIPS_ONLY=true to stop after it when only
    // the hop counts are wanted.
    var fanoutCensus = FanOutRoundTripReport.MeasureAsync().GetAwaiter().GetResult();
    Console.Write(FanOutRoundTripReport.Render(fanoutCensus));
    FanOutRoundTripReport.Write(fanoutCensus, resultsPath);

    if (string.Equals(
            Environment.GetEnvironmentVariable("BENCH_FANOUT_ROUNDTRIPS_ONLY"),
            "true",
            StringComparison.OrdinalIgnoreCase))
    {
        Console.WriteLine("[fanout] BENCH_FANOUT_ROUNDTRIPS_ONLY=true - skipping the latency suite.");
        return 0;
    }

    var fanoutSummary = BenchmarkRunner.Run<FanOutReductionBenchmarks>(config);
    return fanoutSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "catalog", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> catalog (CatalogEnumerationBenchmarks)");

    // The round-trip census runs first and unconditionally: it is exact and
    // deterministic (no host, core-count, or scheduler dependence), so it needs
    // no BenchmarkDotNet job, and it is the figure the batching change actually
    // targets. Set BENCH_CATALOG_ROUNDTRIPS_ONLY=true to stop after it when only
    // the hop counts are wanted.
    var census = CatalogRoundTripReport.MeasureAsync().GetAwaiter().GetResult();
    Console.Write(CatalogRoundTripReport.Render(census));
    CatalogRoundTripReport.Write(census, resultsPath);

    if (string.Equals(
            Environment.GetEnvironmentVariable("BENCH_CATALOG_ROUNDTRIPS_ONLY"),
            "true",
            StringComparison.OrdinalIgnoreCase))
    {
        Console.WriteLine("[catalog] BENCH_CATALOG_ROUNDTRIPS_ONLY=true - skipping the latency suite.");
        return 0;
    }

    var catalogSummary = BenchmarkRunner.Run<CatalogEnumerationBenchmarks>(config);
    return catalogSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "crosstree", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> crosstree (CrossTreeCanonicalSetBenchmarks)");
    var crossTreeSummary = BenchmarkRunner.Run<CrossTreeCanonicalSetBenchmarks>(config);
    return crossTreeSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "alloctrims", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> alloctrims (DictionaryAllocationTrimBenchmarks)");
    var allocTrimsSummary = BenchmarkRunner.Run<DictionaryAllocationTrimBenchmarks>(config);
    return allocTrimsSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "viewdrain", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> viewdrain (ViewDrainClassificationBenchmarks)");
    var viewDrainSummary = BenchmarkRunner.Run<ViewDrainClassificationBenchmarks>(config);
    return viewDrainSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "aggiter", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> aggiter (AggregationDictIterationBenchmarks)");
    var aggIterSummary = BenchmarkRunner.Run<AggregationDictIterationBenchmarks>(config);
    return aggIterSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "viewmaint", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> viewmaint (ViewMaintainerAllocationBenchmarks)");
    var viewMaintSummary = BenchmarkRunner.Run<ViewMaintainerAllocationBenchmarks>(config);
    return viewMaintSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "queryproj", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> queryproj (QueryProjectionAllocationBenchmarks)");
    var queryProjSummary = BenchmarkRunner.Run<QueryProjectionAllocationBenchmarks>(config);
    return queryProjSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "readpathtrims", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> readpathtrims (ReadPathAllocationBenchmarks)");
    var readPathSummary = BenchmarkRunner.Run<ReadPathAllocationBenchmarks>(config);
    return readPathSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "readpathpresize", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> readpathpresize (ReadPathPresizeBenchmarks)");
    var readPathPresizeSummary = BenchmarkRunner.Run<ReadPathPresizeBenchmarks>(config);
    return readPathPresizeSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "draintrims", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> draintrims (DrainAllocationTrimBenchmarks)");
    var drainTrimsSummary = BenchmarkRunner.Run<DrainAllocationTrimBenchmarks>(config);
    return drainTrimsSummary.HasCriticalValidationErrors ? 1 : 0;
}

if (string.Equals(suite, "fusiontrims", StringComparison.OrdinalIgnoreCase))
{
    Console.WriteLine("[microbench] suite   -> fusiontrims (ViewFoldAndMetricsTrimBenchmarks)");
    var fusionTrimsSummary = BenchmarkRunner.Run<ViewFoldAndMetricsTrimBenchmarks>(config);
    return fusionTrimsSummary.HasCriticalValidationErrors ? 1 : 0;
}

var summary = BenchmarkRunner.Run<LatticeMicroBenchmarks>(config);
var bdnExitCode = summary.HasCriticalValidationErrors ? 1 : 0;

// Regression-gate pass: if a baseline is configured (explicitly via --baseline /
// BENCH_BASELINE_PATH, or implicitly via BENCH_REGRESSION_GATE_ENABLED=true which
// resolves to the bundled baseline-v3.4.0.json next to the assembly), compare the
// freshly-written results.json against it and OR-combine the violation count
// into the process exit code.
baselinePath ??= Environment.GetEnvironmentVariable("BENCH_BASELINE_PATH");
toleranceRaw ??= Environment.GetEnvironmentVariable("BENCH_REGRESSION_TOLERANCE");
var gateRequested = !string.IsNullOrWhiteSpace(baselinePath)
    || string.Equals(Environment.GetEnvironmentVariable("BENCH_REGRESSION_GATE_ENABLED"), "true", StringComparison.OrdinalIgnoreCase);
if (gateRequested && !summary.HasCriticalValidationErrors)
{
    if (string.IsNullOrWhiteSpace(baselinePath))
    {
        baselinePath = Path.Combine(AppContext.BaseDirectory, "baseline-v3.4.0.json");
    }
    var tolerance = 10d;
    if (!string.IsNullOrWhiteSpace(toleranceRaw)
        && double.TryParse(toleranceRaw, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var t))
    {
        tolerance = t;
    }

    if (!File.Exists(baselinePath))
    {
        Console.WriteLine($"[regression-gate] baseline file not found: {baselinePath} - skipping gate.");
    }
    else if (!File.Exists(resultsPath))
    {
        Console.WriteLine($"[regression-gate] current results not found: {resultsPath} - skipping gate.");
    }
    else
    {
        var report = RegressionGate.Compare(baselinePath, resultsPath, tolerance);
        Console.Write(RegressionGate.Render(report));
        if (report.Violations.Count > 0)
        {
            // OR-combine with BDN's own exit code so a regression is fail-fast in CI
            // even when the BDN run itself reports clean.
            bdnExitCode |= 2;
        }
    }
}

return bdnExitCode;

// Local-function helper for the top-level fallback path resolution above.
// Walks upward from AppContext.BaseDirectory looking for the canonical
// `benchmark/` folder shipped at the repo root. Returns its full path,
// or null if not found (which only happens when the assembly has been
// published or moved out of the source tree, in which case the caller
// falls back to a CWD-relative path). Keeps ad-hoc `dotnet run --project
// benchmark/host/Bench.Microbench` invocations writing into the same
// gitignored `benchmark/.run/microbench/` location that benchmark.ps1
// uses, instead of dropping a sibling `.run/` at the repo root.
static string? LocateBenchmarkRoot()
{
    // BaseDirectory points at .../benchmark/host/Bench.Microbench/bin/<config>/<tfm>/.
    // Walk up looking for a directory whose name is exactly "benchmark"
    // and whose parent contains a sibling "src" folder (the repo
    // structure). The walk has a small upper bound so a misplaced
    // assembly cannot loop indefinitely.
    var dir = new DirectoryInfo(AppContext.BaseDirectory);
    for (var i = 0; i < 16 && dir is not null; i++, dir = dir.Parent)
    {
        if (string.Equals(dir.Name, "benchmark", StringComparison.OrdinalIgnoreCase)
            && dir.Parent is { } parent
            && Directory.Exists(Path.Combine(parent.FullName, "src")))
        {
            return dir.FullName;
        }
    }
    return null;
}
