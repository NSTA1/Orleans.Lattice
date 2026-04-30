using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Running;
using Orleans.Lattice.Benchmark.Microbench;

// CLI:  --results <path>   write the harness-shaped results.json here
//       --filter  <glob>   pass-through to BDN's --filter (multiple comma-separated globs)
//
// Env-var fallbacks (so benchmark.ps1 doesn't have to assemble a CLI):
//   BENCH_RESULTS_PATH         → --results
//   BENCH_MICROBENCH_WORKLOADS → --filter (comma list of method names; '*' wildcards apply)

string? resultsPath = null;
string? filterRaw = null;

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
    resultsPath = Path.Combine(".run", "B-02", runId, "results.json");
}
resultsPath = Path.GetFullPath(resultsPath);

filterRaw ??= Environment.GetEnvironmentVariable("BENCH_MICROBENCH_WORKLOADS");

Console.WriteLine($"[microbench] results -> {resultsPath}");
if (!string.IsNullOrWhiteSpace(filterRaw))
{
    Console.WriteLine($"[microbench] filter  -> {filterRaw}");
}

var config = (IConfig)new HarnessConfig(resultsPath);

if (!string.IsNullOrWhiteSpace(filterRaw))
{
    // Map a comma-separated list of method names to a BDN GlobFilter so we can
    // run a subset (e.g. "PointRead,RangeScan").
    var globs = filterRaw
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(name => $"*{name}*")
        .ToArray();
    config = config.AddFilter(new GlobFilter(globs));
}

var summary = BenchmarkRunner.Run<LatticeMicroBenchmarks>(config);
return summary.HasCriticalValidationErrors ? 1 : 0;
