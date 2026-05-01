using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Toolchains.InProcess.Emit;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// BenchmarkDotNet <see cref="IConfig"/> tuned for the Orleans.Lattice harness.
/// <para>
/// <b>Toolchain.</b> Defaults to <see cref="InProcessEmitToolchain"/> so the BDN
/// runner does not fork a child process per <c>[Benchmark]</c> &mdash; spawning
/// a child <c>.exe</c> would re-pay the ~5s Orleans cluster startup cost five
/// times. With the in-process toolchain the cluster comes up once in
/// <see cref="LatticeMicroBenchmarks.GlobalSetup"/> and serves all five workloads.
/// Set <c>BENCH_MICROBENCH_FIDELITY=full</c> in the environment to switch to the
/// default forking toolchain when methodology rigour outranks wall-clock budget.
/// </para>
/// <para>
/// <b>Job.</b> <see cref="Job.ShortRun"/> by default to keep microbench in the same
/// 2-3 minute envelope as the other scenarios. <c>BENCH_MICROBENCH_FIDELITY=full</c>
/// also widens to <see cref="Job.Default"/> for higher-confidence statistics.
/// </para>
/// </summary>
internal sealed class HarnessConfig : ManualConfig
{
    public HarnessConfig(string resultsJsonPath)
    {
        var fidelity = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_FIDELITY") ?? "quick";
        var fast = !string.Equals(fidelity, "full", StringComparison.OrdinalIgnoreCase);

        var job = fast ? Job.ShortRun : Job.Default;
        if (fast)
        {
            // Keep the cluster alive for the full BDN session by emitting all
            // workloads inside one process. Loses some JIT-isolation guarantees
            // but the trend dashboard cares about run-over-run delta, not absolute
            // single-digit ns precision.
            job = job.WithToolchain(InProcessEmitToolchain.Instance);
        }

        AddJob(job);

        AddDiagnoser(MemoryDiagnoser.Default);
        AddDiagnoser(ThreadingDiagnoser.Default);

        AddLogger(ConsoleLogger.Default);

        // Keep BDN's standard exporters around so the BenchmarkDotNet.Artifacts/
        // tree still has the human-readable markdown report. The harness also adds
        // its own JSON exporter that writes the harness-shaped results.json that
        // benchmark.ps1 / the cockpit dashboard consume.
        AddExporter(MarkdownExporter.GitHub);
        AddExporter(JsonExporter.FullCompressed);
        AddExporter(new HarnessJsonExporter(resultsJsonPath));

        // Surface every column BDN computes; the harness exporter pulls the
        // numbers it cares about from `summary.Reports[*].ResultStatistics`.
        AddColumnProvider(DefaultColumnProviders.Instance);

        // Run all benchmarks regardless of platform; keep stderr quiet on macOS/Linux.
        WithOptions(ConfigOptions.DisableLogFile | ConfigOptions.JoinSummary);
    }
}
