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
/// <b>Job.</b> Three fidelity levels are recognised via <c>BENCH_MICROBENCH_FIDELITY</c>:
/// <list type="bullet">
///   <item><c>dry</c> &mdash; <see cref="Job.Dry"/> + <see cref="InProcessEmitToolchain"/>.
///     1 warmup, 1 measurement, single iteration. Use for fast smoke-test runs and for
///     optimisation cohorts where the n=3 cohort-average already provides the statistical
///     guard. Per-method wall time drops by roughly an order of magnitude vs <c>quick</c>.</item>
///   <item><c>quick</c> &mdash; <see cref="Job.ShortRun"/> + <see cref="InProcessEmitToolchain"/>.
///     Default. 1 launch, 3 warmup, 3 measurement iterations. Standard cohort fidelity.</item>
///   <item><c>full</c> &mdash; <see cref="Job.Default"/> + default forking toolchain.
///     Gold-standard rigour; ~30+ minutes per run.</item>
/// </list>
/// </para>
/// </summary>
internal sealed class HarnessConfig : ManualConfig
{
    public HarnessConfig(string resultsJsonPath)
    {
        var fidelity = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_FIDELITY") ?? "quick";

        Job job;
        if (string.Equals(fidelity, "full", StringComparison.OrdinalIgnoreCase))
        {
            // Forking toolchain + Job.Default for gold-standard rigour.
            job = Job.Default;
        }
        else if (string.Equals(fidelity, "dry", StringComparison.OrdinalIgnoreCase))
        {
            // Job.Dry: 1 warmup + 1 measurement iteration. Statistical noise per
            // single benchmark is high, so this fidelity is only meaningful when
            // the caller is averaging across an n>=3 cohort (the standard
            // optimisation-cycle pattern). In-process toolchain so the cluster
            // is not respawned per benchmark.
            job = Job.Dry.WithToolchain(InProcessEmitToolchain.Instance);
        }
        else if (string.Equals(fidelity, "quick-oop", StringComparison.OrdinalIgnoreCase))
        {
            // Job.ShortRun on the DEFAULT (out-of-process, forking) toolchain.
            // Same 3 warmup + 3 measured iterations as "quick", but each benchmark
            // runs in its own child process. This is the fidelity to use for the
            // gate-enabled configuration: enabling authorization adds a cold
            // first-call cost to the multi-shard fan-out operations (SetMany over
            // several shards, atomic and cross-tree writes, multi-shard scans) that
            // is large enough for BenchmarkDotNet's in-process toolchain to refuse
            // the benchmark with "takes too long to run". The out-of-process
            // toolchain has no such guard, so it measures those operations cleanly.
            // The tradeoff is a per-benchmark child-process + cluster startup cost.
            job = Job.ShortRun;
        }
        else
        {
            // Default "quick" path: Job.ShortRun + in-process toolchain.
            // Keeps the cluster alive for the full BDN session by emitting all
            // workloads inside one process. Loses some JIT-isolation guarantees
            // but the trend dashboard cares about run-over-run delta, not absolute
            // single-digit ns precision.
            job = Job.ShortRun.WithToolchain(InProcessEmitToolchain.Instance);
        }

        // Optional fixed invocation count. When BENCH_MICROBENCH_INVOCATIONS is set
        // to a positive integer, the pilot stage is skipped and every benchmark runs
        // exactly that many invocations per iteration. This is required for the
        // in-process toolchain when a benchmarked path is slow enough that BDN's pilot
        // would otherwise scale the invocation count into the toolchain's long-running
        // guard (which throws "takes too long to run"). Fixing the count keeps the
        // fast in-process toolchain, keeps the cluster alive for the whole session, and
        // gives disabled and enabled runs an identical invocation budget for a fair
        // per-operation delta. The value must be a multiple of the job's unroll factor
        // (16 for ShortRun/Default, 1 for Dry); 16384 is a safe default.
        var invocationsRaw = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_INVOCATIONS");
        if (int.TryParse(invocationsRaw, out var invocations) && invocations > 0)
        {
            job = job.WithInvocationCount(invocations);
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
