namespace Orleans.Lattice.Benchmark.Microbench.Profiling;

/// <summary>
/// Lifecycle wrapper around an <see cref="EventPipeProfilerSession"/> and its
/// backing <see cref="ProfileAggregator"/>. Resolves the resolved
/// <see cref="ProfilerOptions"/> from environment, opens the session, and on
/// <see cref="Stop"/> writes the final <c>profile.json</c> sidecar next to
/// the run's <c>results.json</c>.
/// </summary>
/// <remarks>
/// The harness uses the static <see cref="TryStart"/> factory from
/// <c>[GlobalSetup]</c> after seeding completes, and calls <see cref="Stop"/>
/// from a new <c>[GlobalCleanup]</c> method. When profiling is disabled (the
/// default), <see cref="TryStart"/> returns <see langword="null"/> and the
/// harness path is a complete no-op.
/// </remarks>
public sealed class BenchmarkProfiler : IDisposable
{
    private readonly ProfilerOptions _options;
    private readonly ProfileAggregator _aggregator;
    private readonly EventPipeProfilerSession _session;
    private readonly string _outputPath;
    private readonly string _runId;
    private readonly string _gitSha;
    private readonly System.Diagnostics.Stopwatch _stopwatch;
    private int _stopped;

    private BenchmarkProfiler(
        ProfilerOptions options,
        ProfileAggregator aggregator,
        EventPipeProfilerSession session,
        string outputPath,
        string runId,
        string gitSha)
    {
        _options = options;
        _aggregator = aggregator;
        _session = session;
        _outputPath = outputPath;
        _runId = runId;
        _gitSha = gitSha;
        _stopwatch = System.Diagnostics.Stopwatch.StartNew();
    }

    /// <summary>The output path that <see cref="Stop"/> will write the JSON report to.</summary>
    public string OutputPath => _outputPath;

    /// <summary>The resolved options the session was started with.</summary>
    public ProfilerOptions Options => _options;

    /// <summary>
    /// Resolves <see cref="ProfilerOptions"/> from environment, refuses to
    /// start on incompatible BDN fidelity (<c>full</c> = forking toolchain),
    /// opens an EventPipe session, and returns a live <see cref="BenchmarkProfiler"/>.
    /// Returns <see langword="null"/> when profiling is disabled, refused, or
    /// the EventPipe session could not be opened.
    /// </summary>
    /// <param name="profileJsonOutputPath">Filesystem path the report will be written to on <see cref="Stop"/>.</param>
    /// <param name="runId">Mirrors <c>BENCH_RUN_ID</c>.</param>
    /// <param name="gitSha">Mirrors <c>BENCH_GIT_SHA</c>.</param>
    public static BenchmarkProfiler? TryStart(string profileJsonOutputPath, string runId, string gitSha)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profileJsonOutputPath);
        ArgumentNullException.ThrowIfNull(runId);
        ArgumentNullException.ThrowIfNull(gitSha);

        var options = ProfilerOptions.FromEnvironment();
        if (!options.IsEnabled)
        {
            return null;
        }

        var fidelity = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_FIDELITY");
        if (string.Equals(fidelity, "full", StringComparison.OrdinalIgnoreCase))
        {
            Console.Error.WriteLine(
                "[microbench] profiler refused: BENCH_MICROBENCH_FIDELITY=full uses the forking BDN toolchain, "
                + "and the EventPipe session must live in the same process as the workload. "
                + "Re-run with BENCH_MICROBENCH_FIDELITY=quick (or dry) when profiling.");
            return null;
        }

        var aggregator = new ProfileAggregator();
        var session = EventPipeProfilerSession.TryStart(options, aggregator);
        if (session is null)
        {
            return null;
        }

        Console.WriteLine(
            $"[microbench] profiler started (mode={options.Mode.ToString().ToLowerInvariant()}, topN={options.TopN}, filterNoise={options.FilterNoiseFrames.ToString().ToLowerInvariant()})"
            + (string.IsNullOrEmpty(options.NetTraceOutputPath) ? string.Empty : $", nettrace={options.NetTraceOutputPath}"));

        return new BenchmarkProfiler(options, aggregator, session, profileJsonOutputPath, runId, gitSha);
    }

    /// <summary>
    /// Stops the EventPipe session, post-processes captured events into
    /// per-method aggregates, and writes <c>profile.json</c> to
    /// <see cref="OutputPath"/>. Idempotent.
    /// </summary>
    public void Stop()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }
        try
        {
            _session.Dispose();
            _stopwatch.Stop();

            var report = _aggregator.Build(
                runId: _runId,
                gitSha: _gitSha,
                mode: _options.Mode,
                duration: _stopwatch.Elapsed,
                topN: _options.TopN);
            report.WriteJson(_outputPath);
            Console.WriteLine(
                $"[microbench] profile written -> {_outputPath} "
                + $"(allocators={report.TopAllocators.Count}, cpu_frames={report.TopCpu.Count}, "
                + $"total_alloc_b={report.TotalAllocationsB}, total_samples={report.TotalCpuSamples})");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[microbench] profiler stop failed: {ex.GetType().Name}: {ex.Message}");
        }
    }

    /// <inheritdoc/>
    public void Dispose() => Stop();

    /// <summary>
    /// Test-only entrypoint: builds a profiler that is fully wired against a
    /// live EventPipe session but with an explicit options override
    /// (bypassing environment-variable parsing). Used by the
    /// <c>BenchmarkProfilerSmokeTests</c> integration check.
    /// </summary>
    /// <param name="options">Resolved options (must have <see cref="ProfilerOptions.IsEnabled"/> = true).</param>
    /// <param name="profileJsonOutputPath">Filesystem path the report will be written to on <see cref="Stop"/>.</param>
    /// <param name="runId">Run identifier embedded in the report.</param>
    /// <param name="gitSha">Git SHA embedded in the report.</param>
    public static BenchmarkProfiler? StartForTesting(
        ProfilerOptions options,
        string profileJsonOutputPath,
        string runId,
        string gitSha)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profileJsonOutputPath);
        ArgumentNullException.ThrowIfNull(runId);
        ArgumentNullException.ThrowIfNull(gitSha);
        if (!options.IsEnabled)
        {
            return null;
        }

        var aggregator = new ProfileAggregator();
        var session = EventPipeProfilerSession.TryStart(options, aggregator);
        if (session is null)
        {
            return null;
        }
        return new BenchmarkProfiler(options, aggregator, session, profileJsonOutputPath, runId, gitSha);
    }
}
