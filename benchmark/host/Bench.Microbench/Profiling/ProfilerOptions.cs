namespace Orleans.Lattice.Benchmark.Microbench.Profiling;

/// <summary>
/// Selects which event categories the EventPipe-backed
/// <see cref="BenchmarkProfiler"/> captures during a run.
/// </summary>
public enum ProfileMode
{
    /// <summary>Profiling disabled (default). No EventPipe session is opened.</summary>
    Off = 0,

    /// <summary>
    /// Capture sampled managed-object allocations
    /// (<c>GCSampledObjectAllocationHigh</c>). Per-method bytes-allocated
    /// attribution is the primary output.
    /// </summary>
    Alloc = 1,

    /// <summary>
    /// Capture managed CPU samples
    /// (<c>Microsoft-DotNETCore-SampleProfiler</c>). Per-method sample-count
    /// attribution is the primary output.
    /// </summary>
    Cpu = 2,

    /// <summary>Capture both allocation events and CPU samples.</summary>
    Both = 3,
}

/// <summary>
/// User-tunable options for the <see cref="BenchmarkProfiler"/> EventPipe
/// session. Resolved from <c>BENCH_MICROBENCH_PROFILE*</c> environment
/// variables; see <see cref="FromEnvironment"/>.
/// </summary>
/// <remarks>
/// Profiling perturbs the measured workload (the EventPipe session adds per-event
/// kernel-mode IO and stack walking). The harness <c>results.json</c> produced
/// by a profile-enabled run is not directly comparable to a profile-disabled
/// cohort; treat the run as a diagnostic snapshot, not a performance baseline.
/// </remarks>
/// <param name="Mode">Which event categories to capture.</param>
/// <param name="TopN">
/// Maximum number of rows to emit in the <c>top_allocators</c> /
/// <c>top_cpu</c> arrays of <c>profile.json</c>. Defaults to 50.
/// </param>
/// <param name="NetTraceOutputPath">
/// Optional filesystem path that receives a raw <c>.nettrace</c> sidecar in
/// addition to the aggregated <c>profile.json</c>. Useful for offline analysis
/// in PerfView / dotnet-trace. <see langword="null"/> disables the sidecar.
/// </param>
/// <param name="FilterNoiseFrames">
/// When <see langword="true"/> (the default), the symbolicator skips
/// measurement-substrate frames (NSubstitute / Castle mock thunks, the BDN
/// engine, async-builder plumbing) and attributes each event to the nearest
/// <em>product</em> frame instead - see <see cref="FrameFilter.IsProductFrame"/>.
/// Set to <see langword="false"/> via
/// <c>BENCH_MICROBENCH_PROFILE_FILTER_NOISE=false</c> to attribute to the
/// deepest named managed frame regardless of classification (the legacy
/// behaviour, useful when diagnosing the harness itself).
/// </param>
public readonly record struct ProfilerOptions(
    ProfileMode Mode,
    int TopN,
    string? NetTraceOutputPath,
    bool FilterNoiseFrames = true)
{
    /// <summary>Default <see cref="TopN"/> when the env var is unset or unparseable.</summary>
    public const int DefaultTopN = 50;

    /// <summary>Sentinel "no profiling" options instance.</summary>
    public static ProfilerOptions Disabled { get; } = new(ProfileMode.Off, DefaultTopN, null);

    /// <summary>
    /// Resolves options from process environment variables, with safe fallbacks:
    /// <list type="bullet">
    ///   <item><c>BENCH_MICROBENCH_PROFILE</c> = <c>off</c> | <c>alloc</c> | <c>cpu</c> | <c>both</c> (default <c>off</c>; unknown values fall back to <c>off</c> with a stderr warning).</item>
    ///   <item><c>BENCH_MICROBENCH_PROFILE_TOPN</c> = positive integer (default <see cref="DefaultTopN"/>; non-positive or unparseable falls back to default).</item>
    ///   <item><c>BENCH_MICROBENCH_PROFILE_NETTRACE_PATH</c> = filesystem path or empty.</item>
    ///   <item><c>BENCH_MICROBENCH_PROFILE_FILTER_NOISE</c> = <c>true</c> (default) | <c>false</c>; when false, attributes to the deepest named managed frame regardless of mock/engine classification.</item>
    /// </list>
    /// </summary>
    public static ProfilerOptions FromEnvironment()
    {
        var modeRaw = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_PROFILE");
        var mode = ParseMode(modeRaw);

        var topNRaw = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_TOPN");
        var topN = DefaultTopN;
        if (!string.IsNullOrWhiteSpace(topNRaw)
            && int.TryParse(topNRaw, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var parsedTopN)
            && parsedTopN > 0)
        {
            topN = parsedTopN;
        }

        var nettrace = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_NETTRACE_PATH");
        if (string.IsNullOrWhiteSpace(nettrace))
        {
            nettrace = null;
        }

        var filterRaw = Environment.GetEnvironmentVariable("BENCH_MICROBENCH_PROFILE_FILTER_NOISE");
        var filterNoise = ParseFilterNoise(filterRaw);

        return new ProfilerOptions(mode, topN, nettrace, filterNoise);
    }

    /// <summary>
    /// Parses the <c>BENCH_MICROBENCH_PROFILE_FILTER_NOISE</c> toggle.
    /// Defaults to <see langword="true"/> (filtering on) when unset; recognises
    /// <c>false</c> / <c>0</c> / <c>no</c> / <c>off</c> (case-insensitive) as
    /// the opt-out. Any other value falls back to the on default.
    /// </summary>
    public static bool ParseFilterNoise(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return true;
        }
        return raw.Trim().ToLowerInvariant() switch
        {
            "false" or "0" or "no" or "off" => false,
            _ => true,
        };
    }

    /// <summary>
    /// Parses a <see cref="ProfileMode"/> from its case-insensitive string form.
    /// Unknown / empty / null inputs return <see cref="ProfileMode.Off"/>; non-empty
    /// unknowns additionally write a one-line warning to stderr so a typo in the
    /// scenario env file is not silently swallowed.
    /// </summary>
    public static ProfileMode ParseMode(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return ProfileMode.Off;
        }
        return raw.Trim().ToLowerInvariant() switch
        {
            "off" or "false" or "0" or "no" => ProfileMode.Off,
            "alloc" or "allocation" or "allocations" => ProfileMode.Alloc,
            "cpu" or "sample" or "samples" => ProfileMode.Cpu,
            "both" or "all" => ProfileMode.Both,
            _ => WarnUnknown(raw),
        };

        static ProfileMode WarnUnknown(string raw)
        {
            Console.Error.WriteLine(
                $"[microbench] unknown BENCH_MICROBENCH_PROFILE value '{raw}'; falling back to 'off'.");
            return ProfileMode.Off;
        }
    }

    /// <summary>True when the resolved <see cref="Mode"/> would open an EventPipe session.</summary>
    public bool IsEnabled => Mode != ProfileMode.Off;

    /// <summary>True when allocation events are part of the resolved <see cref="Mode"/>.</summary>
    public bool CapturesAllocations => Mode is ProfileMode.Alloc or ProfileMode.Both;

    /// <summary>True when CPU samples are part of the resolved <see cref="Mode"/>.</summary>
    public bool CapturesCpu => Mode is ProfileMode.Cpu or ProfileMode.Both;
}
