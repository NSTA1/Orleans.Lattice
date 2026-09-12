using System.Globalization;
using System.Runtime;
using Microsoft.Extensions.Configuration;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Reports the collector this process actually runs under, and says so at warning level
/// when the combination is one that suspends the whole process for minutes at a time.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this exists (issue #2596).</b> The container ran Workstation GC against a 12 GiB
/// memory limit at about 11 GiB resident. Workstation GC collects a single heap and its
/// blocking gen2 phases are effectively single-threaded, so a collection walks the whole
/// heap on one thread while every other thread in the process is suspended. The runtime
/// attributed a 252.2 second pause to garbage collection in one such stall, against a
/// 30 second Orleans request timeout.
/// </para>
/// <para>
/// <b>The remedy is narrower than the symptom, and this file does not overstate it.</b>
/// Server GC with a bounded heap count removes <i>this</i> class of pause: multi-minute,
/// process-wide, and attributed to the collector by the runtime itself. It is not a cure
/// for long stalls in general. Measurement of the same container found garbage collection
/// accounted for 31.7% of long-silence time and did not explain the largest timeout burst
/// at all, so a claim that this configuration fixes the container would be false in
/// advance. Naming a real defect precisely is the whole value here; attaching correct
/// evidence to a broader remedy than it supports is the failure this epic keeps repeating.
/// </para>
/// <para>
/// <b>Why the report and not the existing warning.</b> Orleans already logs
/// <c>Note: Silo not running with ServerGC turned on</c> at startup. It logged it through
/// two failed gate runs and nobody read it. The runtime was separately emitting per-event
/// pause attribution 172 times in one of those runs, while two readers of that same file
/// argued about whether collector pauses could be inferred from log silence. A true signal
/// in a channel nobody reads is the recurring defect of this whole epic, so these facts go
/// into the effective-configuration report, which is the operator-facing surface somebody
/// actually consults when they ask what a deployment is running.
/// </para>
/// <para>
/// This type is observability only: it reads and formats, changes no behaviour, validates
/// nothing, and cannot fail startup. A collector configuration this host disagrees with is
/// still one the operator is entitled to run.
/// </para>
/// </remarks>
public static class RepoContextGarbageCollection
{
    /// <summary>
    /// The environment variable selecting Server (<c>1</c>) or Workstation (<c>0</c>)
    /// garbage collection.
    /// </summary>
    public const string ServerGcKey = "DOTNET_gcServer";

    /// <summary>
    /// The environment variable pinning the Server GC heap count.
    /// </summary>
    /// <remarks>
    /// This is the variable that makes the fix possible at all. Server GC otherwise sizes
    /// its heap count from the processor count, and in this deployment the processor count
    /// is deliberately pinned above the CPU grant (to hold the WAL replay gate's permits),
    /// so enabling Server GC without this variable would create one heap per phantom
    /// processor. One variable was doing two jobs with opposite requirements; this one
    /// separates them.
    /// </remarks>
    public const string HeapCountKey = "DOTNET_GCHeapCount";

    /// <summary>The pseudo-key under which the resolved collector flavour is reported.</summary>
    public const string RuntimeModeKey = "GC.Mode";

    /// <summary>The pseudo-key under which the resolved heap count is reported.</summary>
    public const string RuntimeHeapCountKey = "GC.HeapCount";

    /// <summary>The pseudo-key under which the collector's memory ceiling is reported.</summary>
    public const string RuntimeMemoryLimitKey = "GC.TotalAvailableMemoryBytes";

    /// <summary>
    /// The pseudo-key under which the accumulated stop-the-world pause total is reported.
    /// </summary>
    /// <remarks>
    /// Reported at startup, where the total is necessarily near zero, because the value of
    /// the line is the <i>name of the measurand</i> rather than the figure. Two people
    /// spent two rounds building a proxy for collector pause time out of gaps between log
    /// timestamps while the runtime was emitting the quantity by name in the same file.
    /// Naming the API in the report is what stops the next reader doing it a third time.
    /// </remarks>
    public const string RuntimePauseTotalKey = "GC.GetTotalPauseDuration";

    /// <summary>The rendered value of <see cref="RuntimeModeKey"/> under Server GC.</summary>
    public const string ServerMode = "Server";

    /// <summary>The rendered value of <see cref="RuntimeModeKey"/> under Workstation GC.</summary>
    public const string WorkstationMode = "Workstation";

    /// <summary>
    /// The memory ceiling at or above which Workstation GC is reported as hazardous.
    /// </summary>
    /// <remarks>
    /// <b>A deliberately conservative floor, not a measured boundary.</b> The measured
    /// failure was at about 11 GiB, and no run has established where between here and
    /// there the pause distribution starts to overlap a request timeout. Four GiB is
    /// chosen because it is comfortably below any observed failure while still being far
    /// above the heap sizes for which Workstation GC is the right default, so a warning at
    /// this threshold cannot be the reason somebody misses a real problem. Raising it
    /// towards the measured figure would trade a warning nobody needed for a silence
    /// somebody did.
    /// </remarks>
    public const long PauseHazardMemoryLimitBytes = 4L * 1024 * 1024 * 1024;

    /// <summary>
    /// The token every hazard line carries, so that the hazards are greppable out of
    /// <c>docker logs</c> by one search rather than by knowing which variable to look for.
    /// </summary>
    public const string HazardMarker = "GC HAZARD";

    /// <summary>
    /// Reads the collector facts from the running process.
    /// </summary>
    /// <returns>The facts this process is executing under.</returns>
    /// <remarks>
    /// The heap count comes from <see cref="GC.GetConfigurationVariables"/>, which reports
    /// the figure the collector <i>resolved</i> rather than the one that was declared, and
    /// therefore reports 1 under Workstation GC and the processor-derived figure under
    /// Server GC with nothing pinned. A missing or unexpected entry yields
    /// <see langword="null"/> rather than a guess: the key names of that dictionary are not
    /// a documented contract, and a diagnostic that invents a heap count when it cannot
    /// read one would be worse than one that admits it does not know.
    /// </remarks>
    public static RepoContextGarbageCollectionFacts ReadRuntimeFacts()
        => new(
            GCSettings.IsServerGC,
            ReadResolvedHeapCount(),
            GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
            GC.GetTotalPauseDuration());

    /// <summary>
    /// Whether the combination of collector flavour and memory ceiling is the one that
    /// produces multi-minute process-wide pauses.
    /// </summary>
    /// <param name="facts">The collector facts.</param>
    /// <returns>
    /// <see langword="true"/> when the process runs Workstation GC against a ceiling at or
    /// above <see cref="PauseHazardMemoryLimitBytes"/>.
    /// </returns>
    /// <remarks>
    /// Both halves are required, and neither alone is a defect. Workstation GC on a small
    /// heap is the correct default and is what most hosts should run; a large ceiling under
    /// Server GC is exactly the configuration being recommended. It is the pair that is
    /// hazardous, so the pair is what gets named.
    /// </remarks>
    public static bool IsPauseHazard(RepoContextGarbageCollectionFacts facts)
        => !facts.IsServerGc && facts.TotalAvailableMemoryBytes >= PauseHazardMemoryLimitBytes;

    /// <summary>
    /// Renders the collector facts as effective-configuration value lines, each carrying
    /// its own provenance.
    /// </summary>
    /// <param name="facts">The collector facts.</param>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The rendered lines.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is null.</exception>
    /// <remarks>
    /// The declared variables and the resolved facts are reported as separate lines on
    /// purpose. Collapsing them into one line per concept would print an inferred figure in
    /// the shape of a declared one, which is issue #2586 exactly, and it is the shape that
    /// misleads hardest here: the heap count a container declares and the heap count it
    /// runs differ under two separate mechanisms documented below.
    /// </remarks>
    public static IReadOnlyList<string> DescribeSettings(
        RepoContextGarbageCollectionFacts facts,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var declaredServerGc = configuration[ServerGcKey];
        var declaredHeapCount = configuration[HeapCountKey];

        return
        [
            // Resolved and default are deliberately the same value for these two, so no
            // [OVERRIDDEN] marker is rendered. That marker answers "did this move off the
            // value this host reaches with nothing supplied", which for a raw environment
            // variable is the same question the provenance marker already answers. Printing
            // both would put two markers on one line saying one thing, and a reader who has
            // to work out that they are redundant is a reader who stops reading the line.
            RepoContextEffectiveConfiguration.DescribeSetting(
                ServerGcKey,
                declaredServerGc,
                declaredServerGc,
                RepoContextEffectiveConfiguration.ProvenanceOf(declaredServerGc)),
            RepoContextEffectiveConfiguration.DescribeSetting(
                HeapCountKey,
                declaredHeapCount,
                declaredHeapCount,
                RepoContextEffectiveConfiguration.ProvenanceOf(declaredHeapCount)),
            RepoContextEffectiveConfiguration.DescribeSetting(
                RuntimeModeKey,
                facts.IsServerGc ? ServerMode : WorkstationMode,
                facts.IsServerGc ? ServerMode : WorkstationMode,
                RepoContextSettingProvenance.Runtime),
            RepoContextEffectiveConfiguration.DescribeSetting(
                RuntimeHeapCountKey,
                RenderHeapCount(facts),
                RenderHeapCount(facts),
                RepoContextSettingProvenance.Runtime),
            RepoContextEffectiveConfiguration.DescribeSetting(
                RuntimeMemoryLimitKey,
                RenderBytes(facts.TotalAvailableMemoryBytes),
                RenderBytes(facts.TotalAvailableMemoryBytes),
                RepoContextSettingProvenance.Runtime),
            RepoContextEffectiveConfiguration.DescribeSetting(
                RuntimePauseTotalKey,
                RenderPauseTotal(facts.TotalPauseDuration),
                RenderPauseTotal(facts.TotalPauseDuration),
                RepoContextSettingProvenance.Runtime),
        ];
    }

    /// <summary>
    /// Names every hazardous property of the resolved collector configuration, or returns
    /// an empty list when there is none.
    /// </summary>
    /// <param name="facts">The collector facts.</param>
    /// <param name="configuration">The ambient configuration (environment variables).</param>
    /// <returns>The rendered hazard lines, to be logged at warning level.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="configuration"/> is null.</exception>
    /// <remarks>
    /// Three independent hazards, deliberately not folded into one line: a process can
    /// carry any combination of them, and a reader who is told about the first has no way
    /// to discover the other two.
    /// </remarks>
    public static IReadOnlyList<string> DescribeHazards(
        RepoContextGarbageCollectionFacts facts,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var hazards = new List<string>();
        var declaredHeapCount = configuration[HeapCountKey];

        if (IsPauseHazard(facts))
        {
            hazards.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"{HazardMarker}: this process runs Workstation garbage collection against a "
                + $"memory ceiling of {RenderBytes(facts.TotalAvailableMemoryBytes)}. Workstation "
                + $"GC collects a single heap and its blocking gen2 phases are effectively "
                + $"single-threaded, so once the heap is multi-GiB one collection walks all of "
                + $"it on one thread with every other thread in the process suspended. A "
                + $"deployment of this host measured a single such pause at 252 seconds against "
                + $"a 30 second request timeout. Set {ServerGcKey}=1 to collect in parallel, and "
                + $"bound the heap count with {HeapCountKey} rather than letting it follow "
                + $"DOTNET_PROCESSOR_COUNT, which this deployment pins above its CPU grant for "
                + $"an unrelated reason. This removes that class of pause; it is not a remedy "
                + $"for long stalls in general, and pauses the runtime does not attribute to the "
                + $"collector need a separate diagnosis."));
        }

        if (!facts.IsServerGc && !string.IsNullOrWhiteSpace(declaredHeapCount))
        {
            hazards.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"{HazardMarker}: {HeapCountKey} is declared as '{declaredHeapCount}' but this "
                + $"process runs Workstation garbage collection, which collects a single heap. "
                + $"The declaration is inert and the resolved heap count is "
                + $"{RenderHeapCount(facts)}. A variable an operator set that binds to nothing "
                + $"reads as configured, so it is named here rather than left to look applied. "
                + $"{HeapCountKey} takes effect only when {ServerGcKey}=1."));
        }

        var notation = DescribeHeapCountNotationHazard(declaredHeapCount, facts.ResolvedHeapCount);
        if (notation is not null)
        {
            hazards.Add(notation);
        }

        return hazards;
    }

    /// <summary>
    /// Names the case where a declared heap count was written in a notation the runtime
    /// reads differently, so the process resolved a heap count nobody asked for.
    /// </summary>
    /// <param name="declared">The raw declared value, or null when nothing was declared.</param>
    /// <param name="resolved">The heap count the collector actually resolved.</param>
    /// <returns>The hazard line, or <see langword="null"/> when the notation is unambiguous.</returns>
    /// <remarks>
    /// <para>
    /// <b>The .NET garbage collector reads its numeric environment variables as
    /// hexadecimal.</b> The same setting written in <c>runtimeconfig.json</c> is decimal,
    /// which is what makes this so easy to get wrong: <c>DOTNET_GCHeapCount=10</c> asks for
    /// sixteen heaps, and <c>DOTNET_GCHeapCount=16</c> asks for twenty-two. Verified against
    /// this runtime rather than taken from documentation: setting the variable to <c>10</c>
    /// resolves <c>HeapCount = 16</c>, and to <c>c</c> resolves <c>12</c>.
    /// </para>
    /// <para>
    /// The check compares the resolved figure against the decimal reading rather than
    /// asserting hexadecimal by itself, so it fires on any disagreement between what was
    /// written and what the process runs - the notation trap, and equally a value the
    /// runtime clamped - and stays silent whenever the operator got the number they wrote.
    /// A warning that fired on every declaration would be noise, and noise is how the
    /// original signal came to be ignored.
    /// </para>
    /// </remarks>
    public static string? DescribeHeapCountNotationHazard(string? declared, int? resolved)
    {
        if (string.IsNullOrWhiteSpace(declared) || resolved is not { } resolvedCount)
        {
            return null;
        }

        var trimmed = declared.Trim();
        if (trimmed.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        if (!int.TryParse(trimmed, NumberStyles.None, CultureInfo.InvariantCulture, out var asDecimal)
            || !int.TryParse(trimmed, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var asHex)
            || asDecimal == asHex
            || resolvedCount == asDecimal)
        {
            return null;
        }

        return string.Create(
            CultureInfo.InvariantCulture,
            $"{HazardMarker}: {HeapCountKey} is declared as '{trimmed}' but this process "
            + $"resolved {resolvedCount} heap(s), not {asDecimal}. The garbage collector reads "
            + $"its numeric environment variables as HEXADECIMAL, so '{trimmed}' asks for "
            + $"{asHex}; the same setting written in runtimeconfig.json would be decimal. Write "
            + $"the value in hexadecimal, ideally with an explicit 0x prefix, and confirm the "
            + $"figure against the {RuntimeHeapCountKey} line in this report rather than "
            + $"against the declaration.");
    }

    /// <summary>
    /// Renders a byte count as GiB alongside the exact figure.
    /// </summary>
    /// <param name="bytes">The byte count.</param>
    /// <returns>The rendered value.</returns>
    /// <remarks>
    /// Both forms, because they answer different questions: the GiB figure is the one an
    /// operator compares against a compose file's <c>mem_limit</c>, and the exact byte count
    /// is the one that survives being pasted into an issue as evidence.
    /// </remarks>
    public static string RenderBytes(long bytes)
        => string.Create(
            CultureInfo.InvariantCulture,
            $"{bytes / (double)(1024 * 1024 * 1024):0.##} GiB ({bytes} bytes)");

    private static string RenderHeapCount(RepoContextGarbageCollectionFacts facts)
        => facts.ResolvedHeapCount is { } count
            ? count.ToString(CultureInfo.InvariantCulture)
            : "<unknown: the runtime did not report a heap count>";

    private static string RenderPauseTotal(TimeSpan total)
        => string.Create(
            CultureInfo.InvariantCulture,
            $"{total.TotalSeconds:0.###}s of stop-the-world pause since process start");

    private static int? ReadResolvedHeapCount()
    {
        // "HeapCount" rather than the environment variable's own name, and verified against
        // the runtime rather than assumed: GC.GetConfigurationVariables() reports the
        // RESOLVED figure under this key (1 under Workstation GC, the processor-derived
        // figure under Server GC with nothing pinned), while DOTNET_GCHeapCount does not
        // appear in that dictionary at all.
        if (!GC.GetConfigurationVariables().TryGetValue("HeapCount", out var value))
        {
            return null;
        }

        return value switch
        {
            long count when count is > 0 and <= int.MaxValue => (int)count,
            int count when count > 0 => count,
            _ => null,
        };
    }
}
