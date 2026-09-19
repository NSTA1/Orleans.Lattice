namespace Orleans.Lattice.Benchmark.RegistryFanIn;

/// <summary>
/// The latency distribution of one population of calls. The protocol reports
/// DISTRIBUTIONS rather than peaks, because observed storm magnitude already
/// varies by a factor of several between runs, and a peak cannot distinguish a
/// real effect from that dispersion.
/// </summary>
/// <param name="Count">How many calls are in the population.</param>
/// <param name="P50Ms">Median latency.</param>
/// <param name="P95Ms">95th percentile latency.</param>
/// <param name="P99Ms">99th percentile latency.</param>
/// <param name="MaxMs">Slowest call.</param>
/// <param name="MeanMs">Arithmetic mean latency.</param>
internal sealed record LatencyDistribution(
    int Count,
    double P50Ms,
    double P95Ms,
    double P99Ms,
    double MaxMs,
    double MeanMs)
{
    /// <summary>The distribution of an empty population.</summary>
    public static LatencyDistribution Empty { get; } = new(0, 0, 0, 0, 0, 0);

    /// <summary>
    /// Summarises <paramref name="latenciesMs"/>. Percentiles use nearest-rank on
    /// the sorted sample, so a reported value is always an observed one.
    /// </summary>
    /// <param name="latenciesMs">The latencies to summarise.</param>
    /// <returns>The distribution.</returns>
    public static LatencyDistribution From(IReadOnlyList<double> latenciesMs)
    {
        ArgumentNullException.ThrowIfNull(latenciesMs);
        if (latenciesMs.Count == 0)
        {
            return Empty;
        }

        var sorted = latenciesMs.ToArray();
        Array.Sort(sorted);
        return new LatencyDistribution(
            sorted.Length,
            Quantile(sorted, 0.50),
            Quantile(sorted, 0.95),
            Quantile(sorted, 0.99),
            sorted[^1],
            sorted.Average());
    }

    private static double Quantile(double[] sorted, double q)
    {
        var rank = (int)Math.Ceiling(q * sorted.Length);
        return sorted[Math.Clamp(rank - 1, 0, sorted.Length - 1)];
    }
}

/// <summary>
/// The per-member outcome census for one driven grain member.
/// </summary>
/// <param name="Member">The grain member.</param>
/// <param name="Ok">Calls that returned.</param>
/// <param name="Deadline">Calls that ended in a response-deadline timeout.</param>
/// <param name="DeadlineWithDiagnostics">
/// Deadlines whose message carried Orleans' <c>Diagnostics:</c> clause - the
/// target activation existed and answered a status probe, so the call was
/// admitted and served slowly.
/// </param>
/// <param name="DeadlineWithoutDiagnostics">
/// Deadlines with no <c>Diagnostics:</c> clause - the target was still
/// ACTIVATING, so the call was never served at all.
/// </param>
/// <param name="Fault">Calls that threw something other than a deadline.</param>
/// <param name="FaultTypes">
/// The distinct exception type names behind <paramref name="Fault"/>, with
/// counts. Without this a fault population is just a number, and the rig's own
/// misconfiguration is indistinguishable from a genuine server fault - which is
/// exactly the confusion a measurement rig must not introduce.
/// </param>
/// <param name="FaultExamples">One example message per distinct fault type.</param>
/// <param name="Served">The latency distribution of the calls that returned.</param>
internal sealed record MemberCensus(
    string Member,
    int Ok,
    int Deadline,
    int DeadlineWithDiagnostics,
    int DeadlineWithoutDiagnostics,
    int Fault,
    IReadOnlyDictionary<string, int> FaultTypes,
    IReadOnlyDictionary<string, string> FaultExamples,
    LatencyDistribution Served);

/// <summary>
/// One deadline, with the wall-clock instant it was issued at, so the collector
/// can bucket faults by TIMESTAMP rather than by counter scrape.
/// </summary>
/// <param name="AtUtc">When the timed-out call was issued.</param>
/// <param name="Member">The grain member called.</param>
/// <param name="ElapsedMs">How long the caller waited before the deadline fired.</param>
/// <param name="CarriedDiagnostics">Whether the message carried the <c>Diagnostics:</c> clause.</param>
internal sealed record DeadlineEvent(
    DateTimeOffset AtUtc,
    string Member,
    double ElapsedMs,
    bool CarriedDiagnostics);

/// <summary>
/// The whole client-side result of one driver invocation, written as JSON so the
/// measurement scripts never have to parse console text.
/// </summary>
/// <param name="Verb">Which driver verb produced it.</param>
/// <param name="StartedAtUtc">When the driver started its workload.</param>
/// <param name="CompletedAtUtc">When it finished.</param>
/// <param name="TreeCount">How many trees the invocation addressed.</param>
/// <param name="TreePrefix">The tree-id prefix the invocation owns.</param>
/// <param name="PeakInFlight">
/// The highest number of calls this driver had outstanding at once - the
/// client-side half of the fan-in measurement.
/// </param>
/// <param name="Members">The per-member outcome census.</param>
/// <param name="Deadlines">Every deadline, timestamped.</param>
/// <param name="Estate">
/// How much state the estate held, where the invocation measured it. Reported on
/// every measurement because a timeout count without the estate's depth cannot
/// discriminate registry fan-in (which scales with tree count) from storage
/// starvation during snapshot replay (which scales with leaf count and store
/// size), and the two mechanisms call for different remedies.
/// </param>
/// <param name="Notes">Free-form notes (a refusal reason, a teardown summary).</param>
internal sealed record DriverReport(
    string Verb,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset CompletedAtUtc,
    int TreeCount,
    string TreePrefix,
    int PeakInFlight,
    IReadOnlyList<MemberCensus> Members,
    IReadOnlyList<DeadlineEvent> Deadlines,
    EstateCensus? Estate,
    IReadOnlyList<string> Notes)
{
    /// <summary>
    /// Folds <paramref name="census"/> into a report.
    /// </summary>
    /// <param name="verb">The driver verb.</param>
    /// <param name="startedAt">When the workload started.</param>
    /// <param name="treeCount">How many trees were addressed.</param>
    /// <param name="treePrefix">The tree-id prefix.</param>
    /// <param name="census">The recorded calls.</param>
    /// <param name="notes">Free-form notes.</param>
    /// <param name="estate">The estate census, where one was taken.</param>
    /// <returns>The report.</returns>
    public static DriverReport From(
        string verb,
        DateTimeOffset startedAt,
        int treeCount,
        string treePrefix,
        CallCensus census,
        IReadOnlyList<string> notes,
        EstateCensus? estate = null)
    {
        ArgumentNullException.ThrowIfNull(verb);
        ArgumentNullException.ThrowIfNull(treePrefix);
        ArgumentNullException.ThrowIfNull(census);
        ArgumentNullException.ThrowIfNull(notes);

        var samples = census.Samples;
        var members = samples
            .GroupBy(s => s.Member, StringComparer.Ordinal)
            .OrderBy(g => g.Key, StringComparer.Ordinal)
            .Select(g => new MemberCensus(
                g.Key,
                g.Count(s => s.Outcome == CallOutcome.Ok),
                g.Count(s => s.Outcome == CallOutcome.Deadline),
                g.Count(s => s.Outcome == CallOutcome.Deadline && s.CarriedDiagnostics),
                g.Count(s => s.Outcome == CallOutcome.Deadline && !s.CarriedDiagnostics),
                g.Count(s => s.Outcome == CallOutcome.Fault),
                g.Where(s => s.Outcome == CallOutcome.Fault)
                    .GroupBy(s => s.FaultType ?? "unknown", StringComparer.Ordinal)
                    .ToDictionary(f => f.Key, f => f.Count(), StringComparer.Ordinal),
                g.Where(s => s.Outcome == CallOutcome.Fault)
                    .GroupBy(s => s.FaultType ?? "unknown", StringComparer.Ordinal)
                    .ToDictionary(f => f.Key, f => f.First().FaultMessage ?? string.Empty, StringComparer.Ordinal),
                LatencyDistribution.From(
                    g.Where(s => s.Outcome == CallOutcome.Ok).Select(s => s.ElapsedMs).ToArray())))
            .ToArray();

        var deadlines = samples
            .Where(s => s.Outcome == CallOutcome.Deadline)
            .OrderBy(s => s.StartedAtUtc)
            .Select(s => new DeadlineEvent(s.StartedAtUtc, s.Member, s.ElapsedMs, s.CarriedDiagnostics))
            .ToArray();

        return new DriverReport(
            verb,
            startedAt,
            DateTimeOffset.UtcNow,
            treeCount,
            treePrefix,
            census.PeakInFlight,
            members,
            deadlines,
            estate,
            notes);
    }
}
