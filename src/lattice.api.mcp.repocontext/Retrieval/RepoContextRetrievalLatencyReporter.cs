using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The closed vocabulary of retrieval tools the latency instruments partition by,
/// carried on the <see cref="RepoContextRetrievalLatencyReporter.ToolTagKey"/> tag.
/// <para>
/// Every value is a compile-time constant resolved on the server from the call site
/// that is being timed. No part of it is ever taken from a caller argument, so the
/// dimension is bounded by construction and cannot be widened from the wire.
/// </para>
/// </summary>
internal static class RepoContextRetrievalTool
{
    /// <summary>Tag value for a <c>repocontext_search</c> call.</summary>
    public const string Search = "search";

    /// <summary>Tag value for a <c>repocontext_context</c> bundle call.</summary>
    public const string Context = "context";

    /// <summary>Tag value for a <c>repocontext_outline</c> call.</summary>
    public const string Outline = "outline";

    /// <summary>Tag value for a <c>repocontext_related</c> call.</summary>
    public const string Related = "related";
}

/// <summary>
/// The closed vocabulary of retrieval stages the stage-latency instrument partitions
/// by, carried on the <see cref="RepoContextRetrievalLatencyReporter.StageTagKey"/>
/// tag.
/// <para>
/// The four stages have entirely different owners and entirely different fixes, which
/// is the whole reason they are separated: a slow <see cref="Embed"/> is a fault in a
/// <b>separate service</b> reached over the network, a slow <see cref="VectorSearch"/>
/// is the cost of the nearest-neighbour scan inside this process, a slow
/// <see cref="Hydrate"/> is the store of record, and a slow
/// <see cref="KeywordScan"/> is the bulk tree enumeration the fallback runs. Summed
/// into one end-to-end number they are indistinguishable, which makes a slow result
/// undiagnosable.
/// </para>
/// </summary>
internal static class RepoContextRetrievalStage
{
    /// <summary>
    /// Embedding the query text, which is a network hop to the embedding provider.
    /// Timed whether it succeeds or fails, because a provider that is slow <i>and
    /// then</i> fails is exactly the case an operator needs to see.
    /// </summary>
    public const string Embed = "embed";

    /// <summary>
    /// The nearest-neighbour search itself, over the approximate index or by
    /// exhaustive scan. Timed whether or not it returned matches, because an empty
    /// answer from a full scan is the expensive case.
    /// </summary>
    public const string VectorSearch = "vector_search";

    /// <summary>
    /// Hydrating the ranked identities into canonical records from the store of
    /// record. The index returns identities, never a second copy of the data, so this
    /// stage is a real per-hit read and not free.
    /// </summary>
    public const string Hydrate = "hydrate";

    /// <summary>
    /// The deterministic keyword/structural scan the service degrades to when the
    /// semantic path did not answer. Its cost is bounded by the scan ceiling rather
    /// than by the corpus, so it is not interchangeable with the semantic stages.
    /// </summary>
    public const string KeywordScan = "keyword_scan";
}

/// <summary>
/// Publishes <b>how long retrieval took, and which part of it took the time</b>, as
/// two histograms on the repository-context meter.
/// <para>
/// <b>Why it exists.</b> The surface published eleven instruments covering counts and
/// outcomes - searches attempted, retrieval path taken, approximate-plane state,
/// embedder availability - and not one covering duration (issue #2624). "Are searches
/// faster?" therefore could not be answered from the deployment's own telemetry at
/// all; it had to be hand-timed from a client or read out of container logs. That is
/// the same defect family as the rest of this surface's reliability work: an artefact
/// that reports many things confidently while being structurally incapable of
/// answering the question actually being asked of it.
/// </para>
/// <para>
/// <b>An end-to-end number alone would not have fixed it.</b> A single duration
/// lumps a network hop to a separate embedding service together with an in-process
/// vector scan and a per-hit read from the store of record. Those have different
/// owners, different failure modes, and different fixes, so a slow total that cannot
/// be decomposed is a number rather than a measurement. Hence the second instrument.
/// </para>
/// <para>
/// <b>Every duration carries the retrieval path that produced it.</b> A fast keyword
/// answer and a fast approximate answer mean opposite things about system health - the
/// first is a capability loss served quickly, the second is the system working - so a
/// latency series that cannot say which path served it is not interpretable. The tag
/// values are the existing <see cref="RepoContextRetrievalPath"/> vocabulary, so the
/// dimension reads the same here as it does on a result payload.
/// </para>
/// <para>
/// <b>What voids a measurement, stated exactly.</b> This is the property the
/// surrounding reliability work exists to protect, so it is spelled out rather than
/// implied.
/// <list type="bullet">
/// <item><description>
/// <b>The end-to-end instrument cannot fail to record.</b> Every call that enters a
/// timed seam is recorded exactly once from a <c>finally</c>, including one that
/// cancels or throws its way out. A call that terminated before a retrieval path was
/// resolved is recorded under <see cref="PathUnresolved"/> rather than dropped, so
/// there is no early return, no saturation ceiling, and no fault path that silently
/// voids the count. Only the process dying mid-call loses a measurement.
/// </description></item>
/// <item><description>
/// <b>The stage instrument is conditional by construction, and that is readable
/// rather than ambiguous.</b> A stage records if and only if it ran: no
/// <see cref="RepoContextRetrievalStage.Embed"/> is recorded on a host with no
/// embedding provider bound, and no
/// <see cref="RepoContextRetrievalStage.KeywordScan"/> is recorded while the semantic
/// path is answering. Because the end-to-end instrument counts <i>every</i> call, it
/// is the denominator that makes those absences measured: a zero
/// <c>stage.duration_count</c> for <c>embed</c> beside a rising
/// <c>duration_count</c> is a measured absence of embedding - an intended
/// keyword-only deployment - whereas both reading zero means no retrieval ran at all,
/// which is a different fact.
/// </description></item>
/// </list>
/// </para>
/// <para>
/// <b>What the bundled Prometheus exposition does and does not give you.</b> A
/// <see cref="Histogram{T}"/> renders on the container's <c>/metrics</c> endpoint as a
/// Prometheus <c>summary</c> carrying <c>_sum</c> and <c>_count</c> and <b>no
/// <c>_bucket</c> series</b>, because a <see cref="MeterListener"/> does not surface
/// bucket boundaries and the collector refuses to invent them. Two consequences, both
/// load-bearing:
/// <list type="bullet">
/// <item><description>
/// There is <b>no top bucket, so this instrument cannot saturate</b>. The sum is
/// exact and unbounded, so an exhaustive scan that takes a minute is recorded as a
/// minute rather than being clipped into an overflow bucket. That is the failure the
/// naive fix would have introduced one level up.
/// </description></item>
/// <item><description>
/// The price of that is a <b>mean, not a quantile</b>. Read it as
/// <c>rate(..._sum[5m]) / rate(..._count[5m])</c>, which trends mean latency and
/// decomposes it by stage. A <c>histogram_quantile</c> panel against this endpoint
/// returns nothing and must be treated as unavailable rather than as measured; tail
/// latency needs an OTLP exporter, which sees the real distribution.
/// </description></item>
/// </list>
/// </para>
/// </summary>
internal sealed class RepoContextRetrievalLatencyReporter : IDisposable
{
    /// <summary>
    /// The histogram name recording end-to-end latency of one retrieval tool call.
    /// Tagged by <see cref="ToolTagKey"/> and <see cref="PathTagKey"/>.
    /// </summary>
    internal const string DurationInstrumentName = "repocontext.retrieval.duration";

    /// <summary>
    /// The histogram name recording the latency of one stage within a retrieval call.
    /// Tagged by <see cref="StageTagKey"/> and <see cref="PathTagKey"/>.
    /// </summary>
    internal const string StageDurationInstrumentName = "repocontext.retrieval.stage.duration";

    /// <summary>The low-cardinality tag key carrying which retrieval tool was called.</summary>
    internal const string ToolTagKey = "tool";

    /// <summary>The low-cardinality tag key carrying which stage of a retrieval call was timed.</summary>
    internal const string StageTagKey = "stage";

    /// <summary>The low-cardinality tag key carrying the retrieval path that served the call.</summary>
    internal const string PathTagKey = "path";

    /// <summary>
    /// Tag value for a call that never consults the vector plane at all, so no
    /// <see cref="RepoContextRetrievalPath"/> value applies to it: the structural graph
    /// reads <c>outline</c> and <c>related</c>. It is a distinct value rather than an
    /// absent tag so that a graph read is excluded from a path-scoped query by stating
    /// so, and so that its cost stays comparable against a semantic call in one series.
    /// </summary>
    internal const string PathNotApplicable = "not_applicable";

    /// <summary>
    /// Tag value for a call that terminated - cancelled, or threw out of the timed
    /// seam - <b>before</b> a retrieval path was resolved. Recording it under a named
    /// value rather than dropping it is what keeps the end-to-end count a true total:
    /// a dropped measurement would make an aborting host look idle.
    /// </summary>
    internal const string PathUnresolved = "unresolved";

    // Declared above the instruments it constructs, and both instruments are built
    // from this field, so reordering throws at type-initialisation rather than
    // publishing an instrument against a null meter. See the metrics conventions in
    // .github/copilot-instructions.md.
    private readonly Meter _meter;
    private readonly Histogram<double> _duration;
    private readonly Histogram<double> _stageDuration;

    /// <summary>Creates the reporter and publishes its instruments.</summary>
    public RepoContextRetrievalLatencyReporter()
    {
        // Published under the same meter name as the rest of the repocontext surface
        // so a single scraper subscription covers it.
        _meter = new Meter(RepoContextUsageRecorder.MeterName);
        _duration = _meter.CreateHistogram<double>(
            DurationInstrumentName,
            unit: "s",
            description:
                "End-to-end seconds a repocontext retrieval tool call took, tagged by the tool ('search', "
                + "'context', 'outline', 'related') and by the retrieval path that served it - the "
                + "'semantic.exact' / 'semantic.approximate' / 'keyword.*' vocabulary, plus 'not_applicable' "
                + "for the structural graph reads that never consult the vector plane and 'unresolved' for a "
                + "call that cancelled or threw before a path was resolved. Every call entering the timed "
                + "seam is recorded exactly once from a finally, including a cancelled or faulted one, so "
                + "there is no early return or saturation ceiling that voids a measurement and this count is "
                + "a true total. Rendered by the bundled Prometheus exposition as a summary with _sum and "
                + "_count and no _bucket series: it therefore cannot saturate, but it reports a mean "
                + "(rate(_sum)/rate(_count)) rather than a quantile.");
        _stageDuration = _meter.CreateHistogram<double>(
            StageDurationInstrumentName,
            unit: "s",
            description:
                "Seconds one stage of a repocontext retrieval call took, tagged by the stage ('embed' - the "
                + "network hop to the embedding provider, 'vector_search' - the nearest-neighbour search, "
                + "'hydrate' - reading the ranked identities back from the store of record, or "
                + "'keyword_scan' - the deterministic fallback scan) and by the retrieval path the call "
                + "resolved to. A stage is recorded if and only if it ran, including when it ran and then "
                + "failed; stages are conditional by construction, so no embed is recorded on a host with no "
                + "embedder bound. The end-to-end repocontext.retrieval.duration count is the denominator "
                + "that makes such an absence measured rather than absent: zero embeds beside a rising call "
                + "total is an intended keyword-only deployment, while both reading zero means no retrieval "
                + "ran at all.");
    }

    /// <summary>
    /// Records the end-to-end duration of one retrieval tool call.
    /// </summary>
    /// <param name="tool">A <see cref="RepoContextRetrievalTool"/> value naming the tool. Must not be <see langword="null"/>.</param>
    /// <param name="path">
    /// The <see cref="RepoContextRetrievalPath"/> value that served the call,
    /// <see cref="PathNotApplicable"/> for a structural graph read, or
    /// <see langword="null"/> when the call terminated before a path was resolved - in
    /// which case it is recorded as <see cref="PathUnresolved"/> rather than dropped.
    /// </param>
    /// <param name="elapsed">
    /// How long the call took. A negative value is clamped to zero so a clock
    /// irregularity cannot poison the sum, which would be indistinguishable from a
    /// genuine measurement once summed.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="tool"/> is null.</exception>
    public void RecordCall(string tool, string? path, TimeSpan elapsed)
    {
        ArgumentNullException.ThrowIfNull(tool);
        _duration.Record(
            Seconds(elapsed),
            new KeyValuePair<string, object?>(ToolTagKey, tool),
            new KeyValuePair<string, object?>(PathTagKey, path ?? PathUnresolved),
            LatticeTenantLabel.Platform);
    }

    /// <summary>
    /// Records the duration of one stage within a retrieval call.
    /// </summary>
    /// <param name="stage">A <see cref="RepoContextRetrievalStage"/> value naming the stage. Must not be <see langword="null"/>.</param>
    /// <param name="path">
    /// The retrieval path the call resolved to, or <see langword="null"/> when it
    /// terminated before one was resolved (recorded as <see cref="PathUnresolved"/>).
    /// </param>
    /// <param name="elapsed">How long the stage took; a negative value is clamped to zero.</param>
    /// <exception cref="ArgumentNullException"><paramref name="stage"/> is null.</exception>
    public void RecordStage(string stage, string? path, TimeSpan elapsed)
    {
        ArgumentNullException.ThrowIfNull(stage);
        _stageDuration.Record(
            Seconds(elapsed),
            new KeyValuePair<string, object?>(StageTagKey, stage),
            new KeyValuePair<string, object?>(PathTagKey, path ?? PathUnresolved),
            LatticeTenantLabel.Platform);
    }

    /// <summary>Disposes the underlying meter.</summary>
    public void Dispose() => _meter.Dispose();

    private static double Seconds(TimeSpan elapsed)
        => elapsed < TimeSpan.Zero ? 0d : elapsed.TotalSeconds;
}

/// <summary>
/// Accumulates the per-stage elapsed time of one in-flight retrieval call so the
/// stages can be published <b>after</b> the call resolves its retrieval path.
/// <para>
/// <b>Why the stages are buffered rather than emitted as they finish.</b> A stage
/// completes long before the call knows which path answered - the embed runs before
/// anything is known, and the path is only settled once hydration has either produced
/// hits or failed to. Emitting each stage the moment it finished would mean either
/// dropping the <c>path</c> dimension from the stage instrument, which is the exact
/// uninterpretability this work exists to remove, or tagging it with a path that had
/// not been decided yet. Buffering costs one small allocation on a call that is
/// already doing network and grain I/O.
/// </para>
/// <para>
/// It is a class rather than a <see langword="struct"/> deliberately: the stages are
/// measured inside <see langword="async"/> methods, which cannot take a
/// <see langword="ref"/> parameter, so a value type would be copied per frame and the
/// inner stages would be silently lost.
/// </para>
/// </summary>
internal sealed class RepoContextRetrievalTiming
{
    private readonly List<KeyValuePair<string, TimeSpan>> _stages = new(capacity: 4);

    /// <summary>
    /// Times <paramref name="stage"/> for the duration of the returned scope. Prefer
    /// this over hand-held timestamps: it records from a <see langword="finally"/>, so
    /// a stage that throws is still timed, which is the case an operator most needs.
    /// </summary>
    /// <param name="stage">A <see cref="RepoContextRetrievalStage"/> value. Must not be <see langword="null"/>.</param>
    /// <returns>A scope that records the elapsed time when disposed.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="stage"/> is null.</exception>
    public Scope Time(string stage)
    {
        ArgumentNullException.ThrowIfNull(stage);
        return new Scope(this, stage);
    }

    /// <summary>
    /// Publishes every buffered stage against the now-resolved retrieval path and
    /// clears the buffer, so a reporter can never publish one stage twice.
    /// </summary>
    /// <param name="reporter">The reporter to publish through. Must not be <see langword="null"/>.</param>
    /// <param name="path">
    /// The resolved retrieval path, or <see langword="null"/> when the call terminated
    /// before one was resolved.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="reporter"/> is null.</exception>
    public void Flush(RepoContextRetrievalLatencyReporter reporter, string? path)
    {
        ArgumentNullException.ThrowIfNull(reporter);
        foreach (var (stage, elapsed) in _stages)
        {
            reporter.RecordStage(stage, path, elapsed);
        }

        _stages.Clear();
    }

    private void Add(string stage, TimeSpan elapsed)
        => _stages.Add(new KeyValuePair<string, TimeSpan>(stage, elapsed));

    /// <summary>
    /// The disposable scope returned by <see cref="Time(string)"/>, which records its
    /// stage's elapsed time exactly once when disposed.
    /// </summary>
    internal readonly struct Scope : IDisposable
    {
        private readonly RepoContextRetrievalTiming _owner;
        private readonly string _stage;
        private readonly long _startedAt;

        internal Scope(RepoContextRetrievalTiming owner, string stage)
        {
            _owner = owner;
            _stage = stage;
            _startedAt = Stopwatch.GetTimestamp();
        }

        /// <summary>Records the stage's elapsed time against the owning timing buffer.</summary>
        public void Dispose() => _owner.Add(_stage, Stopwatch.GetElapsedTime(_startedAt));
    }
}
