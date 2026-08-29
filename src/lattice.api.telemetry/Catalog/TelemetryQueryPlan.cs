namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// One catalogue entry in the form the facade executes it: the client-facing
/// descriptor, the compiled template, and the admission decision - everything
/// about the entry that can be settled once at catalogue-build time rather than
/// per request.
/// </summary>
/// <remarks>
/// A plan is built when the catalogue singleton is constructed, so the per-request
/// path performs no template parsing, no metric-name scanning, and no
/// allow-list evaluation - it reads <see cref="IsAdmitted"/> and renders.
/// </remarks>
internal sealed class TelemetryQueryPlan
{
    private TelemetryQueryPlan(
        TelemetryQueryDescriptor descriptor,
        TelemetryQueryTemplate template,
        bool isAdmitted)
    {
        Descriptor = descriptor;
        Template = template;
        IsAdmitted = isAdmitted;
    }

    /// <summary>The client-facing catalogue entry.</summary>
    public TelemetryQueryDescriptor Descriptor { get; }

    /// <summary>The compiled server-authored template.</summary>
    public TelemetryQueryTemplate Template { get; }

    /// <summary>
    /// Whether the configured metric-access allow-list admits every metric this
    /// entry reads. A non-admitted entry is omitted from the catalogue and is
    /// unreachable by id, so an unentitled caller cannot tell it apart from an
    /// entry that does not exist.
    /// </summary>
    /// <remarks>
    /// Decided once, from a scope-free probe render. The tenant and tree matchers
    /// the facade later injects are label matchers, so they cannot introduce a
    /// metric name and cannot change this decision for any request.
    /// </remarks>
    public bool IsAdmitted { get; }

    /// <summary>The entry's catalogue-stable id.</summary>
    public string QueryId => Descriptor.QueryId;

    /// <summary>
    /// Compiles <paramref name="definition"/> into an executable plan and decides
    /// its admission under <paramref name="policy"/>.
    /// </summary>
    /// <param name="definition">The server-authored definition.</param>
    /// <param name="policy">The configured metric-access policy.</param>
    /// <returns>The compiled plan.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="definition"/> or <paramref name="policy"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// The template carries no scope placeholder, a range entry declares no default
    /// step, or the template does not scan to a resolvable set of metric names.
    /// </exception>
    public static TelemetryQueryPlan Compile(
        TelemetryQueryDefinition definition,
        TelemetryMetricAccessPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(policy);

        var template = TelemetryQueryTemplate.Parse(definition.QueryTemplate);
        if (!template.HasScopeSlot)
        {
            throw new ArgumentException(
                $"Telemetry query '{definition.QueryId}' declares a template with no "
                + $"'{TelemetryQueryTemplate.ScopeToken}' placeholder, so the facade could not "
                + "apply the tenant matcher it derives from the authenticated caller. A curated "
                + "query must be scopeable; isolation is never optional.",
                nameof(definition));
        }

        if (definition.Descriptor.Kind == TelemetryQueryKind.Range
            && definition.Descriptor.Bounds.DefaultStep <= TimeSpan.Zero)
        {
            throw new ArgumentException(
                $"Telemetry query '{definition.QueryId}' is a range query but declares no positive "
                + "default step, so a caller that supplies none would leave the resolution "
                + "unbounded. A range entry must declare the step it falls back to.",
                nameof(definition));
        }

        // Probe render: an unscoped selector and the default window yield the
        // template's metric-name footprint, which decides admission.
        var probe = template.Render(TelemetryScopeSelector.Unscoped, TelemetryRateWindow.Default);
        RequireScannableTemplate(definition.QueryId, probe);

        var admitted = TelemetryQueryAuthorizer.TryAuthorizeQuery(policy, probe, out _);
        return new TelemetryQueryPlan(definition.Descriptor, template, admitted);
    }

    /// <summary>
    /// Requires that a template resolve to a fixed set of metric names under the
    /// conservative scanner, at catalogue-build time and independently of the
    /// configured access posture.
    /// </summary>
    /// <remarks>
    /// The permissive read-all posture admits without scanning, so a template that
    /// names a metric by pattern - or carries a selector anchored to no metric
    /// name - would otherwise compile silently on a read-all cluster and fail only
    /// once an operator tightened the allow-list. Asserting here makes a catalogue
    /// that cannot be governed a build failure rather than a deployment-time
    /// surprise.
    /// </remarks>
    private static void RequireScannableTemplate(string queryId, string probe)
    {
        var references = PromQlMetricExtractor.ExtractReferences(probe);
        if (references.HasUnresolvableNameMatcher)
        {
            throw new ArgumentException(
                $"Telemetry query '{queryId}' names a metric by a '__name__' pattern or negative "
                + "matcher, which cannot be reduced to a fixed set of names and so can never be "
                + "governed by the metric-access allow-list.",
                nameof(queryId));
        }

        if (references.HasUnconstrainedSelector)
        {
            throw new ArgumentException(
                $"Telemetry query '{queryId}' carries a label selector anchored to no metric name, "
                + "which matches series across every metric and so defeats the metric-access "
                + "allow-list.",
                nameof(queryId));
        }

        if (references.Names.Count == 0)
        {
            throw new ArgumentException(
                $"Telemetry query '{queryId}' names no metric the metric-access allow-list can "
                + "govern.",
                nameof(queryId));
        }
    }
}
