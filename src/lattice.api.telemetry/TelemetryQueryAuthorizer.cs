namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The deny-all authorization gate for a PromQL expression: it scans the query
/// with <see cref="PromQlMetricExtractor"/> and admits it only when every metric
/// the expression can be shown to reference is admitted by the configured
/// <see cref="TelemetryMetricAccessPolicy"/>.
/// </summary>
/// <remarks>
/// <para>
/// The gate <b>fails closed</b>. A query is rejected when it names a metric by an
/// unresolvable <c>__name__</c> matcher (a regex <c>=~</c> or a negative
/// <c>!=</c> / <c>!~</c>), when it carries an unconstrained label-only selector
/// that is anchored to no metric name, or when no metric name could be extracted
/// at all - because in each of those cases the expression can select series the
/// allow-list never admitted.
/// </para>
/// <para>
/// In the <see cref="LatticeTelemetryMetricAccessMode.ReadAll"/> posture the gate
/// admits without scanning, so the read-all path costs nothing.
/// </para>
/// </remarks>
public static class TelemetryQueryAuthorizer
{
    /// <summary>
    /// Tests whether <paramref name="query"/> may be evaluated under
    /// <paramref name="policy"/>.
    /// </summary>
    /// <param name="policy">The configured metric-access policy.</param>
    /// <param name="query">The PromQL expression to authorize.</param>
    /// <param name="denialMessage">
    /// Receives the caller-facing denial message when the query is rejected, or
    /// <see langword="null"/> when it is admitted.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the query may be evaluated;
    /// <see langword="false"/> when <paramref name="denialMessage"/> explains why
    /// it was denied.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="policy"/> or <paramref name="query"/> is <see langword="null"/>.
    /// </exception>
    public static bool TryAuthorizeQuery(
        TelemetryMetricAccessPolicy policy,
        string query,
        out string? denialMessage)
    {
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(query);

        denialMessage = null;
        if (policy.IsReadAll)
        {
            return true;
        }

        var references = PromQlMetricExtractor.ExtractReferences(query);
        if (references.HasUnresolvableNameMatcher)
        {
            denialMessage =
                "The query references a metric by a '__name__' pattern or negative matcher, "
                + "which the telemetry metric-access allow-list cannot admit.";
            return false;
        }

        if (references.HasUnconstrainedSelector)
        {
            // Fail closed: a label-only selector that is not anchored to a metric
            // name (for example the right-hand side of `up or {job="api"}`) matches
            // series across every metric name, so it defeats the allow-list even
            // when the expression also names an admitted metric.
            denialMessage =
                "The query selects series by label without constraining the metric name, "
                + "which the telemetry metric-access allow-list cannot admit.";
            return false;
        }

        if (references.Names.Count == 0)
        {
            // Fail closed: a deny-all query whose metric names cannot be extracted
            // (for example a label-only selector) is rejected rather than admitted.
            denialMessage =
                "The query does not name a metric the telemetry metric-access allow-list can admit.";
            return false;
        }

        foreach (var name in references.Names)
        {
            if (!policy.IsAdmitted(name))
            {
                denialMessage = DeniedMessage(name);
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// The caller-facing message reporting that a named metric is outside the
    /// allow-list, so every surface that denies a metric reports it identically.
    /// </summary>
    /// <param name="metric">The denied metric name.</param>
    /// <returns>The denial message.</returns>
    public static string DeniedMessage(string metric)
        => $"Metric '{metric}' is not permitted by the telemetry metric-access allow-list.";
}
