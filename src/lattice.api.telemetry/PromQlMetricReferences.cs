namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The metric references a conservative scan of a PromQL expression surfaced: the
/// distinct metric names it names in metric-name position or through an exact
/// reserved <c>__name__</c> label matcher, together with a flag that is set when
/// the expression carries a <c>__name__</c> matcher that cannot be reduced to a
/// fixed set of names (a regex <c>=~</c> matcher or a negative <c>!=</c> / <c>!~</c>
/// matcher).
/// </summary>
/// <remarks>
/// The deny-all authorization gate treats a set flag as a hard denial: a matcher
/// that selects a series by pattern or by exclusion could name an allow-listed
/// series only by accident, so the gate fails closed rather than admit it.
/// </remarks>
public readonly record struct PromQlMetricReferences
{
    /// <summary>
    /// The distinct metric names the expression references in metric-name position
    /// or through an exact <c>__name__="..."</c> label matcher, in first-seen order.
    /// </summary>
    public required IReadOnlyList<string> Names { get; init; }

    /// <summary>
    /// Whether the expression carries a <c>__name__</c> matcher that cannot be
    /// reduced to a fixed set of names (a regex <c>=~</c> matcher or a negative
    /// <c>!=</c> / <c>!~</c> matcher), which the deny-all gate rejects outright.
    /// </summary>
    public required bool HasUnresolvableNameMatcher { get; init; }

    /// <summary>
    /// Whether the expression carries an <b>unconstrained</b> <c>{...}</c> label
    /// selector: a label-set matcher that is not anchored to a metric name in name
    /// position and carries no exact <c>__name__="..."</c> matcher (for example the
    /// right-hand selector in <c>up or {job="api"}</c>). Such a selector matches
    /// every series bearing the label across all metric names, so the deny-all gate
    /// rejects it outright - even when the expression also names an admitted metric,
    /// which would otherwise leave <see cref="Names"/> non-empty and slip past the
    /// gate. Defaults to <c>false</c>.
    /// </summary>
    public bool HasUnconstrainedSelector { get; init; }
}
