namespace Orleans.Lattice;

/// <summary>
/// Classifies an <see cref="AggregationContribution"/> emitted by an
/// <see cref="ILatticeAggregationProjection"/>, telling the view maintainer how
/// to fold it into the aggregation view's group accumulators.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AggregationContributionKind)]
public enum AggregationContributionKind
{
    /// <summary>
    /// The source key contributes (or updates its contribution) to
    /// <see cref="AggregationContribution.GroupKey"/>. The maintainer reads the
    /// source key's prior contribution row, retracts it from its prior group,
    /// and folds the new contribution into the named group.
    /// </summary>
    Contribute = 0,

    /// <summary>
    /// The source key no longer contributes to any group (a source delete, a
    /// tombstone, or a value that fell out of the projection filter). The
    /// maintainer reads the source key's prior contribution row to discover the
    /// group it last belonged to and retracts it; the carried
    /// <see cref="AggregationContribution.GroupKey"/> is ignored.
    /// </summary>
    Retract = 1,

    /// <summary>
    /// An unconstrained range delete the projection cannot lower to exact
    /// per-key retractions. The maintainer reconciles the affected range by
    /// rebuilding the view, mirroring the filter/re-project
    /// <c>RangeReconcile</c> path.
    /// </summary>
    RangeReconcile = 2,
}
