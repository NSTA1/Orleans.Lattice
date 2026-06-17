namespace Orleans.Lattice;

/// <summary>
/// Lowers a source <see cref="LatticeMutation"/> into the set of
/// <see cref="AggregationContribution"/>s that maintain an <b>aggregation</b>
/// materialised view (a grouped reduce: count / sum / min / max / set-union).
/// The maintainer invokes <see cref="Project"/> for every user mutation it reads
/// off the source tree's write-ahead log and folds the resulting contributions
/// into the per-group accumulators in the <c>view-{name}</c> tree.
/// <para>
/// Group-by is the legitimate many-to-one mapping (many source keys to one group
/// key), unlike the injective re-key of an <see cref="ILatticeViewProjection"/>.
/// A projection must be a <b>pure, deterministic</b> function of its input
/// mutation so that every cluster derives an identical view from converged
/// source state. Implementations are resolved as services (never serialized).
/// </para>
/// </summary>
public interface ILatticeAggregationProjection
{
    /// <summary>
    /// A stable identifier for the projection's logic. Stamped into the view
    /// maintainer's durable checkpoint; a mismatch on startup signals that the
    /// projection changed and the view must be rebuilt.
    /// </summary>
    string ProjectionVersion { get; }

    /// <summary>
    /// The reduce this projection's view computes. Tells the maintainer which
    /// accumulator shape and retraction mechanism to use.
    /// </summary>
    AggregationKind Aggregation { get; }

    /// <summary>
    /// Returns the contributions <paramref name="mutation"/> produces. A source
    /// <c>Set</c> yields a <see cref="AggregationContributionKind.Contribute"/>
    /// (or a <see cref="AggregationContributionKind.Retract"/> when the value
    /// fell out of the projection filter); a delete or tombstone yields a
    /// <see cref="AggregationContributionKind.Retract"/>; an unconstrained range
    /// delete yields a <see cref="AggregationContributionKind.RangeReconcile"/>.
    /// </summary>
    /// <param name="mutation">The committed source mutation to project.</param>
    IEnumerable<AggregationContribution> Project(LatticeMutation mutation);
}
