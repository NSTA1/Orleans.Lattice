using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// An <see cref="ILatticeAggregationProjection"/> whose per-group reduce is a
/// <b>user-defined, non-commutative fold</b> rather than one of the built-in
/// commutative reducers. Its <see cref="ILatticeAggregationProjection.Aggregation"/>
/// is <see cref="AggregationKind.Fold"/>, and its
/// <see cref="ILatticeAggregationProjection.Project"/> lowers a source <c>Set</c>
/// into a <see cref="AggregationContribution.Fold"/> carrying the entry's value
/// bytes.
/// <para>
/// The fold is expressed as a seed (<see cref="Initial"/>) and a step
/// (<see cref="Apply"/>). Because a general fold is <b>not invertible</b>, the
/// maintainer cannot un-apply a single member on a delete or filter-exit; instead
/// it keeps each source key's contributed value and <b>re-folds the whole group</b>
/// - over the group's surviving members, in ascending source-HLC order - whenever
/// a member is added, retracted, or range-reconciled. The fold must therefore be a
/// <b>pure, deterministic</b> function of the group's member set (the maintainer
/// supplies the HLC order) so every cluster derives an identical materialised value
/// from converged source state.
/// </para>
/// <para>
/// The materialised group value is the accumulator's opaque bytes returned by the
/// final <see cref="Apply"/> (or <see cref="Initial"/> for a single member seeded
/// then applied once); read it with <c>ILatticeView.GetAsync(groupKey)</c> and
/// deserialize with the accumulator's own serializer.
/// </para>
/// </summary>
public interface ILatticeFoldProjection : ILatticeAggregationProjection
{
    /// <summary>
    /// The empty accumulator for a group: the fold's seed value, folded over the
    /// group's members in HLC order. Must be a pure constant (or an equal fresh
    /// value each call) so the re-fold is deterministic.
    /// </summary>
    byte[] Initial();

    /// <summary>
    /// Folds one surviving member into the running accumulator. Pure and
    /// deterministic in <paramref name="accumulator"/>, <paramref name="sourceKey"/>,
    /// <paramref name="sourceValue"/>, and <paramref name="timestamp"/>; the
    /// maintainer invokes it for each surviving member in ascending source-HLC
    /// order and stores the result as the group's materialised value.
    /// </summary>
    /// <param name="accumulator">The running accumulator (seeded from <see cref="Initial"/>).</param>
    /// <param name="sourceKey">The member's source key.</param>
    /// <param name="sourceValue">The member's committed source value bytes.</param>
    /// <param name="timestamp">The member's source entry HLC.</param>
    byte[] Apply(byte[] accumulator, string sourceKey, byte[] sourceValue, HybridLogicalClock timestamp);
}
