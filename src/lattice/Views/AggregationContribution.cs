using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// A single effect an <see cref="ILatticeAggregationProjection"/> wants folded
/// into an aggregation view, derived from one source
/// <see cref="LatticeMutation"/>. Unlike a filter/re-project
/// <see cref="ViewWrite"/> (which targets a view key directly), a contribution
/// describes how a <see cref="SourceKey"/> participates in the group named by
/// <see cref="GroupKey"/>; the maintainer resolves the per-source-key prior
/// contribution (the "read before write") and applies the convergent delta.
/// <para>
/// A <see cref="AggregationContributionKind.Retract"/> contribution carries only
/// the <see cref="SourceKey"/> and <see cref="Timestamp"/>: the group and the
/// prior contributed value are recovered from the maintainer's stored
/// contribution row, because a source delete carries no value to derive them
/// from.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.AggregationContribution)]
[Immutable]
public readonly record struct AggregationContribution
{
    /// <summary>How the maintainer folds this contribution into the view.</summary>
    [Id(0)] public AggregationContributionKind Kind { get; init; }

    /// <summary>
    /// The group the source key contributes to for a
    /// <see cref="AggregationContributionKind.Contribute"/>; ignored for a
    /// <see cref="AggregationContributionKind.Retract"/> (the group is recovered
    /// from the stored contribution row). For a
    /// <see cref="AggregationContributionKind.RangeReconcile"/> this is the
    /// inclusive start of the affected source range.
    /// </summary>
    [Id(1)] public string GroupKey { get; init; }

    /// <summary>The source key that produces this contribution.</summary>
    [Id(2)] public string SourceKey { get; init; }

    /// <summary>
    /// The numeric the source key contributes for a
    /// <see cref="AggregationKind.Sum"/>, <see cref="AggregationKind.Min"/>, or
    /// <see cref="AggregationKind.Max"/> view; <c>0</c> and unused for
    /// <see cref="AggregationKind.Count"/> and <see cref="AggregationKind.SetUnion"/>.
    /// </summary>
    [Id(3)] public double Numeric { get; init; }

    /// <summary>
    /// The member the source key contributes to a
    /// <see cref="AggregationKind.SetUnion"/> view's distinct-member set;
    /// <see langword="null"/> for every other kind.
    /// </summary>
    [Id(4)] public string? Member { get; init; }

    /// <summary>
    /// The source entry's <see cref="HybridLogicalClock"/>, used to order the
    /// contributions in a drain batch so per-source-key read-before-write applies
    /// in source-commit order.
    /// </summary>
    [Id(5)] public HybridLogicalClock Timestamp { get; init; }

    /// <summary>
    /// Exclusive upper bound of the affected source range for a
    /// <see cref="AggregationContributionKind.RangeReconcile"/>;
    /// <see langword="null"/> otherwise.
    /// </summary>
    [Id(6)] public string? EndKey { get; init; }

    /// <summary>
    /// The source entry's value bytes a <see cref="AggregationKind.Fold"/> view
    /// folds into its group accumulator (carried so the maintainer can store the
    /// per-source-key contribution and re-fold the group on any change);
    /// <see langword="null"/> for every other kind.
    /// </summary>
    [Id(7)] public byte[]? Value { get; init; }

    /// <summary>
    /// Creates a <see cref="AggregationContributionKind.Contribute"/> over a
    /// numeric value (sum / min / max).
    /// </summary>
    /// <param name="groupKey">The group the source key contributes to. Must not be <see langword="null"/>.</param>
    /// <param name="sourceKey">The contributing source key. Must not be <see langword="null"/>.</param>
    /// <param name="numeric">The numeric contributed.</param>
    /// <param name="timestamp">The source entry HLC.</param>
    public static AggregationContribution OfNumeric(string groupKey, string sourceKey, double numeric, HybridLogicalClock timestamp)
    {
        ArgumentNullException.ThrowIfNull(groupKey);
        ArgumentNullException.ThrowIfNull(sourceKey);
        return new AggregationContribution
        {
            Kind = AggregationContributionKind.Contribute,
            GroupKey = groupKey,
            SourceKey = sourceKey,
            Numeric = numeric,
            Timestamp = timestamp,
        };
    }

    /// <summary>
    /// Creates a <see cref="AggregationContributionKind.Contribute"/> that adds
    /// the source key to a group (count) with no numeric or member payload.
    /// </summary>
    /// <param name="groupKey">The group the source key contributes to. Must not be <see langword="null"/>.</param>
    /// <param name="sourceKey">The contributing source key. Must not be <see langword="null"/>.</param>
    /// <param name="timestamp">The source entry HLC.</param>
    public static AggregationContribution Membership(string groupKey, string sourceKey, HybridLogicalClock timestamp)
    {
        ArgumentNullException.ThrowIfNull(groupKey);
        ArgumentNullException.ThrowIfNull(sourceKey);
        return new AggregationContribution
        {
            Kind = AggregationContributionKind.Contribute,
            GroupKey = groupKey,
            SourceKey = sourceKey,
            Timestamp = timestamp,
        };
    }

    /// <summary>
    /// Creates a <see cref="AggregationContributionKind.Contribute"/> that adds
    /// the source key's <paramref name="member"/> to a set-union group.
    /// </summary>
    /// <param name="groupKey">The group the source key contributes to. Must not be <see langword="null"/>.</param>
    /// <param name="sourceKey">The contributing source key. Must not be <see langword="null"/>.</param>
    /// <param name="member">The member added to the group's distinct-member set. Must not be <see langword="null"/>.</param>
    /// <param name="timestamp">The source entry HLC.</param>
    public static AggregationContribution SetMember(string groupKey, string sourceKey, string member, HybridLogicalClock timestamp)
    {
        ArgumentNullException.ThrowIfNull(groupKey);
        ArgumentNullException.ThrowIfNull(sourceKey);
        ArgumentNullException.ThrowIfNull(member);
        return new AggregationContribution
        {
            Kind = AggregationContributionKind.Contribute,
            GroupKey = groupKey,
            SourceKey = sourceKey,
            Member = member,
            Timestamp = timestamp,
        };
    }

    /// <summary>
    /// Creates a <see cref="AggregationContributionKind.Contribute"/> that folds
    /// the source key's <paramref name="value"/> bytes into a
    /// <see cref="AggregationKind.Fold"/> group. The maintainer stores the value
    /// as the source key's contribution and re-folds the group in HLC order.
    /// </summary>
    /// <param name="groupKey">The group the source key contributes to. Must not be <see langword="null"/>.</param>
    /// <param name="sourceKey">The contributing source key. Must not be <see langword="null"/>.</param>
    /// <param name="value">The source value bytes folded into the group accumulator. Must not be <see langword="null"/>.</param>
    /// <param name="timestamp">The source entry HLC.</param>
    public static AggregationContribution Fold(string groupKey, string sourceKey, byte[] value, HybridLogicalClock timestamp)
    {
        ArgumentNullException.ThrowIfNull(groupKey);
        ArgumentNullException.ThrowIfNull(sourceKey);
        ArgumentNullException.ThrowIfNull(value);
        return new AggregationContribution
        {
            Kind = AggregationContributionKind.Contribute,
            GroupKey = groupKey,
            SourceKey = sourceKey,
            Value = value,
            Timestamp = timestamp,
        };
    }

    /// <summary>
    /// Creates a <see cref="AggregationContributionKind.Retract"/> removing a
    /// source key's contribution (a delete, tombstone, or filter exit).
    /// </summary>
    /// <param name="sourceKey">The source key whose contribution is retracted. Must not be <see langword="null"/>.</param>
    /// <param name="timestamp">The source entry HLC.</param>
    public static AggregationContribution Retract(string sourceKey, HybridLogicalClock timestamp)
    {
        ArgumentNullException.ThrowIfNull(sourceKey);
        return new AggregationContribution
        {
            Kind = AggregationContributionKind.Retract,
            GroupKey = string.Empty,
            SourceKey = sourceKey,
            Timestamp = timestamp,
        };
    }

    /// <summary>
    /// Creates a <see cref="AggregationContributionKind.RangeReconcile"/> asking
    /// the maintainer to rebuild the affected source range
    /// <c>[<paramref name="startInclusive"/>, <paramref name="endExclusive"/>)</c>.
    /// </summary>
    /// <param name="startInclusive">Inclusive lower bound of the affected source range. Must not be <see langword="null"/>.</param>
    /// <param name="endExclusive">Exclusive upper bound of the affected source range. Must not be <see langword="null"/>.</param>
    /// <param name="timestamp">The source HLC of the range delete that triggered the reconcile.</param>
    public static AggregationContribution RangeReconcile(string startInclusive, string endExclusive, HybridLogicalClock timestamp)
    {
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        return new AggregationContribution
        {
            Kind = AggregationContributionKind.RangeReconcile,
            GroupKey = startInclusive,
            SourceKey = string.Empty,
            EndKey = endExclusive,
            Timestamp = timestamp,
        };
    }

    /// <summary>
    /// Compares two contributions by value: every scalar field plus the
    /// <see cref="Value"/> bytes compared by content. The compiler-generated
    /// record-struct equality compares <see cref="Value"/> with
    /// <see cref="EqualityComparer{T}.Default"/>, which for a <see cref="byte"/>
    /// array is reference equality, so two structurally identical fold
    /// contributions - including a contribution and its post-serialization self -
    /// would otherwise never compare equal, silently defeating any dedup, cache
    /// lookup, or round-trip check framed as record equality.
    /// </summary>
    /// <param name="other">The contribution to compare against.</param>
    public bool Equals(AggregationContribution other) =>
        Kind == other.Kind
        && string.Equals(GroupKey, other.GroupKey, StringComparison.Ordinal)
        && string.Equals(SourceKey, other.SourceKey, StringComparison.Ordinal)
        && Numeric.Equals(other.Numeric)
        && string.Equals(Member, other.Member, StringComparison.Ordinal)
        && Timestamp.Equals(other.Timestamp)
        && string.Equals(EndKey, other.EndKey, StringComparison.Ordinal)
        && (Value is null ? other.Value is null : other.Value is not null && Value.AsSpan().SequenceEqual(other.Value));

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Kind);
        hash.Add(GroupKey, StringComparer.Ordinal);
        hash.Add(SourceKey, StringComparer.Ordinal);
        hash.Add(Numeric);
        hash.Add(Member, StringComparer.Ordinal);
        hash.Add(Timestamp);
        hash.Add(EndKey, StringComparer.Ordinal);
        if (Value is { } bytes)
        {
            hash.AddBytes(bytes);
        }

        return hash.ToHashCode();
    }
}
