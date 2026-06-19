using System.IO.Hashing;
using System.Text;

namespace Orleans.Lattice;

/// <summary>
/// Built-in <see cref="ILatticeAggregationProjection"/> for grouped reduces. An
/// operator supplies a <b>group-key selector</b> (the legitimate many-to-one
/// mapping from a source value to a group key) plus, depending on the
/// <see cref="AggregationKind"/>, a numeric value selector
/// (<see cref="AggregationKind.Sum"/> / <see cref="AggregationKind.Min"/> /
/// <see cref="AggregationKind.Max"/>) or a member selector
/// (<see cref="AggregationKind.SetUnion"/>). An optional
/// <see cref="LatticePredicateNode"/> filter restricts which source entries
/// participate; an entry that updates out of the filter is retracted from its
/// group.
/// <para>
/// <b>Retraction.</b> A source delete or tombstone carries no value, so the
/// projection emits a <see cref="AggregationContributionKind.Retract"/> carrying
/// only the source key; the maintainer recovers the group and the prior
/// contribution from its stored contribution row. A <c>Set</c> that fails the
/// filter is retracted the same way.
/// </para>
/// <para>
/// <b>Determinism.</b> The selectors are delegates (not structurally hashable),
/// so <see cref="ProjectionVersion"/> folds in a caller-declared
/// <c>selectorVersion</c> tag alongside the aggregation kind and the structural
/// hash of the filter. Bump the tag whenever a selector's logic changes so the
/// maintainer rebuilds.
/// </para>
/// </summary>
public sealed class AggregationLatticeViewProjection : ILatticeAggregationProjection
{
    private readonly Func<byte[], string> _groupKeySelector;
    private readonly Func<byte[], double>? _valueSelector;
    private readonly Func<byte[], string>? _memberSelector;
    private readonly LatticePredicateNode? _filter;
    private readonly string _projectionVersion;

    /// <summary>
    /// Creates an aggregation projection.
    /// </summary>
    /// <param name="aggregation">The reduce the view computes.</param>
    /// <param name="groupKeySelector">
    /// Maps a source entry's value bytes to the group key it belongs to. Must not
    /// be <see langword="null"/>. The group key must not begin with the reserved
    /// NUL (<c>\u0000</c>) character (the maintainer reserves that prefix for
    /// internal accumulator rows).
    /// </param>
    /// <param name="selectorVersion">
    /// Stable tag identifying the selectors' logic, folded into
    /// <see cref="ProjectionVersion"/>. Required because the selectors are
    /// delegates and cannot be structurally hashed.
    /// </param>
    /// <param name="valueSelector">
    /// Maps a source value to the numeric it contributes. Required for
    /// <see cref="AggregationKind.Sum"/>, <see cref="AggregationKind.Min"/>, and
    /// <see cref="AggregationKind.Max"/>; ignored otherwise.
    /// </param>
    /// <param name="memberSelector">
    /// Maps a source value to the member it contributes to the group's
    /// distinct-member set. Required for <see cref="AggregationKind.SetUnion"/>;
    /// ignored otherwise.
    /// </param>
    /// <param name="filter">
    /// Optional value filter (the predicate IR produced by
    /// <c>LatticePredicateTranslator</c>). When <see langword="null"/> every
    /// source entry participates.
    /// </param>
    public AggregationLatticeViewProjection(
        AggregationKind aggregation,
        Func<byte[], string> groupKeySelector,
        string selectorVersion,
        Func<byte[], double>? valueSelector = null,
        Func<byte[], string>? memberSelector = null,
        LatticePredicateNode? filter = null)
    {
        ArgumentNullException.ThrowIfNull(groupKeySelector);
        ArgumentException.ThrowIfNullOrEmpty(selectorVersion);

        if (aggregation is AggregationKind.Sum or AggregationKind.Min or AggregationKind.Max && valueSelector is null)
        {
            throw new ArgumentException(
                $"A value selector is required for a {aggregation} aggregation.",
                nameof(valueSelector));
        }

        if (aggregation is AggregationKind.SetUnion && memberSelector is null)
        {
            throw new ArgumentException(
                "A member selector is required for a set-union aggregation.",
                nameof(memberSelector));
        }

        Aggregation = aggregation;
        _groupKeySelector = groupKeySelector;
        _valueSelector = valueSelector;
        _memberSelector = memberSelector;
        _filter = filter;
        _projectionVersion = ComputeVersion(aggregation, selectorVersion, filter);
    }

    /// <summary>
    /// Creates an aggregation projection whose selectors run against the
    /// deserialized value type <typeparamref name="T"/> instead of raw
    /// <c>byte[]</c>. The supplied <paramref name="serializer"/> (or
    /// <see cref="JsonLatticeSerializer{T}.Default"/> when omitted) deserializes
    /// each source value before the selectors see it, so an operator writes
    /// <c>u =&gt; u.Name</c> rather than
    /// <c>bytes =&gt; serializer.Deserialize(bytes).Name</c>.
    /// </summary>
    /// <typeparam name="T">The source value type the selectors operate on.</typeparam>
    /// <param name="aggregation">The reduce the view computes.</param>
    /// <param name="groupKeySelector">
    /// Maps a deserialized source value to the group key it belongs to. Must not
    /// be <see langword="null"/>. The group key must not begin with the reserved
    /// NUL (<c>\u0000</c>) character.
    /// </param>
    /// <param name="selectorVersion">
    /// Stable tag identifying the selectors' logic, folded into
    /// <see cref="ProjectionVersion"/>. Required because the selectors are
    /// delegates and cannot be structurally hashed.
    /// </param>
    /// <param name="valueSelector">
    /// Maps a deserialized source value to the numeric it contributes. Required
    /// for <see cref="AggregationKind.Sum"/>, <see cref="AggregationKind.Min"/>,
    /// and <see cref="AggregationKind.Max"/>; ignored otherwise.
    /// </param>
    /// <param name="memberSelector">
    /// Maps a deserialized source value to the member it contributes to the
    /// group's distinct-member set. Required for
    /// <see cref="AggregationKind.SetUnion"/>; ignored otherwise.
    /// </param>
    /// <param name="filter">
    /// Optional value filter (the predicate IR produced by
    /// <c>LatticePredicateTranslator</c>). When <see langword="null"/> every
    /// source entry participates.
    /// </param>
    /// <param name="serializer">
    /// Deserializes source values to <typeparamref name="T"/>. Defaults to
    /// <see cref="JsonLatticeSerializer{T}.Default"/> when <see langword="null"/>.
    /// </param>
    public static AggregationLatticeViewProjection Create<T>(
        AggregationKind aggregation,
        Func<T, string> groupKeySelector,
        string selectorVersion,
        Func<T, double>? valueSelector = null,
        Func<T, string>? memberSelector = null,
        LatticePredicateNode? filter = null,
        ILatticeSerializer<T>? serializer = null)
    {
        ArgumentNullException.ThrowIfNull(groupKeySelector);
        serializer ??= JsonLatticeSerializer<T>.Default;

        return new AggregationLatticeViewProjection(
            aggregation,
            groupKeySelector: bytes => groupKeySelector(serializer.Deserialize(bytes)),
            selectorVersion: selectorVersion,
            valueSelector: valueSelector is null ? null : bytes => valueSelector(serializer.Deserialize(bytes)),
            memberSelector: memberSelector is null ? null : bytes => memberSelector(serializer.Deserialize(bytes)),
            filter: filter);
    }

    /// <inheritdoc />
    public AggregationKind Aggregation { get; }

    /// <inheritdoc />
    public string ProjectionVersion => _projectionVersion;

    /// <inheritdoc />
    public IEnumerable<AggregationContribution> Project(LatticeMutation mutation)
    {
        switch (mutation.Kind)
        {
            case MutationKind.Set:
                if (mutation.Value is null)
                {
                    yield break;
                }

                if (_filter is { } filter && !LatticePredicateEvaluator.Matches(mutation.Value, filter))
                {
                    // The entry fell out of the filter: retract whatever it last
                    // contributed (the maintainer recovers the prior group).
                    yield return AggregationContribution.Retract(mutation.Key, mutation.Timestamp);
                    yield break;
                }

                var groupKey = _groupKeySelector(mutation.Value);
                yield return Aggregation switch
                {
                    AggregationKind.Count => AggregationContribution.Membership(groupKey, mutation.Key, mutation.Timestamp),
                    AggregationKind.SetUnion => AggregationContribution.SetMember(groupKey, mutation.Key, _memberSelector!(mutation.Value), mutation.Timestamp),
                    _ => AggregationContribution.OfNumeric(groupKey, mutation.Key, _valueSelector!(mutation.Value), mutation.Timestamp),
                };
                break;

            case MutationKind.Delete:
            case MutationKind.Tombstone:
                yield return AggregationContribution.Retract(mutation.Key, mutation.Timestamp);
                break;

            case MutationKind.DeleteRange:
                if (mutation.MatchedKeys is { Count: > 0 } matched)
                {
                    foreach (var key in matched)
                    {
                        yield return AggregationContribution.Retract(key, mutation.Timestamp);
                    }

                    yield break;
                }

                // An unconstrained range delete cannot be lowered to exact
                // per-key retractions without a reverse index: ask the maintainer
                // to reconcile the affected range by rebuilding.
                if (!string.IsNullOrEmpty(mutation.EndExclusiveKey))
                {
                    yield return AggregationContribution.RangeReconcile(mutation.Key, mutation.EndExclusiveKey, mutation.Timestamp);
                }

                break;

            default:
                yield break;
        }
    }

    private static string ComputeVersion(AggregationKind aggregation, string selectorVersion, LatticePredicateNode? filter)
    {
        var builder = new StringBuilder();
        builder.Append("agg-v1|kind=").Append((int)aggregation);
        builder.Append("|selectors=").Append(selectorVersion);
        builder.Append("|filter=");
        if (filter is { } node)
        {
            AppendNode(builder, node);
        }
        else
        {
            builder.Append("none");
        }

        var hash = XxHash128.Hash(Encoding.UTF8.GetBytes(builder.ToString()));
        return Convert.ToHexString(hash);
    }

    private static void AppendNode(StringBuilder builder, in LatticePredicateNode node)
    {
        builder.Append('(')
            .Append((int)node.Kind).Append(':')
            .Append(node.MemberPath ?? string.Empty).Append(':')
            .Append((int)node.ComparisonOperator).Append(':')
            .Append((int)node.BooleanOperator).Append(':')
            .Append((int)node.StringMethod).Append(':')
            .Append(node.Constant.ToString());

        if (node.Children is { } children)
        {
            builder.Append(":[");
            foreach (var child in children)
            {
                AppendNode(builder, child);
            }

            builder.Append(']');
        }

        builder.Append(')');
    }
}
