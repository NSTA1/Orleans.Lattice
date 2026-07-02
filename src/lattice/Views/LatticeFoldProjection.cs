using System.Text;
using System.IO.Hashing;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Built-in <see cref="ILatticeFoldProjection"/> for a custom grouped fold. An
/// operator supplies a <b>group-key selector</b> (the many-to-one mapping from a
/// source value to a group key), a <b>seed</b> factory
/// (<see cref="ILatticeFoldProjection.Initial"/>), and a <b>step</b>
/// (<see cref="ILatticeFoldProjection.Apply"/>) that folds one member into the
/// running accumulator. An optional <see cref="LatticePredicateNode"/> filter
/// restricts which source entries participate; an entry that updates out of the
/// filter is retracted from its group.
/// <para>
/// <b>Determinism.</b> The selectors and fold are delegates (not structurally
/// hashable), so <see cref="ILatticeAggregationProjection.ProjectionVersion"/>
/// folds in a caller-declared <c>foldVersion</c> tag alongside the structural
/// hash of the filter. Bump the tag whenever the fold's logic changes so the
/// maintainer rebuilds the view.
/// </para>
/// </summary>
public sealed class LatticeFoldProjection : ILatticeFoldProjection
{
    private readonly Func<byte[], string> _groupKeySelector;
    private readonly Func<byte[]> _initial;
    private readonly Func<byte[], string, byte[], HybridLogicalClock, byte[]> _apply;
    private readonly LatticePredicateNode? _filter;
    private readonly string _projectionVersion;

    /// <summary>
    /// Creates a folded aggregation projection over raw <c>byte[]</c> source
    /// values.
    /// </summary>
    /// <param name="groupKeySelector">
    /// Maps a source entry's value bytes to the group key it belongs to. Must not
    /// be <see langword="null"/>. A group key that is empty or begins with the
    /// reserved NUL (<c>\u0000</c>) prefix (which the maintainer reserves for its
    /// internal accumulator rows) is rejected: the contribution is dropped and
    /// counted via the <c>orleans.lattice.view.aggregation_rejected</c> metric
    /// rather than materialised.
    /// </param>
    /// <param name="initial">
    /// Produces the empty accumulator for a group. Must not be
    /// <see langword="null"/> and must return an equal fresh value each call.
    /// </param>
    /// <param name="apply">
    /// Folds one member <c>(accumulator, sourceKey, sourceValue, hlc)</c> into the
    /// running accumulator. Must not be <see langword="null"/> and must be pure.
    /// </param>
    /// <param name="foldVersion">
    /// Stable tag identifying the fold's logic, folded into
    /// <see cref="ILatticeAggregationProjection.ProjectionVersion"/>. Required
    /// because the delegates cannot be structurally hashed.
    /// </param>
    /// <param name="filter">
    /// Optional value filter (the predicate IR produced by
    /// <c>LatticePredicateTranslator</c>). When <see langword="null"/> every source
    /// entry participates.
    /// </param>
    public LatticeFoldProjection(
        Func<byte[], string> groupKeySelector,
        Func<byte[]> initial,
        Func<byte[], string, byte[], HybridLogicalClock, byte[]> apply,
        string foldVersion,
        LatticePredicateNode? filter = null)
    {
        ArgumentNullException.ThrowIfNull(groupKeySelector);
        ArgumentNullException.ThrowIfNull(initial);
        ArgumentNullException.ThrowIfNull(apply);
        ArgumentException.ThrowIfNullOrEmpty(foldVersion);

        _groupKeySelector = groupKeySelector;
        _initial = initial;
        _apply = apply;
        _filter = filter;
        _projectionVersion = ComputeVersion(foldVersion, filter);
    }

    /// <summary>
    /// Creates a folded aggregation projection whose selectors and fold run against
    /// the deserialized source value type <typeparamref name="TValue"/> and a
    /// typed accumulator <typeparamref name="TAccumulator"/>. The supplied
    /// serializers (or <see cref="JsonLatticeSerializer{T}.Default"/> when omitted)
    /// bridge the typed fold to the maintainer's opaque <c>byte[]</c> accumulator,
    /// so an operator writes <c>(acc, key, v, hlc) =&gt; ...</c> over domain types
    /// rather than raw bytes.
    /// </summary>
    /// <typeparam name="TValue">The source value type the selectors and fold operate on.</typeparam>
    /// <typeparam name="TAccumulator">The accumulator type the fold produces.</typeparam>
    /// <param name="groupKeySelector">Maps a deserialized source value to its group key. Must not be <see langword="null"/>.</param>
    /// <param name="initial">Produces the empty typed accumulator for a group. Must not be <see langword="null"/>.</param>
    /// <param name="apply">Folds one member into the typed accumulator. Must not be <see langword="null"/>.</param>
    /// <param name="foldVersion">Stable tag identifying the fold's logic. Required.</param>
    /// <param name="filter">Optional value filter. When <see langword="null"/> every source entry participates.</param>
    /// <param name="valueSerializer">Deserializes source values to <typeparamref name="TValue"/>. Defaults to <see cref="JsonLatticeSerializer{T}.Default"/>.</param>
    /// <param name="accumulatorSerializer">Round-trips the accumulator to <c>byte[]</c>. Defaults to <see cref="JsonLatticeSerializer{T}.Default"/>.</param>
    public static LatticeFoldProjection Create<TValue, TAccumulator>(
        Func<TValue, string> groupKeySelector,
        Func<TAccumulator> initial,
        Func<TAccumulator, string, TValue, HybridLogicalClock, TAccumulator> apply,
        string foldVersion,
        LatticePredicateNode? filter = null,
        ILatticeSerializer<TValue>? valueSerializer = null,
        ILatticeSerializer<TAccumulator>? accumulatorSerializer = null)
    {
        ArgumentNullException.ThrowIfNull(groupKeySelector);
        ArgumentNullException.ThrowIfNull(initial);
        ArgumentNullException.ThrowIfNull(apply);
        valueSerializer ??= JsonLatticeSerializer<TValue>.Default;
        accumulatorSerializer ??= JsonLatticeSerializer<TAccumulator>.Default;

        return new LatticeFoldProjection(
            groupKeySelector: bytes => groupKeySelector(valueSerializer.Deserialize(bytes)),
            initial: () => accumulatorSerializer.Serialize(initial()),
            apply: (acc, sourceKey, value, hlc) => accumulatorSerializer.Serialize(
                apply(accumulatorSerializer.Deserialize(acc), sourceKey, valueSerializer.Deserialize(value), hlc)),
            foldVersion: foldVersion,
            filter: filter);
    }

    /// <inheritdoc />
    public AggregationKind Aggregation => AggregationKind.Fold;

    /// <inheritdoc />
    public string ProjectionVersion => _projectionVersion;

    /// <inheritdoc />
    public byte[] Initial() => _initial();

    /// <inheritdoc />
    public byte[] Apply(byte[] accumulator, string sourceKey, byte[] sourceValue, HybridLogicalClock timestamp)
    {
        ArgumentNullException.ThrowIfNull(accumulator);
        ArgumentNullException.ThrowIfNull(sourceKey);
        ArgumentNullException.ThrowIfNull(sourceValue);
        return _apply(accumulator, sourceKey, sourceValue, timestamp);
    }

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

                yield return AggregationContribution.Fold(
                    _groupKeySelector(mutation.Value),
                    mutation.Key,
                    mutation.Value,
                    mutation.Timestamp);
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

                // An unconstrained range delete cannot be lowered to exact per-key
                // retractions without a reverse index: ask the maintainer to
                // reconcile the affected range by rebuilding.
                if (!string.IsNullOrEmpty(mutation.EndExclusiveKey))
                {
                    yield return AggregationContribution.RangeReconcile(mutation.Key, mutation.EndExclusiveKey, mutation.Timestamp);
                }

                break;

            default:
                yield break;
        }
    }

    private static string ComputeVersion(string foldVersion, LatticePredicateNode? filter)
    {
        var builder = new StringBuilder();
        builder.Append("fold-v1|fold=").Append(foldVersion);
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
