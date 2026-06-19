using System.IO.Hashing;
using System.Text;

namespace Orleans.Lattice;

/// <summary>
/// Built-in <see cref="ILatticeViewProjection"/> for Phase 1 filter /
/// re-project views. It keeps the subset of source keys whose value satisfies an
/// optional <see cref="LatticePredicateNode"/> filter, optionally transforms the
/// stored value, and optionally re-keys the view entry through an injective key
/// map. Key-preserving (view key == source key, value verbatim) is the default
/// when no selectors are supplied.
/// <para>
/// <b>Retraction.</b> A source <c>Set</c> whose value no longer satisfies the
/// filter (or whose value selector returns <see langword="null"/>) emits a view
/// <see cref="ViewWriteKind.Delete"/> rather than nothing, so a key that updates
/// its way out of the predicate is removed from the view and the projection
/// converges. Deleting a view key the view never held is an idempotent no-op.
/// </para>
/// <para>
/// <b>Determinism.</b> The filter is the wire-stable predicate IR, evaluated
/// against the value's JSON document view exactly as the server-side scan path
/// evaluates it, so the projection is a pure function of the mutation.
/// <see cref="ProjectionVersion"/> is a structural hash of the filter plus the
/// caller-declared selector version tags; changing the filter or bumping a
/// selector tag changes the version and triggers a rebuild.
/// </para>
/// <para>
/// <b>Re-key rule.</b> The key re-map is a pure function of the <i>source key</i>
/// (<c>Func&lt;string, string&gt;</c>), never the value. This is what makes a
/// delete re-keyable: a <see cref="MutationKind.Delete"/> / tombstone carries the
/// source key but not the value, so the view key is recomputed directly from the
/// source key. The value selector still transforms the stored <i>value</i>;
/// value-derived keying (secondary indexes, aggregation) is a separate view
/// kind and out of scope here. The re-map must also be <b>injective</b> - two
/// source keys mapping to one view key is a configuration error the maintainer
/// surfaces via a collision metric, falling back to source-HLC last-writer-wins
/// so the view stays well-defined. Every point write is stamped with its
/// originating <see cref="ViewWrite.SourceKey"/> so the maintainer can detect
/// such collisions.
/// </para>
/// <para>
/// <b>Range deletes.</b> A <see cref="MutationKind.DeleteRange"/> that carries
/// <see cref="LatticeMutation.MatchedKeys"/> (predicate-filtered deletes do) is
/// lowered to one exact per-key <see cref="ViewWriteKind.Delete"/> per matched
/// source key, re-keyed as usual. An unconstrained range delete (no matched
/// keys) is lowered to a single <see cref="ViewWriteKind.RangeDelete"/> for a
/// key-preserving view - the view's slice of the range is exactly the affected
/// entries - or a <see cref="ViewWriteKind.RangeReconcile"/> for a re-keyed
/// view, whose scattered view keys cannot be recovered without a reverse index.
/// </para>
/// </summary>
public sealed class PredicateLatticeViewProjection : ILatticeViewProjection
{
    private readonly LatticePredicateNode? _filter;
    private readonly Func<byte[]?, byte[]?>? _valueSelector;
    private readonly Func<string, string>? _keySelector;
    private readonly string _projectionVersion;

    /// <summary>
    /// Creates a predicate projection.
    /// </summary>
    /// <param name="filter">
    /// Optional value filter (the predicate IR produced by
    /// <c>LatticePredicateTranslator</c>). When <see langword="null"/> every
    /// source key is kept.
    /// </param>
    /// <param name="valueSelector">
    /// Optional value transform applied to a kept entry's value. When
    /// <see langword="null"/> the source value is stored verbatim. Returning
    /// <see langword="null"/> drops the write.
    /// </param>
    /// <param name="keySelector">
    /// Optional injective re-map from source key to view key. When
    /// <see langword="null"/> the view key equals the source key.
    /// </param>
    /// <param name="valueSelectorVersion">
    /// Stable tag identifying the value-selector logic, folded into
    /// <see cref="ProjectionVersion"/>. Required when
    /// <paramref name="valueSelector"/> is supplied so a selector change is
    /// detectable (delegates are not structurally hashable).
    /// </param>
    /// <param name="keySelectorVersion">
    /// Stable tag identifying the key-selector logic, folded into
    /// <see cref="ProjectionVersion"/>. Required when
    /// <paramref name="keySelector"/> is supplied.
    /// </param>
    public PredicateLatticeViewProjection(
        LatticePredicateNode? filter = null,
        Func<byte[]?, byte[]?>? valueSelector = null,
        Func<string, string>? keySelector = null,
        string? valueSelectorVersion = null,
        string? keySelectorVersion = null)
    {
        if (valueSelector is not null && string.IsNullOrEmpty(valueSelectorVersion))
        {
            throw new ArgumentException(
                "A value-selector version tag is required when a value selector is supplied.",
                nameof(valueSelectorVersion));
        }

        if (keySelector is not null && string.IsNullOrEmpty(keySelectorVersion))
        {
            throw new ArgumentException(
                "A key-selector version tag is required when a key selector is supplied.",
                nameof(keySelectorVersion));
        }

        _filter = filter;
        _valueSelector = valueSelector;
        _keySelector = keySelector;
        _projectionVersion = ComputeVersion(filter, valueSelectorVersion, keySelectorVersion);
    }

    /// <inheritdoc />
    public string ProjectionVersion => _projectionVersion;

    /// <summary>
    /// Creates a predicate projection whose value transform runs against the
    /// deserialized value type <typeparamref name="T"/> instead of raw
    /// <c>byte[]</c>. The supplied <paramref name="serializer"/> (or
    /// <see cref="JsonLatticeSerializer{T}.Default"/> when omitted) deserializes
    /// each source value before <paramref name="valueSelector"/> sees it and
    /// re-serializes the transformed result for storage, so an operator writes a
    /// typed transform rather than hand-rolling <c>byte[]</c> round-trips.
    /// </summary>
    /// <typeparam name="T">The source value type the transform operates on.</typeparam>
    /// <param name="filter">
    /// Optional value filter (the predicate IR produced by
    /// <c>LatticePredicateTranslator</c>). When <see langword="null"/> every
    /// source key is kept.
    /// </param>
    /// <param name="valueSelector">
    /// Optional typed value transform applied to a kept entry's deserialized
    /// value; the result is re-serialized for storage. When <see langword="null"/>
    /// the source value is stored verbatim.
    /// </param>
    /// <param name="keySelector">
    /// Optional injective re-map from source key to view key. When
    /// <see langword="null"/> the view key equals the source key.
    /// </param>
    /// <param name="valueSelectorVersion">
    /// Stable tag identifying the value-selector logic, folded into
    /// <see cref="ProjectionVersion"/>. Required when
    /// <paramref name="valueSelector"/> is supplied.
    /// </param>
    /// <param name="keySelectorVersion">
    /// Stable tag identifying the key-selector logic, folded into
    /// <see cref="ProjectionVersion"/>. Required when
    /// <paramref name="keySelector"/> is supplied.
    /// </param>
    /// <param name="serializer">
    /// Serializes and deserializes source values to <typeparamref name="T"/>.
    /// Defaults to <see cref="JsonLatticeSerializer{T}.Default"/> when
    /// <see langword="null"/>.
    /// </param>
    public static PredicateLatticeViewProjection Create<T>(
        LatticePredicateNode? filter = null,
        Func<T, T>? valueSelector = null,
        Func<string, string>? keySelector = null,
        string? valueSelectorVersion = null,
        string? keySelectorVersion = null,
        ILatticeSerializer<T>? serializer = null)
    {
        serializer ??= JsonLatticeSerializer<T>.Default;

        Func<byte[]?, byte[]?>? wrapped = valueSelector is null
            ? null
            : bytes => bytes is null ? null : serializer.Serialize(valueSelector(serializer.Deserialize(bytes)));

        return new PredicateLatticeViewProjection(filter, wrapped, keySelector, valueSelectorVersion, keySelectorVersion);
    }

    /// <inheritdoc />
    public IEnumerable<ViewWrite> Project(LatticeMutation mutation)
    {
        switch (mutation.Kind)
        {
            case MutationKind.Set:
                if (_filter is { } filter && !LatticePredicateEvaluator.Matches(mutation.Value, filter))
                {
                    // Retraction: a key whose value no longer satisfies the filter
                    // must be removed from the view, otherwise an update that moves
                    // a key out of the predicate would leave a stale entry. Deleting
                    // a view key the view never held is an idempotent no-op.
                    yield return ViewWrite.Delete(MapKey(mutation.Key), mutation.Timestamp, mutation.Key);
                    yield break;
                }

                var value = _valueSelector is null ? mutation.Value : _valueSelector(mutation.Value);
                if (value is null)
                {
                    // A value selector that drops the entry (returns null) retracts
                    // the view key for the same convergence reason as a filter miss.
                    yield return ViewWrite.Delete(MapKey(mutation.Key), mutation.Timestamp, mutation.Key);
                    yield break;
                }

                yield return ViewWrite.Upsert(MapKey(mutation.Key), value, mutation.Timestamp, mutation.ExpiresAtTicks, mutation.Key);
                break;

            case MutationKind.Delete:
            case MutationKind.Tombstone:
                // Deletes and tombstone reaps propagate unconditionally: removing
                // a key the view never held is an idempotent no-op, and the
                // filter cannot be evaluated against a value-less tombstone. The
                // view key is recomputed from the source key (the re-key rule),
                // which is exactly why a re-keyed delete is resolvable.
                yield return ViewWrite.Delete(MapKey(mutation.Key), mutation.Timestamp, mutation.Key);
                break;

            case MutationKind.DeleteRange:
                // A range delete that records the exact matched keys (the
                // predicate-filtered case) lowers to one precise per-key delete
                // per matched source key, re-keyed as usual - exact for any view
                // shape, no predicate re-evaluation.
                if (mutation.MatchedKeys is { Count: > 0 } matched)
                {
                    foreach (var key in matched)
                    {
                        yield return ViewWrite.Delete(MapKey(key), mutation.Timestamp, key);
                    }

                    yield break;
                }

                // An unconstrained range delete (no matched-key set) needs the
                // whole range removed. EndExclusiveKey is required to bound it;
                // without it there is nothing actionable.
                if (string.IsNullOrEmpty(mutation.EndExclusiveKey))
                {
                    yield break;
                }

                if (_keySelector is null)
                {
                    // Key-preserving: the view key equals the source key, so the
                    // view's slice of [start, end) is exactly the affected
                    // entries. A single view-side range delete is exact.
                    yield return ViewWrite.RangeDelete(mutation.Key, mutation.EndExclusiveKey, mutation.Timestamp);
                }
                else
                {
                    // Re-keyed: the deleted source keys' view keys are scattered
                    // and unrecoverable without a reverse index, so ask the
                    // maintainer to reconcile (rebuild) the affected range.
                    yield return ViewWrite.RangeReconcile(mutation.Key, mutation.EndExclusiveKey, mutation.Timestamp);
                }

                break;

            default:
                yield break;
        }
    }

    private string MapKey(string sourceKey) => _keySelector is null ? sourceKey : _keySelector(sourceKey);

    private static string ComputeVersion(
        LatticePredicateNode? filter,
        string? valueSelectorVersion,
        string? keySelectorVersion)
    {
        var builder = new StringBuilder();
        builder.Append("v1|filter=");
        if (filter is { } node)
        {
            AppendNode(builder, node);
        }
        else
        {
            builder.Append("none");
        }

        builder.Append("|value=").Append(valueSelectorVersion ?? "identity");
        builder.Append("|key=").Append(keySelectorVersion ?? "identity");

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
