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
/// Collision handling for a non-injective key re-map is a later-phase concern;
/// in Phase 1 two source keys that map to the same view key resolve by
/// last-writer-wins on the source HLC.
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
                    yield return ViewWrite.Delete(MapKey(mutation.Key), mutation.Timestamp);
                    yield break;
                }

                var value = _valueSelector is null ? mutation.Value : _valueSelector(mutation.Value);
                if (value is null)
                {
                    // A value selector that drops the entry (returns null) retracts
                    // the view key for the same convergence reason as a filter miss.
                    yield return ViewWrite.Delete(MapKey(mutation.Key), mutation.Timestamp);
                    yield break;
                }

                yield return ViewWrite.Upsert(MapKey(mutation.Key), value, mutation.Timestamp, mutation.ExpiresAtTicks);
                break;

            case MutationKind.Delete:
            case MutationKind.Tombstone:
                // Deletes and tombstone reaps propagate unconditionally: removing
                // a key the view never held is an idempotent no-op, and the
                // filter cannot be evaluated against a value-less tombstone.
                yield return ViewWrite.Delete(MapKey(mutation.Key), mutation.Timestamp);
                break;

            case MutationKind.DeleteRange:
                // A predicate-filtered range delete records the exact matched keys
                // so the view can tombstone precisely the affected view keys with
                // no predicate re-evaluation. An unconditional range delete (no
                // matched-key set) cannot be lowered to per-key view writes in
                // Phase 1 and is left to the rebuild path.
                if (mutation.MatchedKeys is { Count: > 0 } matched)
                {
                    foreach (var key in matched)
                    {
                        yield return ViewWrite.Delete(MapKey(key), mutation.Timestamp);
                    }
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
