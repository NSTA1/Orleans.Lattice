using System.Linq.Expressions;

namespace Orleans.Lattice;

/// <summary>
/// Client-side helper that the typed predicate overloads use to turn a
/// caller's <c>Expression&lt;Func&lt;T, bool&gt;&gt;</c> into a serializable
/// <see cref="LatticePredicateNode"/> IR, gating on the serializer's
/// <see cref="ILatticePredicateSerializer"/> capability first.
/// <para>
/// The capability check happens here, <b>before any RPC</b>: a serializer that
/// cannot expose a navigable JSON document throws a clear
/// <see cref="NotSupportedException"/> at the call site rather than shipping an
/// IR the server cannot evaluate.
/// </para>
/// </summary>
internal static class LatticePredicatePushdown
{
    /// <summary>
    /// Validates that <paramref name="serializer"/> supports predicate
    /// push-down and translates <paramref name="predicate"/> to the IR.
    /// </summary>
    /// <exception cref="ArgumentNullException">A required argument is <c>null</c>.</exception>
    /// <exception cref="NotSupportedException">
    /// The serializer does not implement <see cref="ILatticePredicateSerializer"/>,
    /// or the expression contains a construct outside the allowlist.
    /// </exception>
    public static LatticePredicateNode Compile<T>(Expression<Func<T, bool>> predicate, ILatticeSerializer<T> serializer)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(serializer);

        if (serializer is not ILatticePredicateSerializer)
        {
            throw new NotSupportedException(
                $"Predicate push-down is unsupported for serializer '{serializer.GetType().Name}'. " +
                "The serializer must implement ILatticePredicateSerializer (for example JsonLatticeSerializer<T>) " +
                "so the server can evaluate the predicate against a navigable JSON document.");
        }

        return LatticePredicateTranslator.Translate(predicate);
    }
}
