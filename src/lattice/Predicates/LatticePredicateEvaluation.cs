namespace Orleans.Lattice;

/// <summary>
/// The public value-evaluation helper over the predicate IR. A companion package
/// (for example a schema-enforcement add-on registered through
/// <see cref="ILatticeWriteInterceptor"/>) uses it to decide whether a value's
/// JSON document satisfies a <see cref="LatticePredicateNode"/> tree, without
/// reimplementing JSON evaluation or taking a dependency on the internal
/// evaluator.
/// </summary>
/// <remarks>
/// <para>
/// The contract is purely "the bytes are a UTF-8 JSON document": the evaluator
/// never sees the value type or the serializer. Property-name matching against
/// the document is ordinal and case-insensitive. The predicate is strictly
/// subtractive - a null or empty payload, or one that does not parse as JSON,
/// evaluates to <c>false</c>.
/// </para>
/// <para>
/// This is a tightly-scoped public surface: it exposes only the single-value
/// match already implemented by the internal evaluator, so downstream enforcement
/// packages share exactly the server-side semantics used for push-down.
/// </para>
/// </remarks>
public static class LatticePredicateEvaluation
{
    /// <summary>
    /// Evaluates <paramref name="predicate"/> against the JSON document in
    /// <paramref name="value"/>.
    /// </summary>
    /// <param name="value">
    /// The value bytes, interpreted as a UTF-8 JSON document. A <c>null</c> or
    /// empty payload, or one that does not parse as JSON, evaluates to
    /// <c>false</c>.
    /// </param>
    /// <param name="predicate">The predicate IR tree to fold against the document.</param>
    /// <returns><c>true</c> when the document satisfies the predicate; otherwise <c>false</c>.</returns>
    public static bool Matches(byte[]? value, in LatticePredicateNode predicate) =>
        LatticePredicateEvaluator.Matches(value, in predicate);
}
