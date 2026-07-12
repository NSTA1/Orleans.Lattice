namespace Orleans.Lattice.Schema;

/// <summary>
/// The public value-evaluation helper over the <see cref="LatticeValueTransform"/>
/// IR. The schema-enforcement remediation and schema-versioning upcast layers use
/// it to rewrite a value's JSON document server-side, without reimplementing JSON
/// evaluation or taking a dependency on the internal evaluator. It is the
/// transform-side sibling of <see cref="LatticePredicateEvaluation"/>.
/// </summary>
/// <remarks>
/// The contract is "the bytes are a UTF-8 JSON document, in and out". The
/// evaluator is deterministic and total per value, reading members from the input
/// document (ordinal, case-insensitive) and producing a fresh UTF-8 JSON
/// document. A null, empty, or malformed payload throws
/// <see cref="InvalidOperationException"/> rather than silently corrupting the
/// value, so a consumer can abort a shadow build cleanly.
/// </remarks>
public static class LatticeValueTransformEvaluation
{
    /// <summary>
    /// Evaluates <paramref name="transform"/> against the JSON document in
    /// <paramref name="value"/> and returns the resulting UTF-8 JSON document.
    /// </summary>
    /// <param name="value">
    /// The value bytes, interpreted as a UTF-8 JSON document.
    /// </param>
    /// <param name="transform">The transform IR tree to apply to the document.</param>
    /// <returns>The rewritten UTF-8 JSON document.</returns>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="value"/> is null, empty, or not a well-formed JSON
    /// document, or the transform is structurally invalid.
    /// </exception>
    public static byte[] Evaluate(byte[]? value, in LatticeValueTransform transform) =>
        LatticeValueTransformEvaluator.Evaluate(value, in transform);
}
