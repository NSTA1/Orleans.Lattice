namespace Orleans.Lattice.Schema;

/// <summary>
/// Selects the computed-field operation of a
/// <see cref="LatticeValueTransformKind.Compute"/> value expression. The v1
/// allowlist is deliberately tight: string concatenation and null-coalescing
/// cover the common computed-field cases (building a display name, filling a
/// default from a fallback member). Arithmetic operators are intentionally
/// deferred until a consumer needs them.
/// </summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeComputeOperator)]
public enum LatticeComputeOperator : byte
{
    /// <summary>
    /// Concatenate every operand, rendered as its string form, left to right.
    /// A missing or null operand contributes the empty string.
    /// </summary>
    Concat = 0,

    /// <summary>
    /// Yield the first operand that is neither missing nor JSON null; if every
    /// operand is missing or null, yield null.
    /// </summary>
    Coalesce = 1,
}
