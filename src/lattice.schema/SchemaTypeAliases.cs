namespace Orleans.Lattice.Schema;

/// <summary>
/// Centralized Orleans serialization alias constants for the
/// <c>Orleans.Lattice.Schema</c> package. Mirrors the core <c>TypeAliases</c>
/// table and the sibling <c>AuthTypeAliases</c> / <c>MembershipTypeAliases</c>:
/// every constant uses the <c>ols.</c> prefix, is at most 6 characters, and is
/// unique - invariants enforced by <c>SchemaTypeAliasesTests</c>.
/// </summary>
internal static class SchemaTypeAliases
{
    /// <summary>Alias for <see cref="LatticeValueTransform"/>.</summary>
    internal const string LatticeValueTransform = "ols.vt";

    /// <summary>Alias for <see cref="LatticeValueTransformKind"/>.</summary>
    internal const string LatticeValueTransformKind = "ols.vk";

    /// <summary>Alias for <see cref="LatticeComputeOperator"/>.</summary>
    internal const string LatticeComputeOperator = "ols.co";
}
