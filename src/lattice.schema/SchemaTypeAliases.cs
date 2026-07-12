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

    /// <summary>Alias for <see cref="LatticeSchemaPolicy"/>.</summary>
    internal const string LatticeSchemaPolicy = "ols.sp";

    /// <summary>Alias for <see cref="LatticeSchemaRule"/>.</summary>
    internal const string LatticeSchemaRule = "ols.sr";

    /// <summary>Alias for <see cref="LatticeSchemaRuleKind"/>.</summary>
    internal const string LatticeSchemaRuleKind = "ols.rk";

    /// <summary>Alias for <see cref="LatticeSchemaEncodingKind"/>.</summary>
    internal const string LatticeSchemaEncodingKind = "ols.ek";

    /// <summary>Alias for <see cref="LatticeSchemaDeadLetterEntry"/>.</summary>
    internal const string LatticeSchemaDeadLetterEntry = "ols.dl";

    /// <summary>Alias for <see cref="LatticeSchemaDeadLetterSource"/>.</summary>
    internal const string LatticeSchemaDeadLetterSource = "ols.ds";

    /// <summary>Alias for <see cref="LatticeSchemaViolationException"/>.</summary>
    internal const string LatticeSchemaViolationException = "ols.sv";

    /// <summary>Alias for <see cref="ILatticeSchemaRemediationGrain"/>.</summary>
    internal const string ILatticeSchemaRemediationGrain = "ols.rg";

    /// <summary>Alias for <see cref="LatticeSchemaRemediationReport"/>.</summary>
    internal const string LatticeSchemaRemediationReport = "ols.rr";

    /// <summary>Alias for <see cref="LatticeSchemaRemediationPhase"/>.</summary>
    internal const string LatticeSchemaRemediationPhase = "ols.rp";

    /// <summary>Alias for <see cref="SchemaRemediationState"/>.</summary>
    internal const string SchemaRemediationState = "ols.rs";

    /// <summary>Alias for <see cref="LatticeSchemaVersionConfig"/>.</summary>
    internal const string LatticeSchemaVersionConfig = "ols.vc";

    /// <summary>Alias for <see cref="SchemaRemediationMode"/>.</summary>
    internal const string SchemaRemediationMode = "ols.rm";
}
