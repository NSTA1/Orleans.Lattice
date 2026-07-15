namespace Orleans.Lattice.Schema;

/// <summary>
/// A single row of a compliance-audit breakdown: the number of scanned values that
/// failed the current policy for one distinct failure <see cref="Reason"/> (the
/// reason string reported by the first failing rule). The breakdown groups the
/// non-compliant population by reason so an operator can see <i>which</i> rule the
/// existing data violates, not just how many rows are non-compliant.
/// </summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaComplianceRuleCount)]
[Immutable]
public readonly record struct LatticeSchemaComplianceRuleCount
{
    /// <summary>
    /// The failure reason shared by every value counted here - the reason string of
    /// the first rule those values failed.
    /// </summary>
    [Id(0)] public required string Reason { get; init; }

    /// <summary>The number of scanned values that failed with this <see cref="Reason"/>.</summary>
    [Id(1)] public required int Count { get; init; }
}
