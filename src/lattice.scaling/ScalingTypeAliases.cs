namespace Orleans.Lattice.Scaling;

/// <summary>
/// Centralised Orleans serialization alias constants for every type that
/// participates in the <c>Orleans.Lattice.Scaling</c> wire format. Each alias
/// is a short, fixed string that provides a stable wire-format identity
/// independent of CLR type names.
/// <para>
/// The constants live in this package rather than the core
/// <c>Orleans.Lattice.TypeAliases</c> table because the core
/// <c>TypeAliasesTests.Every_alias_constant_is_referenced_by_exactly_one_type</c>
/// gate is scoped to the core assembly: a constant declared in core but
/// referenced only from a type in this (separate) assembly would be flagged
/// as dead. The <c>Orleans.Lattice.Replication</c> package follows the same
/// pattern with its own <c>ReplicationTypeAliases</c> table. The scaling
/// aliases keep the canonical <c>ol.</c> prefix and are verified unique
/// against the core table at authoring time; <c>ScalingTypeAliasesTests</c>
/// enforces the prefix, length, uniqueness, and single-reference invariants
/// for this assembly.
/// </para>
/// </summary>
public static class ScalingTypeAliases
{
    /// <summary>Alias for <see cref="ScalingSignal"/>.</summary>
    public const string ScalingSignal = "ol.scs";

    /// <summary>Alias for <see cref="ComputePressure"/>.</summary>
    public const string ComputePressure = "ol.scp";

    /// <summary>Alias for <see cref="StoragePressure"/>.</summary>
    public const string StoragePressure = "ol.stp";

    /// <summary>Alias for <see cref="WalAccountPressure"/>.</summary>
    public const string WalAccountPressure = "ol.wap";

    /// <summary>Alias for <see cref="WalRebalanceRecommendation"/>.</summary>
    public const string WalRebalanceRecommendation = "ol.wrr";

    /// <summary>Alias for <see cref="WalPressureClassification"/>.</summary>
    public const string WalPressureClassification = "ol.wpc";
}
