namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Centralised Orleans serialization alias constants for every type that
/// participates in the <c>Orleans.Lattice.GrainIndex</c> wire format. Each
/// alias is a short, fixed string that gives a type a stable wire-format
/// identity independent of its CLR name, so a type can be renamed without
/// breaking a persisted or in-flight payload.
/// <para>
/// The constants live in this package rather than in the core
/// <c>Orleans.Lattice.TypeAliases</c> table because the core
/// <c>TypeAliasesTests.Every_alias_constant_is_referenced_by_exactly_one_type</c>
/// gate is scoped to the core assembly: a constant declared in core but
/// referenced only from a type in this (separate) assembly would be flagged as
/// dead. The <c>Orleans.Lattice.Replication</c> and
/// <c>Orleans.Lattice.Scaling</c> packages follow the same pattern with their
/// own tables.
/// </para>
/// <para>
/// Invariants every constant added here must satisfy, enforced by
/// <c>TypeAliasesTests</c> in this package's test project: the canonical
/// <c>ol.</c> prefix, a total length of at most six characters, uniqueness
/// within this table, and (once the package declares serializable types)
/// exactly one referencing type. A new alias must also be checked for
/// collisions against the core and sibling-package tables at authoring time,
/// because Orleans resolves aliases from a single cluster-wide registry.
/// </para>
/// <para>
/// The table currently names the three types this package puts on the wire or
/// into the index registry: the persisted index descriptor, the persisted
/// projected-property descriptor, and the grain-key encoding failure. Later work
/// adds one constant here per <c>[GenerateSerializer]</c> type it introduces.
/// </para>
/// </summary>
internal static class TypeAliases
{
    /// <summary>
    /// The canonical prefix every alias constant in this table must carry, shared
    /// with the core and sibling-package alias tables so the cluster-wide alias
    /// registry stays recognisably one namespace.
    /// </summary>
    internal const string Prefix = "ol.";

    /// <summary>
    /// The maximum total length, in characters, of an alias constant in this
    /// table (prefix included). Aliases are kept short because every serialized
    /// payload carries one.
    /// </summary>
    internal const int MaxAliasLength = 6;

    /// <summary>Alias for <see cref="GrainIndexDescriptor"/>.</summary>
    internal const string GrainIndexDescriptor = "ol.gix";

    /// <summary>Alias for <see cref="GrainIndexPropertyDescriptor"/>.</summary>
    internal const string GrainIndexPropertyDescriptor = "ol.gip";

    /// <summary>Alias for <see cref="GrainIndexKeyEncodingException"/>.</summary>
    internal const string GrainIndexKeyEncodingException = "ol.gie";

    /// <summary>Alias for <see cref="GrainIndexEntry"/>.</summary>
    internal const string GrainIndexEntry = "ol.gxe";

    /// <summary>Alias for <see cref="GrainIndexProjection"/>.</summary>
    internal const string GrainIndexProjection = "ol.gxp";

    /// <summary>Alias for <see cref="GrainIndexUpdatePlan"/>.</summary>
    internal const string GrainIndexUpdatePlan = "ol.gxu";
}
