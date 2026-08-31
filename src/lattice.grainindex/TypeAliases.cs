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
/// within this table, and exactly one referencing type. A new alias must also be
/// checked for collisions against the core and sibling-package tables at
/// authoring time, because Orleans resolves aliases from a single cluster-wide
/// registry.
/// </para>
/// <para>
/// The table names every type this package puts on the wire or into durable
/// storage, in the categories the package is built from: the persisted
/// declaration shapes and their fingerprint, the index-registry and enrolment
/// records, the projection and entry-encoding types, the backfill checkpoint and
/// its grain interface, the administrative report surface, the query match
/// record, and the package's serializable exceptions. Add one constant here per
/// <c>[GenerateSerializer]</c> type introduced, and per grain interface that
/// needs a stable wire identity.
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

    /// <summary>Alias for <see cref="GrainIndexFingerprint"/>.</summary>
    internal const string GrainIndexFingerprint = "ol.gif";

    /// <summary>Alias for <c>GrainIndexRegistryRecord</c>.</summary>
    internal const string GrainIndexRegistryRecord = "ol.gir";

    /// <summary>Alias for <see cref="GrainIndexConfigurationDriftException"/>.</summary>
    internal const string GrainIndexConfigurationDriftException = "ol.gid";

    /// <summary>Alias for <see cref="GrainIndexReplicationNotAllowedException"/>.</summary>
    internal const string GrainIndexReplicationNotAllowedException = "ol.gin";

    /// <summary>Alias for <see cref="GrainIndexMatch"/>.</summary>
    internal const string GrainIndexMatch = "ol.gqm";

    /// <summary>Alias for <see cref="GrainIndexPropertyNotIndexedException"/>.</summary>
    internal const string GrainIndexPropertyNotIndexedException = "ol.gqn";

    /// <summary>Alias for <c>GrainIndexEnrollmentRecord</c>.</summary>
    internal const string GrainIndexEnrollmentRecord = "ol.gxs";

    /// <summary>Alias for <c>GrainIndexPendingProjection</c>.</summary>
    internal const string GrainIndexPendingProjection = "ol.gxo";

    /// <summary>Alias for <see cref="GrainIndexBackfillStatus"/>.</summary>
    internal const string GrainIndexBackfillStatus = "ol.gbs";

    /// <summary>Alias for <see cref="GrainIndexBackfillBatchResult"/>.</summary>
    internal const string GrainIndexBackfillBatchResult = "ol.gbb";

    /// <summary>Alias for <c>GrainIndexBackfillCheckpoint</c>.</summary>
    internal const string GrainIndexBackfillCheckpoint = "ol.gbc";

    /// <summary>Alias for <c>IGrainIndexBackfillGrain</c>.</summary>
    internal const string IGrainIndexBackfillGrain = "ol.gbg";

    /// <summary>Alias for <see cref="GrainIndexStatus"/>.</summary>
    internal const string GrainIndexStatus = "ol.gas";

    /// <summary>Alias for <see cref="GrainIndexProgress"/>.</summary>
    internal const string GrainIndexProgress = "ol.gap";

    /// <summary>Alias for <see cref="GrainIndexDriftStatus"/>.</summary>
    internal const string GrainIndexDriftStatus = "ol.gds";

    /// <summary>Alias for <see cref="GrainIndexNotDeclaredException"/>.</summary>
    internal const string GrainIndexNotDeclaredException = "ol.gan";
}
