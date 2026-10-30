namespace Orleans.Lattice.Schema;

/// <summary>
/// The cluster-wide version of one schema configuration tree
/// (<c>sys-schema-policy</c> or <c>sys-schema-version</c>) that every silo's cached
/// provider is measured against. Held in memory by the
/// <see cref="ISchemaConfigEpochGrain"/> for that tree and advanced after every
/// committed write to it.
/// </summary>
/// <remarks>
/// <see cref="Incarnation"/> is minted afresh by every activation of the epoch
/// grain, so a restarted grain can never report a version a silo has already
/// observed: a silo treats any change of incarnation as a configuration change and
/// drops its cache. A lost counter can therefore only ever cost a reload, never make
/// a stale silo look current.
/// </remarks>
/// <param name="Incarnation">The identity of the epoch-grain activation that minted this epoch.</param>
/// <param name="Version">The monotonic version within <paramref name="Incarnation"/>.</param>
[GenerateSerializer]
[Immutable]
[Alias(SchemaTypeAliases.SchemaConfigEpoch)]
internal readonly record struct SchemaConfigEpoch(
    [property: Id(0)] Guid Incarnation,
    [property: Id(1)] long Version);
