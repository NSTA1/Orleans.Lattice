using Orleans.Concurrency;

namespace Orleans.Lattice.Schema;

/// <summary>
/// The cluster-wide record of how far one schema configuration tree has moved. One
/// activation per configuration tree, keyed by that tree's id
/// (<c>sys-schema-policy</c> or <c>sys-schema-version</c>): the silo that commits a
/// write to the tree advances it, and every silo reads it to learn of a write it did
/// not observe itself. The core change feed fires only on the silo hosting the leaf
/// that committed a write, so without it every other silo would keep its cached
/// configuration (#5238).
/// </summary>
[Alias(SchemaTypeAliases.ISchemaConfigEpochGrain)]
internal interface ISchemaConfigEpochGrain : IGrainWithStringKey
{
    /// <summary>Returns the current epoch.</summary>
    /// <returns>The current epoch.</returns>
    [ReadOnly]
    Task<SchemaConfigEpoch> GetAsync();

    /// <summary>Advances the epoch after a committed configuration write.</summary>
    /// <returns>The advanced epoch.</returns>
    Task<SchemaConfigEpoch> AdvanceAsync();
}
