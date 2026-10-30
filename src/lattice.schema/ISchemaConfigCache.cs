namespace Orleans.Lattice.Schema;

/// <summary>
/// A silo-local cache of per-tree schema configuration that a
/// <see cref="SchemaConfigEpochSynchronizer{TCache}"/> keeps current with changes
/// committed on other silos.
/// </summary>
internal interface ISchemaConfigCache
{
    /// <summary>
    /// Drops every cached entry and overtakes every load still in flight, so the
    /// next lookup of any tree re-reads the durable store.
    /// </summary>
    void InvalidateAll();
}
