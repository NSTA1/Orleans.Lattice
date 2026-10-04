namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Cluster-wide durable set of the <see cref="IWalOffsetConsumer"/> grains that
/// read one physical tree's WAL, keyed by that physical tree id (issue #4579).
/// The WAL GC reads the set on every pass and asks each member for its durable
/// read position, so a consumer protects its unread entries from a GC pass on
/// any silo, and across a restart, without reporting into the process-local
/// <see cref="IWalCursorRegistry"/>.
/// <para>
/// A consumer registers before it reads the log, so there is no window in which
/// it has read entries the GC cannot see it needs. Registration is idempotent
/// and is written only when the set changes, so a steady-state consumer costs
/// nothing here.
/// </para>
/// </summary>
[Alias(TypeAliases.IWalOffsetConsumerRegistryGrain)]
internal interface IWalOffsetConsumerRegistryGrain : IGrainWithStringKey
{
    /// <summary>Adds <paramref name="consumer"/> to the set, persisting the change before returning.</summary>
    /// <param name="consumer">The consumer grain, which must implement <see cref="IWalOffsetConsumer"/>.</param>
    Task RegisterAsync(GrainId consumer);

    /// <summary>Removes <paramref name="consumer"/> from the set. A consumer that is not registered is a no-op.</summary>
    /// <param name="consumer">The consumer grain to remove.</param>
    Task UnregisterAsync(GrainId consumer);

    /// <summary>Returns the registered consumers.</summary>
    Task<IReadOnlyList<GrainId>> GetConsumersAsync();
}
