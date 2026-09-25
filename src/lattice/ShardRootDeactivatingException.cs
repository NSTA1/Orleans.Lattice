namespace Orleans.Lattice;

/// <summary>
/// Thrown by <c>ShardRootGrain</c> when a point write reaches an activation
/// that has already requested its own deactivation, before the write touches
/// any leaf. Nothing was written, so the caller retries the same operation
/// after a short backoff, and the retry lands on the next activation once the
/// Orleans directory re-places the grain.
/// <para>
/// Point writes on a shard root interleave (issue #812). An activation that is
/// deactivating waits for its running requests to finish, and no longer serves
/// new calls. A point write admitted in that window would dispatch to its leaf,
/// and the leaf's persist path calls back into the owning shard root to publish
/// its byte footprint. That callback cannot be served, the write stalls until
/// the response timeout, and a split carried back on the timed-out reply is
/// lost. Refusing the write up front removes the cycle.
/// </para>
/// <para>
/// This exception is part of the internal coordination protocol between
/// <c>LatticeGrain</c> and <c>ShardRootGrain</c>. It is classified as transient
/// silo churn by <c>ShardActivationRetry.IsTransientSiloChurn</c>, so every
/// retry arm that already absorbs a forward-to-deactivating rejection absorbs
/// it too, and external callers do not see it.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardRootDeactivating)]
internal sealed class ShardRootDeactivatingException : Exception
{
    /// <summary>Creates a new <see cref="ShardRootDeactivatingException"/> for the given shard root.</summary>
    /// <param name="shardKey">The grain key of the deactivating shard root.</param>
    public ShardRootDeactivatingException(string shardKey)
        : base($"Shard root '{shardKey}' is deactivating and refused a point write before dispatching it. Nothing was written; retry the operation.")
    {
        ShardKey = shardKey;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public ShardRootDeactivatingException() { }

    /// <summary>The grain key of the deactivating shard root.</summary>
    [Id(0)] public string ShardKey { get; set; } = string.Empty;
}
