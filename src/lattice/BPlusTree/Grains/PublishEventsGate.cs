namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-grain-activation cache of the effective <c>PublishEvents</c> flag for a
/// single tree. Resolution order: the
/// <see cref="State.TreeRegistryEntry.PublishEvents"/> override on the registry
/// entry if set, otherwise <see cref="LatticeOptions.PublishEvents"/>.
/// Cached results expire after a short TTL so that runtime changes made via
/// <see cref="ILattice.SetPublishEventsEnabledAsync(bool?, CancellationToken)"/>
/// propagate to activations on other silos within the window. Changes made
/// through the <em>same</em> activation invalidate the cache immediately via
/// <see cref="Invalidate"/>.
/// </summary>
internal sealed class PublishEventsGate
{
    private static readonly TimeSpan CacheTtl = TimeSpan.FromSeconds(5);

    private bool _effective;
    private DateTime _expiresAtUtc = DateTime.MinValue;

    /// <summary>
    /// Resolves the effective flag, reading from the registry at most once per
    /// <see cref="CacheTtl"/>. Returns a <see cref="ValueTask{Boolean}"/> so the
    /// common cache-hit path allocates nothing. Registry lookup failures fall
    /// back to the silo option so event publication never blocks a write when
    /// the registry is briefly unavailable.
    /// </summary>
    public ValueTask<bool> IsEnabledAsync(IGrainFactory grainFactory, string treeId, LatticeOptions options)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(options);

        // System trees (e.g. the registry tree itself) are backed by an ILattice
        // grain that writes into the registry grain's own tree. Routing the
        // gate lookup through the registry would deadlock on the non-reentrant
        // activation that is currently servicing the write. System trees never
        // publish user-visible events, so just honour the silo option directly.
        if (treeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            return new ValueTask<bool>(options.PublishEvents);
        }

        if (DateTime.UtcNow < _expiresAtUtc)
        {
            return new ValueTask<bool>(_effective);
        }

        return RefreshAsync(grainFactory, treeId, options);
    }

    private async ValueTask<bool> RefreshAsync(IGrainFactory grainFactory, string treeId, LatticeOptions options)
    {
        bool effective;
        try
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            var entry = await registry.GetEntryAsync(treeId);
            effective = entry?.PublishEvents ?? options.PublishEvents;
        }
        catch
        {
            // Never fail a write because the registry is unreachable.
            effective = options.PublishEvents;
        }

        _effective = effective;
        _expiresAtUtc = DateTime.UtcNow.Add(CacheTtl);
        return effective;
    }

    /// <summary>
    /// Forces the next <see cref="IsEnabledAsync"/> call to re-read the registry
    /// entry rather than return a cached value. Called after this activation
    /// mutates the per-tree override so subsequent publishes in the same call
    /// path observe the change immediately.
    /// </summary>
    public void Invalidate() => _expiresAtUtc = DateTime.MinValue;
}

