using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Carries routing maps across the alias swap of a shadow-cutover restore or a
/// schema remediation cutover (issue #4250), and of an explicit alias set through
/// the tree-administration facade (issue #4263).
/// <para>
/// Routing reads the shard map under the id it is addressed by, and splits,
/// folds and reshards write it there, so for an aliased tree the logical entry
/// holds the only live map and the physical copy's own entry is not consulted.
/// A destination copy is built by routing under its own id, so its layout is
/// described by its own map. Swapping only the alias therefore left the logical
/// tree routing the copy's shards by the replaced tree's map, which reads most
/// keys as absent. A resize already re-stamps the copy's map onto the logical
/// entry at its swap (#3880); these helpers do the same for the other cutovers
/// and carry the replaced tree's map back on a revert.
/// </para>
/// <para>
/// Every write is to the registry from outside its turn, so nothing here can
/// re-enter a registry call (issue #4128). The caller holds the alias-change
/// reservation; each helper runs under a system-origin scope of its own.
/// </para>
/// </summary>
internal static class AliasCutoverShardMaps
{
    /// <summary>
    /// Prepares a cutover of <paramref name="logicalTreeId"/> onto
    /// <paramref name="destinationPhysicalTreeId"/>; call it immediately before the
    /// alias swap. Records the map the logical tree addresses its current physical
    /// tree by on the destination entry (once, so a resumed cutover keeps the
    /// original), stamps that map onto the current physical tree's own entry when
    /// it is not the logical id itself, and carries the destination's own map onto
    /// the logical entry.
    /// </summary>
    /// <returns>
    /// The map the replaced physical tree's shards are addressed by, for arming or
    /// enumerating them; <see langword="null"/> when the alias already resolves to
    /// the destination and no map was recorded (a cutover begun before this
    /// existed), in which case nothing is changed.
    /// </returns>
    public static async Task<ShardMap?> PrepareCutoverAsync(
        IGrainFactory grainFactory,
        string logicalTreeId,
        string destinationPhysicalTreeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(logicalTreeId);
        ArgumentNullException.ThrowIfNull(destinationPhysicalTreeId);

        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();
        var registry = grainFactory.GetLatticeRegistry();
        var current = await registry.ResolveAsync(logicalTreeId);
        var destination = await registry.GetEntryAsync(destinationPhysicalTreeId) ?? new TreeRegistryEntry();

        // The logical map is carried before the swap, so a cutover resumed after it
        // has nothing left to carry: the logical entry now describes the
        // destination, and splits since may have moved it on.
        if (string.Equals(current, destinationPhysicalTreeId, StringComparison.Ordinal))
        {
            return destination.ReplacedShardMap;
        }

        var replaced = destination.ReplacedShardMap;
        if (replaced is null)
        {
            var routing = await grainFactory.GetGrain<ILattice>(logicalTreeId)
                .GetRoutingAsync(forceRefresh: true, cancellationToken);
            var logicalBefore = await registry.GetEntryAsync(logicalTreeId);
            replaced = Copy(routing.Map, routing.Map.Version);
            destination = destination with
            {
                ReplacedShardMap = replaced,
                ReplacedNextShardIndex = logicalBefore?.NextShardIndex,
            };
            await registry.UpdateAsync(destinationPhysicalTreeId, destination);
        }

        // A replaced physical tree with its own id is addressed by the logical
        // map, never its own, so its own entry is stale; once the alias moves off
        // it, that entry is all that describes it to a direct-physical walk.
        if (!string.Equals(current, logicalTreeId, StringComparison.Ordinal)
            && await registry.GetEntryAsync(current) is { } previous)
        {
            await registry.UpdateAsync(current, previous with
            {
                ShardMap = Restamp(replaced, previous.ShardMap),
                NextShardIndex = destination.ReplacedNextShardIndex ?? previous.NextShardIndex,
            });
        }

        var destinationRouting = await grainFactory.GetGrain<ILattice>(destinationPhysicalTreeId)
            .GetRoutingAsync(forceRefresh: true, cancellationToken);
        var logical = await registry.GetEntryAsync(logicalTreeId) ?? new TreeRegistryEntry();
        await registry.UpdateAsync(logicalTreeId, logical with
        {
            ShardMap = Restamp(destinationRouting.Map, logical.ShardMap),
            NextShardIndex = destination.NextShardIndex,
        });

        return replaced;
    }

    /// <summary>
    /// Prepares a revert that moves <paramref name="logicalTreeId"/> off
    /// <paramref name="shadowPhysicalTreeId"/> back onto
    /// <paramref name="previousPhysicalTreeId"/>; call it immediately before the
    /// alias swap. Stamps the logical map, which describes the shadow, onto the
    /// shadow's own entry, then carries the map recorded at the cutover back onto
    /// the logical entry. A no-op unless the alias currently resolves to the shadow.
    /// </summary>
    public static async Task PrepareRevertAsync(
        IGrainFactory grainFactory,
        string logicalTreeId,
        string shadowPhysicalTreeId,
        string previousPhysicalTreeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(logicalTreeId);
        ArgumentNullException.ThrowIfNull(shadowPhysicalTreeId);
        ArgumentNullException.ThrowIfNull(previousPhysicalTreeId);

        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();
        var registry = grainFactory.GetLatticeRegistry();
        if (string.Equals(shadowPhysicalTreeId, previousPhysicalTreeId, StringComparison.Ordinal)
            || !string.Equals(await registry.ResolveAsync(logicalTreeId), shadowPhysicalTreeId, StringComparison.Ordinal))
        {
            return;
        }

        var shadow = await registry.GetEntryAsync(shadowPhysicalTreeId);
        var restored = shadow?.ReplacedShardMap;
        var restoredNextShardIndex = shadow?.ReplacedNextShardIndex;
        if (restored is null)
        {
            // A cutover from before the capture existed. A previous tree with its
            // own id may still describe itself; the logical id's own shards do not.
            if (string.Equals(previousPhysicalTreeId, logicalTreeId, StringComparison.Ordinal)
                || await registry.GetEntryAsync(previousPhysicalTreeId) is not { ShardMap: { } own } previous)
            {
                return;
            }

            restored = own;
            restoredNextShardIndex = previous.NextShardIndex;
        }

        var live = (await grainFactory.GetGrain<ILattice>(logicalTreeId)
            .GetRoutingAsync(forceRefresh: true, cancellationToken)).Map;
        var logical = await registry.GetEntryAsync(logicalTreeId) ?? new TreeRegistryEntry();

        // A revert resumed after the map was carried back would read the restored
        // map here, so leave the shadow's entry as the first pass stamped it.
        if (shadow is not null && !SameSlots(live, restored))
        {
            await registry.UpdateAsync(shadowPhysicalTreeId, shadow with
            {
                ShardMap = Restamp(live, shadow.ShardMap),
                NextShardIndex = logical.NextShardIndex,
            });
        }

        await registry.UpdateAsync(logicalTreeId, logical with
        {
            ShardMap = Restamp(restored, logical.ShardMap),
            NextShardIndex = restoredNextShardIndex,
        });
    }

    /// <summary>
    /// Clears the map a cutover recorded on <paramref name="shadowPhysicalTreeId"/>
    /// once a revert has moved the alias off it, so a later commit of the same
    /// shadow records the logical tree's map as it then stands rather than reusing
    /// one the tree has since moved on from.
    /// </summary>
    public static async Task CompleteRevertAsync(IGrainFactory grainFactory, string shadowPhysicalTreeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(shadowPhysicalTreeId);

        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();
        var registry = grainFactory.GetLatticeRegistry();
        if (await registry.GetEntryAsync(shadowPhysicalTreeId) is { ReplacedShardMap: not null } shadow)
        {
            await registry.UpdateAsync(shadowPhysicalTreeId, shadow with
            {
                ReplacedShardMap = null,
                ReplacedNextShardIndex = null,
            });
        }
    }

    /// <summary>
    /// Runs the explicit alias swap <paramref name="swapAsync"/> of
    /// <paramref name="logicalTreeId"/> onto <paramref name="targetPhysicalTreeId"/>
    /// and carries the routing maps across it (issue #4263). After the swap the
    /// logical entry takes the target's own map and split allocation mark, and the
    /// map the logical tree addressed its previous physical tree by is written to
    /// that tree's own entry when it has one and is not the logical id itself, so
    /// an alias back onto it finds its layout.
    /// <para>
    /// The maps are read before the swap and written only after it succeeds, so
    /// an alias the registry refuses (a multi-level target, a deleted tree, an
    /// ownership denial) changes nothing. A re-set of the current alias carries
    /// nothing: the logical map already describes the target. Every write is to
    /// the registry from outside its turn (issue #4128).
    /// </para>
    /// </summary>
    public static async Task CarryAcrossExplicitAliasAsync(
        IGrainFactory grainFactory,
        string logicalTreeId,
        string targetPhysicalTreeId,
        Func<Task> swapAsync)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(logicalTreeId);
        ArgumentNullException.ThrowIfNull(targetPhysicalTreeId);
        ArgumentNullException.ThrowIfNull(swapAsync);

        var registry = grainFactory.GetLatticeRegistry();
        var logicalBefore = await registry.GetEntryAsync(logicalTreeId);
        var current = logicalBefore?.PhysicalTreeId ?? logicalTreeId;
        if (string.Equals(current, targetPhysicalTreeId, StringComparison.Ordinal))
        {
            await swapAsync();
            return;
        }

        var replaced = EffectiveMap(logicalBefore);
        await swapAsync();

        // A previous physical tree with its own id was addressed by the logical
        // map, never its own, so its own entry is stale once the alias moves off.
        if (!string.Equals(current, logicalTreeId, StringComparison.Ordinal)
            && await registry.GetEntryAsync(current) is { } previous
            && (previous.ShardMap is null || !SameSlots(replaced, previous.ShardMap)))
        {
            await registry.UpdateAsync(current, previous with
            {
                ShardMap = Restamp(replaced, previous.ShardMap),
                NextShardIndex = logicalBefore?.NextShardIndex ?? previous.NextShardIndex,
            });
        }

        // Routing reads the map under the logical id, so the target's shards are
        // addressed by whatever map the logical entry holds: carry the target's.
        var target = await registry.GetEntryAsync(targetPhysicalTreeId);
        var targetMap = EffectiveMap(target);
        var logical = await registry.GetEntryAsync(logicalTreeId) ?? new TreeRegistryEntry();
        if (!SameSlots(EffectiveMap(logical), targetMap) || logical.NextShardIndex != target?.NextShardIndex)
        {
            await registry.UpdateAsync(logicalTreeId, logical with
            {
                ShardMap = Restamp(targetMap, logical.ShardMap),
                NextShardIndex = target?.NextShardIndex,
            });
        }
    }

    /// <summary>
    /// The map routing addresses a tree's shards by: its persisted map, or the
    /// default map for its shard-count pin, as the tree router resolves it.
    /// </summary>
    private static ShardMap EffectiveMap(TreeRegistryEntry? entry) =>
        entry?.ShardMap ?? ShardMap.GetOrCreateDefaultShared(
            LatticeConstants.DefaultVirtualShardCount,
            entry?.ShardCount ?? LatticeConstants.DefaultShardCount);

    /// <summary>
    /// A copy of <paramref name="map"/> versioned above both it and
    /// <paramref name="existing"/>, so every cached router sees the change.
    /// </summary>
    private static ShardMap Restamp(ShardMap map, ShardMap? existing) =>
        Copy(map, Math.Max(existing?.Version ?? 0L, map.Version) + 1);

    private static ShardMap Copy(ShardMap map, long version) =>
        new() { Slots = (int[])map.Slots.Clone(), Version = version };

    private static bool SameSlots(ShardMap left, ShardMap right) =>
        left.Slots.AsSpan().SequenceEqual(right.Slots);
}
