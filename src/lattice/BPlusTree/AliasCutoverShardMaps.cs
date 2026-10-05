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
/// The alias and the map it is paired with are written together, in one write
/// of the logical row (<see cref="ILatticeRegistry.SwapAliasAsync"/>). Writing
/// them in two registry calls - the map first and the alias second, or the
/// reverse - let every reader that resolved routing between them address one
/// physical copy by another copy's map, and a routing activation that cached
/// such a pair kept it (issue #4336).
/// </para>
/// <para>
/// Every write is to the registry from outside its turn, so nothing here can
/// re-enter a registry call (issue #4128). The caller holds the alias-change
/// reservation where one exists; each lifecycle helper runs under a
/// system-origin scope of its own.
/// </para>
/// </summary>
internal static class AliasCutoverShardMaps
{
    /// <summary>
    /// Prepares a cutover of <paramref name="logicalTreeId"/> onto
    /// <paramref name="destinationPhysicalTreeId"/>; call it before
    /// <see cref="SwapCutoverAsync"/>. Records the map the logical tree addresses
    /// its current physical tree by on the destination entry (once, so a resumed
    /// cutover keeps the original), and stamps that map onto the current physical
    /// tree's own entry when it is not the logical id itself. Leaves the logical
    /// entry untouched: its map moves only with the alias.
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
        cancellationToken.ThrowIfCancellationRequested();

        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();
        var registry = grainFactory.GetLatticeRegistry();
        var logicalBefore = await registry.GetEntryAsync(logicalTreeId);
        var current = logicalBefore?.PhysicalTreeId ?? logicalTreeId;
        var recorded = await registry.GetEntryAsync(destinationPhysicalTreeId);

        // A cutover resumed after its swap has nothing left to record: the logical
        // entry now describes the destination, and splits since may have moved it.
        if (string.Equals(current, destinationPhysicalTreeId, StringComparison.Ordinal))
        {
            return recorded?.ReplacedShardMap;
        }

        // A destination with no row yet is normal: a restore's or remediation's
        // copy can be addressed before its row is materialised, and this record is
        // what first creates it.
        var destination = recorded ?? new TreeRegistryEntry();
        var replaced = destination.ReplacedShardMap;
        if (replaced is null)
        {
            replaced = ReplacedMapOf(logicalBefore);
            destination = destination with
            {
                ReplacedShardMap = replaced,
                ReplacedNextShardIndex = logicalBefore?.NextShardIndex,
            };
            await registry.UpdateAsync(destinationPhysicalTreeId, destination);
        }

        await StampReplacedCopyAsync(
            registry, logicalTreeId, current, replaced, destination.ReplacedNextShardIndex);
        return replaced;
    }

    /// <summary>
    /// Swaps <paramref name="logicalTreeId"/> onto
    /// <paramref name="destinationPhysicalTreeId"/> together with the
    /// destination's own map and split allocation mark, in one registry write;
    /// call it after <see cref="PrepareCutoverAsync"/>. A cutover resumed after the
    /// swap changes nothing.
    /// <para>
    /// A split or fold may still have committed onto the replaced tree's map after
    /// the prepare recorded it; once the alias moves, no further one can (#4264).
    /// The map the swap actually replaced is therefore that tree's final layout,
    /// and it supersedes the recorded one on the destination entry (for a revert)
    /// and on the replaced tree's own entry.
    /// </para>
    /// </summary>
    /// <returns>
    /// The final map of the replaced physical tree, for arming or enumerating its
    /// shards; on a resumed cutover, the map recorded at the original swap
    /// (<see langword="null"/> when none was).
    /// </returns>
    public static async Task<ShardMap?> SwapCutoverAsync(
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
        if (string.Equals(current, destinationPhysicalTreeId, StringComparison.Ordinal))
        {
            return (await registry.GetEntryAsync(destinationPhysicalTreeId))?.ReplacedShardMap;
        }

        // Forced: the destination's stateless routing workers may hold a map from
        // before its own reshard (#4206).
        var destinationRouting = await grainFactory.GetGrain<ILattice>(destinationPhysicalTreeId)
            .GetRoutingAsync(forceRefresh: true, cancellationToken);
        var destination = await registry.GetEntryAsync(destinationPhysicalTreeId);
        var before = await registry.SwapAliasAsync(
            logicalTreeId,
            destinationPhysicalTreeId,
            destinationRouting.Map,
            destination?.NextShardIndex,
            expectedPhysicalTreeId: current);

        // A row an older build left mid-cutover already carries the destination's
        // map; its recorded map is the only description of the replaced tree.
        if (before?.AliasCutoverTarget is not null)
        {
            return destination?.ReplacedShardMap;
        }

        var replaced = ReplacedMapOf(before);
        if (destination?.ReplacedShardMap is { } recorded && !SameSlots(recorded, replaced)
            && await registry.GetEntryAsync(destinationPhysicalTreeId) is { } latest)
        {
            await registry.UpdateAsync(destinationPhysicalTreeId, latest with
            {
                ReplacedShardMap = replaced,
                ReplacedNextShardIndex = before?.NextShardIndex,
            });
            await StampReplacedCopyAsync(registry, logicalTreeId, current, replaced, before?.NextShardIndex);
        }

        return replaced;
    }

    /// <summary>
    /// Moves <paramref name="logicalTreeId"/> off
    /// <paramref name="shadowPhysicalTreeId"/> back onto
    /// <paramref name="previousPhysicalTreeId"/>, carrying the map recorded at the
    /// cutover back with the alias in one registry write, after stamping the
    /// logical map, which describes the shadow, onto the shadow's own entry. A
    /// previous tree equal to the logical id moves the tree back onto its own
    /// shards. When no map was recorded (a cutover from before the capture
    /// existed) or the alias no longer resolves to the shadow (a resumed revert),
    /// only the alias is moved, as before.
    /// </summary>
    public static async Task RevertAsync(
        IGrainFactory grainFactory,
        string logicalTreeId,
        string? shadowPhysicalTreeId,
        string previousPhysicalTreeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(logicalTreeId);
        ArgumentNullException.ThrowIfNull(previousPhysicalTreeId);
        cancellationToken.ThrowIfCancellationRequested();

        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();
        var registry = grainFactory.GetLatticeRegistry();
        var moveBackToSelf = string.Equals(previousPhysicalTreeId, logicalTreeId, StringComparison.Ordinal);

        if (!string.IsNullOrEmpty(shadowPhysicalTreeId)
            && !string.Equals(shadowPhysicalTreeId, previousPhysicalTreeId, StringComparison.Ordinal)
            && string.Equals(await registry.ResolveAsync(logicalTreeId), shadowPhysicalTreeId, StringComparison.Ordinal)
            && await RestoredMapAsync(registry, shadowPhysicalTreeId, previousPhysicalTreeId, moveBackToSelf)
                is { } restored)
        {
            var logical = await RequireEntryAsync(registry, logicalTreeId, RevertOperation);
            var live = EffectiveMap(logical);
            if (restored.Shadow is { } shadow && !SameSlots(live, restored.Map))
            {
                await registry.UpdateAsync(shadowPhysicalTreeId, shadow with
                {
                    ShardMap = Restamp(live, shadow.ShardMap),
                    NextShardIndex = logical.NextShardIndex,
                });
            }

            await registry.SwapAliasAsync(
                logicalTreeId,
                previousPhysicalTreeId,
                restored.Map,
                restored.NextShardIndex,
                expectedPhysicalTreeId: shadowPhysicalTreeId);
            return;
        }

        if (moveBackToSelf)
        {
            await registry.RemoveAliasAsync(logicalTreeId);
        }
        else
        {
            await registry.SetAliasAsync(logicalTreeId, previousPhysicalTreeId);
        }
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
    /// Sets the explicit alias of <paramref name="logicalTreeId"/> onto
    /// <paramref name="targetPhysicalTreeId"/> (issue #4263), moving the target's
    /// own map and split allocation mark onto the logical entry in the same
    /// registry write as the alias. The registry validates and authorizes the
    /// alias before anything is written, so an alias it refuses (a multi-level
    /// target, a deleted tree, an ownership denial, an access-gate refusal)
    /// changes nothing.
    /// <para>
    /// After the swap, the map the logical tree addressed its previous physical
    /// tree by is written to that tree's own entry when it has one and is not the
    /// logical id itself, so an alias back onto it finds its layout. Every shard
    /// of the previous copy is then armed to redirect traffic routed through the
    /// logical id onto the target, exactly as a shadow-cutover restore arms the
    /// tree it retains: a stateless routing activation that cached the previous
    /// copy otherwise keeps serving it, since nothing else tells it the alias
    /// moved. Any redirect the target carries for this logical id - left by an
    /// earlier alias off it - is released, so routing onto it does not bounce.
    /// </para>
    /// <para>
    /// A re-set of the current alias carries nothing, because the logical map
    /// already describes the target, but still releases the target's redirect, so
    /// retrying an alias that was interrupted after its swap repairs it.
    /// </para>
    /// </summary>
    public static async Task CarryAcrossExplicitAliasAsync(
        IGrainFactory grainFactory,
        string logicalTreeId,
        string targetPhysicalTreeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(logicalTreeId);
        ArgumentNullException.ThrowIfNull(targetPhysicalTreeId);
        cancellationToken.ThrowIfCancellationRequested();

        var registry = grainFactory.GetLatticeRegistry();
        var logicalBefore = await registry.GetEntryAsync(logicalTreeId);
        var current = logicalBefore?.PhysicalTreeId ?? logicalTreeId;
        if (string.Equals(current, targetPhysicalTreeId, StringComparison.Ordinal))
        {
            await registry.SetAliasAsync(logicalTreeId, targetPhysicalTreeId);
            await ReleaseRedirectsAsync(
                grainFactory, targetPhysicalTreeId, EffectiveMap(logicalBefore), logicalTreeId, cancellationToken);
            return;
        }

        // Routing reads the map under the logical id, so the target's shards are
        // addressed by whatever map the logical entry holds: carry the target's.
        var target = await registry.GetEntryAsync(targetPhysicalTreeId);
        var targetMap = EffectiveMap(target);

        // Re-stamp the lineage before the swap (#4537): a crash in between leaves
        // only a spurious re-stamp over the old contents, which is conservative,
        // never the new contents under the old lineage.
        if (await registry.GetEntryAsync(logicalTreeId) is { } moving)
        {
            await registry.UpdateAsync(logicalTreeId, moving with { Lineage = Guid.NewGuid() });
        }

        var before = await registry.SwapAliasAsync(
            logicalTreeId,
            targetPhysicalTreeId,
            targetMap,
            target?.NextShardIndex,
            expectedPhysicalTreeId: current);

        // The swap's own read of the row is authoritative: a split that committed
        // onto the previous copy's map after this method read it is part of that
        // copy's layout.
        var replacedEntry = before ?? logicalBefore;
        var replaced = EffectiveMap(replacedEntry);
        await StampReplacedCopyAsync(registry, logicalTreeId, current, replaced, replacedEntry?.NextShardIndex);

        await ReleaseRedirectsAsync(grainFactory, targetPhysicalTreeId, targetMap, logicalTreeId, cancellationToken);
        await ArmRedirectsAsync(
            grainFactory,
            current,
            replaced,
            targetPhysicalTreeId,
            logicalTreeId,
            $"alias:{logicalTreeId}->{targetPhysicalTreeId}",
            cancellationToken);
    }

    /// <summary>
    /// The map the replaced tree's shards were addressed by, read off the logical
    /// row as it stood before a swap: a copy of its persisted map, or the default
    /// map for its shard-count pin.
    /// </summary>
    private static ShardMap ReplacedMapOf(TreeRegistryEntry? logical)
    {
        var map = EffectiveMap(logical);
        return Copy(map, map.Version);
    }

    /// <summary>
    /// Writes <paramref name="replaced"/> onto the replaced physical tree's own
    /// entry. A replaced tree with its own id is addressed by the logical map,
    /// never its own, so its own entry is stale; once the alias moves off it, that
    /// entry is all that describes it to a direct-physical walk or an alias back.
    /// Nothing is written for the logical id's own shards (the logical row now
    /// describes the new copy) or when the entry already matches.
    /// </summary>
    private static async Task StampReplacedCopyAsync(
        ILatticeRegistry registry,
        string logicalTreeId,
        string replacedPhysicalTreeId,
        ShardMap replaced,
        int? replacedNextShardIndex)
    {
        if (string.Equals(replacedPhysicalTreeId, logicalTreeId, StringComparison.Ordinal)
            || await registry.GetEntryAsync(replacedPhysicalTreeId) is not { } previous
            || (previous.ShardMap is { } own && SameSlots(replaced, own)
                && previous.NextShardIndex == (replacedNextShardIndex ?? previous.NextShardIndex)))
        {
            return;
        }

        await registry.UpdateAsync(replacedPhysicalTreeId, previous with
        {
            ShardMap = Restamp(replaced, previous.ShardMap),
            NextShardIndex = replacedNextShardIndex ?? previous.NextShardIndex,
        });
    }

    /// <summary>
    /// The map a revert carries back onto the logical entry: the one recorded on
    /// the shadow at the cutover, or - for a cutover from before the capture
    /// existed - the previous tree's own map when it has its own id.
    /// </summary>
    private static async Task<RestoredMap?> RestoredMapAsync(
        ILatticeRegistry registry, string shadowPhysicalTreeId, string previousPhysicalTreeId, bool moveBackToSelf)
    {
        var shadow = await registry.GetEntryAsync(shadowPhysicalTreeId);
        if (shadow?.ReplacedShardMap is { } recorded)
        {
            return new RestoredMap(shadow, recorded, shadow.ReplacedNextShardIndex);
        }

        // The logical id's own shards have no entry of their own to fall back on.
        return !moveBackToSelf && await registry.GetEntryAsync(previousPhysicalTreeId) is { ShardMap: { } own } previous
            ? new RestoredMap(shadow, own, previous.NextShardIndex)
            : null;
    }

    /// <summary>
    /// Arms every shard of <paramref name="retainedPhysicalTreeId"/> described by
    /// <paramref name="retainedMap"/> to redirect traffic routed through
    /// <paramref name="logicalTreeId"/> onto <paramref name="destinationPhysicalTreeId"/>.
    /// Idempotent per <paramref name="operationId"/>.
    /// </summary>
    internal static async Task ArmRedirectsAsync(
        IGrainFactory grainFactory,
        string retainedPhysicalTreeId,
        ShardMap retainedMap,
        string destinationPhysicalTreeId,
        string logicalTreeId,
        string operationId,
        CancellationToken cancellationToken)
    {
        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();
        var indices = retainedMap.GetPhysicalShardIndices();
        var tasks = new Task[indices.Count];
        for (var i = 0; i < indices.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            tasks[i] = grainFactory.GetGrain<IShardRootGrain>($"{retainedPhysicalTreeId}/{indices[i]}")
                .MarkRetainedRedirectAsync(destinationPhysicalTreeId, operationId, logicalTreeId);
        }

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Releases, on every shard of <paramref name="physicalTreeId"/> described by
    /// <paramref name="map"/>, any redirect of traffic routed through
    /// <paramref name="logicalTreeId"/>: the logical id now resolves to that tree.
    /// </summary>
    private static async Task ReleaseRedirectsAsync(
        IGrainFactory grainFactory,
        string physicalTreeId,
        ShardMap map,
        string logicalTreeId,
        CancellationToken cancellationToken)
    {
        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();
        var indices = map.GetPhysicalShardIndices();
        var tasks = new Task[indices.Count];
        for (var i = 0; i < indices.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            tasks[i] = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{indices[i]}")
                .ReleaseRetainedRedirectAsync(logicalTreeId);
        }

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// The map routing addresses a tree's shards by: its persisted map, or the
    /// default map for its shard-count pin, as the tree router resolves it.
    /// </summary>
    internal static ShardMap EffectiveMap(TreeRegistryEntry? entry) =>
        entry?.ShardMap ?? ShardMap.GetOrCreateDefaultShared(
            LatticeConstants.DefaultVirtualShardCount,
            entry?.ShardCount ?? LatticeConstants.DefaultShardCount);

    /// <summary>
    /// A copy of <paramref name="map"/> versioned above both it and
    /// <paramref name="existing"/>, so every cached router sees the change.
    /// </summary>
    private static ShardMap Restamp(ShardMap map, ShardMap? existing) =>
        Copy(map, Math.Max(existing?.Version ?? 0L, map.Version) + 1);

    private const string RevertOperation = "the alias-revert shard-map carry";

    /// <summary>
    /// Reads the row a carry is about to rewrite, refusing when there is none
    /// (issue #4270). <see cref="ILatticeRegistry.UpdateAsync"/> is an
    /// unconditional upsert, so defaulting a missing row to an empty entry would
    /// create one with no structural pins and hide whatever removed it.
    /// </summary>
    private static async Task<TreeRegistryEntry> RequireEntryAsync(
        ILatticeRegistry registry, string treeId, string operation) =>
        await registry.GetEntryAsync(treeId) ?? throw new LatticeTreeNotRegisteredException(treeId, operation);

    private static ShardMap Copy(ShardMap map, long version) =>
        new() { Slots = (int[])map.Slots.Clone(), Version = version };

    private static bool SameSlots(ShardMap left, ShardMap right) =>
        left.Slots.AsSpan().SequenceEqual(right.Slots);

    /// <summary>The map a revert carries back, the split mark that goes with it, and the shadow's entry.</summary>
    private sealed record RestoredMap(TreeRegistryEntry? Shadow, ShardMap Map, int? NextShardIndex);
}
