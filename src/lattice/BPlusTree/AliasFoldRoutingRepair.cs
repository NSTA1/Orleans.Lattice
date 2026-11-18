using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Repairs a logical tree whose routing still sends virtual slots to a shard
/// that a shard fold retired on its alias target (issue #6083).
/// <para>
/// Before the fix, automatic shard healing keyed its fold coordinators by the
/// physical copy an aliased tree resolves to. The fold then drained, swapped and
/// retired the donor correctly, but committed its slot diff to the copy's own
/// registry row. Routing reads the <em>logical</em> row, so the folded slots
/// kept routing to the retired donor and every call into them failed with a
/// stale-routing fault that no refresh could clear. The data itself sits on the
/// survivor, intact.
/// </para>
/// <para>
/// The repair adopts the copy's own map for exactly those slots. Evidence is
/// required twice: the logical map must reference a shard the copy's map has
/// dropped, and that shard must report itself retired. A retired shard owns no
/// data and is never re-allocated, and a fold retires its donor only after the
/// copy's map stopped routing to it, so the copy's map is the authority for its
/// slots. A shard the logical map gained legitimately (a later split, keyed by
/// the logical tree, which writes only the logical row) is absent from the
/// copy's map too, but is never retired, so it is left alone. A slot whose
/// target the copy's map names is itself retired is left alone and logged.
/// </para>
/// <para>
/// The write is the fenced <see cref="ILatticeRegistry.ReassignSlotsAsync(string, int[], int, ShardMap, string)"/>
/// on the logical row, bound to the copy the evidence was read from, so an alias
/// change in the meantime refuses it. Re-running it is harmless.
/// </para>
/// </summary>
internal static class AliasFoldRoutingRepair
{
    /// <summary>The outcome of one <see cref="RepairAsync"/> call.</summary>
    /// <param name="Settled">
    /// <see langword="true"/> when the logical map references no shard the alias
    /// target's map has dropped, so nothing could need repair until the logical
    /// map changes again.
    /// </param>
    /// <param name="SlotsRepaired">The number of virtual slots re-pointed.</param>
    internal readonly record struct Outcome(bool Settled, int SlotsRepaired);

    /// <summary>
    /// Checks <paramref name="logicalEntry"/>, the registry row of
    /// <paramref name="logicalTreeId"/>, and repairs it when routing sends
    /// slots to a retired shard of its alias target. An unaliased tree, or one
    /// mid-cutover, is settled without a call.
    /// </summary>
    public static async Task<Outcome> RepairAsync(
        IGrainFactory grainFactory,
        string logicalTreeId,
        TreeRegistryEntry? logicalEntry,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(logicalTreeId);
        ArgumentNullException.ThrowIfNull(logger);

        if (logicalEntry is not { PhysicalTreeId: { } physicalTreeId, ShardMap: { } logicalMap }
            || logicalEntry.AliasCutoverTarget is not null
            || string.Equals(physicalTreeId, logicalTreeId, StringComparison.Ordinal))
        {
            return new Outcome(Settled: true, SlotsRepaired: 0);
        }

        var registry = grainFactory.GetLatticeRegistry();
        var copyMap = (await registry.GetEntryAsync(physicalTreeId))?.ShardMap;
        if (copyMap is null || copyMap.Slots.Length != logicalMap.Slots.Length)
            return new Outcome(Settled: true, SlotsRepaired: 0);

        var copyIndices = copyMap.GetPhysicalShardIndices();
        List<int>? candidates = null;
        foreach (var index in logicalMap.GetPhysicalShardIndices())
        {
            if (!Contains(copyIndices, index)) (candidates ??= []).Add(index);
        }

        if (candidates is null) return new Outcome(Settled: true, SlotsRepaired: 0);

        var repaired = 0;
        foreach (var donor in candidates)
        {
            if (!await Shard(grainFactory, physicalTreeId, donor).IsRetiredAsync()) continue;

            var byTarget = new SortedDictionary<int, List<int>>();
            for (var slot = 0; slot < logicalMap.Slots.Length; slot++)
            {
                if (logicalMap.Slots[slot] != donor) continue;
                var target = copyMap.Slots[slot];
                if (target == donor) continue;
                if (!byTarget.TryGetValue(target, out var slots)) byTarget[target] = slots = [];
                slots.Add(slot);
            }

            foreach (var (target, slots) in byTarget)
            {
                if (await Shard(grainFactory, physicalTreeId, target).IsRetiredAsync())
                {
                    logger.LogWarning(
                        "Tree {TreeId} routes {SlotCount} virtual slot(s) to retired shard {Donor} of {PhysicalTreeId}, but that tree's own map sends them to shard {Target}, which is retired too; left unrepaired.",
                        logicalTreeId, slots.Count, donor, physicalTreeId, target);
                    continue;
                }

                var reassigned = await registry.ReassignSlotsAsync(
                    logicalTreeId, slots.ToArray(), target, logicalMap, physicalTreeId);
                if (reassigned is null)
                {
                    // The alias moved since the evidence was read; whoever reads
                    // the new row re-checks it.
                    return new Outcome(Settled: false, SlotsRepaired: repaired);
                }

                repaired += slots.Count;
                logger.LogWarning(
                    "Repaired the routing of tree {TreeId}: {SlotCount} virtual slot(s) routed to shard {Donor} of {PhysicalTreeId}, which a shard fold retired without updating this tree's map, now route to shard {Target} (issue #6083).",
                    logicalTreeId, slots.Count, donor, physicalTreeId, target);
            }
        }

        return new Outcome(Settled: false, SlotsRepaired: repaired);
    }

    private static IShardRootGrain Shard(IGrainFactory grainFactory, string physicalTreeId, int index)
        => grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{index}");

    private static bool Contains(IReadOnlyList<int> ascending, int value)
    {
        int lo = 0, hi = ascending.Count - 1;
        while (lo <= hi)
        {
            var mid = lo + ((hi - lo) >> 1);
            var current = ascending[mid];
            if (current == value) return true;
            if (current < value) lo = mid + 1;
            else hi = mid - 1;
        }

        return false;
    }
}
