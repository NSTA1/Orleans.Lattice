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
    /// <see langword="true"/> when the logical row and the alias target's own row
    /// agree, or nothing more can be done until one of them changes.
    /// </param>
    /// <param name="SlotsRepaired">The number of logical-row virtual slots re-pointed.</param>
    /// <param name="CopySynced">Whether the alias target's own row was brought into line.</param>
    internal readonly record struct Outcome(bool Settled, int SlotsRepaired, bool CopySynced = false);

    /// <summary>
    /// Checks <paramref name="logicalEntry"/>, the registry row of
    /// <paramref name="logicalTreeId"/>, against the own row of the copy it
    /// aliases, and repairs whichever is behind. An unaliased tree, or one
    /// mid-cutover, is settled without a call.
    /// <para>
    /// <b>The logical row is behind</b> when it routes slots to a shard the
    /// copy's map has dropped and that shard is retired: a fold committed onto
    /// the copy's row before #6083 was fixed. Those slots are re-pointed where the
    /// copy's map sends them.
    /// </para>
    /// <para>
    /// <b>The copy's row is behind</b> otherwise, when the two maps differ and the
    /// copy is derived from the logical tree: a split or fold committed onto the
    /// logical row between the #6083 and #6089 fixes. One empty fenced
    /// reassignment copies the logical map onto it inside the registry
    /// (issue #6089).
    /// </para>
    /// <para>
    /// Cost: one registry read of the copy's row (none when the caller already
    /// holds it, <paramref name="copyEntry"/>), a span comparison that settles the
    /// common in-sync case with no further call, and a retirement probe only for
    /// a shard the logical map names and the copy's does not.
    /// </para>
    /// </summary>
    public static async Task<Outcome> RepairAsync(
        IGrainFactory grainFactory,
        string logicalTreeId,
        TreeRegistryEntry? logicalEntry,
        ILogger logger,
        TreeRegistryEntry? copyEntry = null)
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
        var copy = copyEntry ?? await registry.GetEntryAsync(physicalTreeId);
        var copyMap = copy?.ShardMap;
        if (copyMap is null
            || copyMap.Slots.Length != logicalMap.Slots.Length
            || copyMap.Slots.AsSpan().SequenceEqual(logicalMap.Slots))
        {
            return new Outcome(Settled: true, SlotsRepaired: 0);
        }

        // Shards the logical map names and the copy's does not: retired ones are
        // pre-#6083 damage (the copy's map is newer there); live ones are splits
        // committed onto the logical row since (the logical map is newer).
        var copyIndices = copyMap.GetPhysicalShardIndices();
        List<int>? retiredDonors = null;
        foreach (var index in logicalMap.GetPhysicalShardIndices())
        {
            if (!Contains(copyIndices, index)
                && await Shard(grainFactory, physicalTreeId, index).IsRetiredAsync())
            {
                (retiredDonors ??= []).Add(index);
            }
        }

        if (retiredDonors is null)
        {
            // The logical map is the authority: bring a derived copy's row in line.
            if (!string.Equals(copy!.DerivedFrom, logicalTreeId, StringComparison.Ordinal))
                return new Outcome(Settled: true, SlotsRepaired: 0);

            if (await registry.ReassignSlotsAsync(logicalTreeId, [], 0, logicalMap, physicalTreeId) is null)
                return new Outcome(Settled: false, SlotsRepaired: 0);

            logger.LogWarning(
                "Brought the shard map of {PhysicalTreeId} back into line with tree {TreeId}, which aliases it: a split or fold committed onto {TreeId} had left the copy's own map behind (issue #6089).",
                physicalTreeId, logicalTreeId, logicalTreeId);
            return new Outcome(Settled: true, SlotsRepaired: 0, CopySynced: true);
        }

        var repaired = 0;
        foreach (var donor in retiredDonors)
        {
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
