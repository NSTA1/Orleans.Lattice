using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Test access to one initiator's slot in a WAL partition's routing-generation table
/// (#5987): one row per partition, a slot per initiator.
/// </summary>
internal static class RoutingGenerationSlots
{
    /// <summary>One initiator's slot: whether the partition's table names it, and its generation.</summary>
    internal readonly record struct Slot(bool RecordExists, long Generation);

    /// <summary>Reads <paramref name="initiator"/>'s slot in <paramref name="partition"/>'s table.</summary>
    internal static async Task<Slot> ReadAsync(IGrainStorage storage, string treeId, int partition, WalRoutingInitiator initiator)
    {
        var row = await WalAppendFenceRows.ReadGenerationsAsync(storage, treeId, partition);
        var present = row.RecordExists && row.State.Generations.ContainsKey(WalAppendFenceRows.InitiatorTag(initiator));
        return new Slot(present, WalAppendFenceRows.SlotOf(row.State, initiator));
    }

    /// <summary>
    /// Sets <paramref name="initiator"/>'s slot in <paramref name="partition"/>'s table to
    /// <paramref name="generation"/>, leaving every other slot as it is.
    /// </summary>
    internal static async Task WriteAsync(IGrainStorage storage, string treeId, int partition, WalRoutingInitiator initiator, long generation)
    {
        var row = await WalAppendFenceRows.ReadGenerationsAsync(storage, treeId, partition);
        row.State.Generations[WalAppendFenceRows.InitiatorTag(initiator)] = generation;
        await WalAppendFenceRows.WriteAsync(
            storage,
            WalAppendFenceRows.GenerationGrainType,
            WalAppendFenceRows.GenerationsKey(treeId, partition),
            row);
    }

    /// <summary>
    /// Every slot of <paramref name="partition"/>'s table by initiator, and whether the row
    /// exists at all. A slot whose tag this build cannot parse fails the read.
    /// </summary>
    internal static async Task<(bool RecordExists, IReadOnlyDictionary<WalRoutingInitiator, long> Slots)> ReadAllAsync(
        IGrainStorage storage, string treeId, int partition)
    {
        var row = await WalAppendFenceRows.ReadGenerationsAsync(storage, treeId, partition);
        var slots = new Dictionary<WalRoutingInitiator, long>();
        foreach (var (tag, generation) in row.State.Generations)
        {
            if (!WalAppendFenceRows.TryParseInitiatorTag(tag, out var initiator))
            {
                throw new InvalidOperationException($"partition {partition}'s routing-generation table holds an unknown slot '{tag}'.");
            }

            slots[initiator] = generation;
        }

        return (row.RecordExists, slots);
    }
}
