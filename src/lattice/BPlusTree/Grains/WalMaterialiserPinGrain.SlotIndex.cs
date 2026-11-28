namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The per-slot consumer index (issue #6166). A slot write rebuilds each written slot's
/// contents from the in-memory map; walking the whole map to find them made every write
/// cost <c>O(consumers on the shard)</c>, so a birth seed, a coalesced flush and every write
/// queued behind them grew with the tree. The index lists each slot's consumers, so a write
/// costs <c>O(consumers in the slots it writes)</c>.
/// </summary>
/// <remarks>
/// <para>
/// The index holds references to the map's own consumer-id strings: one list per slot and
/// one reference per consumer. It is built lazily, for the current layout width only, on
/// the first slot write that needs it, and dropped whenever the width changes or the map is
/// loaded or cleared, so it is never consulted for a layout it was not built for.
/// </para>
/// <para>
/// A consumer is added once, when it first enters the map (<see cref="IndexNewConsumer"/>),
/// and removed when it leaves it. Every reader looks each listed consumer up in the map, so
/// an entry for a consumer that has since left contributes nothing, and the slot contents
/// built from the index equal those built by walking the map.
/// </para>
/// </remarks>
internal sealed partial class WalMaterialiserPinGrain
{
    /// <summary>Each slot's consumers under <see cref="_slotIndexWidth"/>; <see langword="null"/> until built.</summary>
    private List<string>[]? _slotIndex;

    /// <summary>The layout width <see cref="_slotIndex"/> was built for.</summary>
    private int _slotIndexWidth;

    /// <summary>Consumers a slice build visited; test seam for the per-write cost (#6166).</summary>
    internal long SliceBuildVisitsForTest { get; private set; }

    /// <summary>Whether <paramref name="consumerId"/> has any entry in the map.</summary>
    private bool IsMapped(string consumerId) =>
        _state.State.Pins.ContainsKey(consumerId)
        || _state.State.Offsets.ContainsKey(consumerId)
        || _state.State.OverrideHolds.Contains(consumerId);

    /// <summary>
    /// Adds a consumer that has just entered the map to the index, when one is built. Call it
    /// only for a consumer that had no entry in the map before, so it is listed once.
    /// </summary>
    private void IndexNewConsumer(string consumerId)
    {
        if (_slotIndex is { } index)
        {
            var slot = SlotOf(consumerId, _slotIndexWidth);
            (index[slot] ??= new List<string>()).Add(consumerId);
        }
    }

    /// <summary>Removes a consumer that has left the map from the index, when one is built.</summary>
    private void UnindexConsumer(string consumerId)
    {
        if (_slotIndex is { } index && index[SlotOf(consumerId, _slotIndexWidth)] is { } list)
        {
            list.Remove(consumerId);
        }
    }

    /// <summary>Drops the index; the next slot write that needs one rebuilds it.</summary>
    private void InvalidateSlotIndex()
    {
        _slotIndex = null;
        _slotIndexWidth = 0;
    }

    /// <summary>Returns the index for <paramref name="width"/>, building it from the map if needed.</summary>
    private List<string>[] SlotIndexFor(int width)
    {
        if (_slotIndex is { } built && _slotIndexWidth == width)
        {
            return built;
        }

        var index = new List<string>[width];
        foreach (var consumerId in _state.State.Pins.Keys)
        {
            Add(consumerId);
        }

        foreach (var consumerId in _state.State.Offsets.Keys)
        {
            if (!_state.State.Pins.ContainsKey(consumerId))
            {
                Add(consumerId);
            }
        }

        foreach (var consumerId in _state.State.OverrideHolds)
        {
            if (!_state.State.Pins.ContainsKey(consumerId) && !_state.State.Offsets.ContainsKey(consumerId))
            {
                Add(consumerId);
            }
        }

        _slotIndex = index;
        _slotIndexWidth = width;
        return index;

        void Add(string consumerId) => (index[SlotOf(consumerId, width)] ??= new List<string>()).Add(consumerId);
    }

    /// <summary>
    /// Fills <paramref name="slices"/> with the map entries of the slots they key, under the
    /// current layout, reading only those slots' consumers from the index.
    /// </summary>
    private void FillSlicesFromIndex(Dictionary<int, WalMaterialiserPinState> slices)
    {
        var index = SlotIndexFor(_bucketCount);
        var pins = _state.State.Pins;
        var offsets = _state.State.Offsets;
        var holds = _state.State.OverrideHolds;
        foreach (var (slot, slice) in slices)
        {
            if (index[slot] is not { } consumers)
            {
                continue;
            }

            SliceBuildVisitsForTest += consumers.Count;
            foreach (var consumerId in consumers)
            {
                if (pins.TryGetValue(consumerId, out var pin))
                {
                    slice.Pins[consumerId] = pin;
                }

                if (offsets.TryGetValue(consumerId, out var offset))
                {
                    slice.Offsets[consumerId] = offset;
                }

                if (holds.Contains(consumerId))
                {
                    slice.OverrideHolds.Add(consumerId);
                }
            }
        }
    }
}
