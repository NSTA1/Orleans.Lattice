namespace Orleans.Lattice.Explorer.UI.Layout;

/// <summary>
/// Default <see cref="IExplorerChromeSlotCatalog"/>: groups the registered
/// contributions by placement once, so the banner's render path is an array
/// index rather than a filter.
/// </summary>
public sealed class ExplorerChromeSlotCatalog : IExplorerChromeSlotCatalog
{
    private static readonly ExplorerChromeSlot[] None = [];

    // One bucket per declared placement, indexed by the enum's value. Built once
    // at construction: the banner asks for a placement on every render, and
    // filtering a registration list per ask would allocate on each of them.
    private readonly ExplorerChromeSlot[][] _byPlacement;

    /// <summary>Groups <paramref name="slots"/> by placement.</summary>
    /// <param name="slots">The registered contributions. Must not be <see langword="null"/> or contain nulls.</param>
    /// <exception cref="ArgumentNullException"><paramref name="slots"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="slots"/> contains a <see langword="null"/>.</exception>
    public ExplorerChromeSlotCatalog(IEnumerable<ExplorerChromeSlot> slots)
    {
        ArgumentNullException.ThrowIfNull(slots);

        var placements = Enum.GetValues<ExplorerChromeSlotPlacement>();
        var maximum = 0;
        foreach (var placement in placements)
        {
            var index = (int)placement;
            if (index > maximum)
            {
                maximum = index;
            }
        }

        var buckets = new List<ExplorerChromeSlot>?[maximum + 1];
        foreach (var slot in slots)
        {
            if (slot is null)
            {
                throw new ArgumentException("A chrome slot registration was null.", nameof(slots));
            }

            var index = (int)slot.Placement;
            if (index < 0 || index >= buckets.Length)
            {
                // A placement outside the declared set can only come from a cast
                // integer. Dropping it is the fail-closed answer: the shell
                // renders no region for it, so nothing is silently misplaced.
                continue;
            }

            (buckets[index] ??= []).Add(slot);
        }

        _byPlacement = new ExplorerChromeSlot[buckets.Length][];
        for (var i = 0; i < buckets.Length; i++)
        {
            var bucket = buckets[i];
            if (bucket is null)
            {
                _byPlacement[i] = None;
                continue;
            }

            // A stable sort, so contributions with equal hints keep the order
            // their heads registered them in.
            var ordered = bucket.ToArray();
            StableSortByOrder(ordered);
            _byPlacement[i] = ordered;
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<ExplorerChromeSlot> ForPlacement(ExplorerChromeSlotPlacement placement)
    {
        var index = (int)placement;
        return index >= 0 && index < _byPlacement.Length ? _byPlacement[index] : None;
    }

    // An insertion sort rather than Array.Sort: the comparison is not stable in
    // the framework's introsort, the sets here are a handful of entries at
    // most, and this runs once per container.
    private static void StableSortByOrder(ExplorerChromeSlot[] slots)
    {
        for (var i = 1; i < slots.Length; i++)
        {
            var candidate = slots[i];
            var j = i - 1;
            while (j >= 0 && slots[j].Order > candidate.Order)
            {
                slots[j + 1] = slots[j];
                j--;
            }

            slots[j + 1] = candidate;
        }
    }
}
