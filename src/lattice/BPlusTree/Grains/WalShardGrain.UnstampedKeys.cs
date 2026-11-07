namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The WAL shard's key-filtered unstamped watermark (#5851, a5e2e2ef's K1-K6): a split
/// cut reads the unstamped high only for the donor's own key range, so a stray
/// unstamped append elsewhere in the partition no longer caps every split's hint.
/// </summary>
/// <remarks>
/// <para>
/// K1: the partition keeps its most recent <see cref="UnstampedKeyTableCapacity"/>
/// key-local unstamped records as (key, offset). A record overwritten past capacity
/// max-folds its offset into <see cref="_unstampedKeyFloor"/>, which every reader is
/// bounded by. The global sticky high (<c>_unstampedWatermark</c>) is unchanged and is
/// still what every stamped append response reports.
/// </para>
/// <para>
/// K3: only a single-key <c>Set</c>, <c>Delete</c> or <c>Tombstone</c> record is
/// key-local. A range delete, a transaction terminal, or a record naming matched keys
/// can bear on keys other than its own, so it folds straight into the floor and bounds
/// every reader.
/// </para>
/// <para>
/// K2, K4: a reader passes the range it owns, read in the same turn as the cut, so a
/// leaf whose range widened reads the widened range; the widening path's own hand-off
/// marker bounds the records of the absorbed span that are in flight.
/// </para>
/// <para>
/// K5: the table is held in memory only. At activation the floor restarts at the
/// recovered head minus one, as the global watermark does
/// (<c>ReadPositionHonestRestartForgetsUnstamped</c>).
/// </para>
/// <para>
/// K6: the table is written in the append's existing allocation step under
/// <see cref="_stateGate"/>, with no call and no allocation once its two arrays exist;
/// it is read only by <see cref="IWalShardGrain.GetSplitCutAsync"/>.
/// </para>
/// </remarks>
internal sealed partial class WalShardGrain
{
    /// <summary>How many key-local unstamped records one shard activation tables before folding the oldest into the floor.</summary>
    internal const int UnstampedKeyTableCapacity = 256;

    private string?[]? _unstampedKeys;
    private long[]? _unstampedKeyOffsets;
    private int _unstampedKeyNext;
    private int _unstampedKeyCount;

    // Every unstamped record at or below it may lie in any range: the recovered head minus
    // one, raised by every non-key-local record and every record folded out of the table.
    private long _unstampedKeyFloor = -1;

    /// <summary>Starts the table afresh at activation, below the recovered head (K5). Called before any append is admitted.</summary>
    private void ResetUnstampedKeys(long recoveredNextOffset)
    {
        if (_unstampedKeys is not null)
        {
            Array.Clear(_unstampedKeys);
        }

        _unstampedKeyNext = 0;
        _unstampedKeyCount = 0;
        _unstampedKeyFloor = recoveredNextOffset - 1;
    }

    /// <summary>
    /// Records an append at <paramref name="offset"/> that raised the global unstamped
    /// watermark: tabled by its key when it is key-local (K3), else folded into the floor.
    /// Called under <see cref="_stateGate"/>.
    /// </summary>
    private void NoteUnstampedKeyLocked(in WalRecord entry, long offset)
    {
        if (!IsKeyLocalUnstamped(in entry))
        {
            _unstampedKeyFloor = Math.Max(_unstampedKeyFloor, offset);
            return;
        }

        _unstampedKeys ??= new string?[UnstampedKeyTableCapacity];
        _unstampedKeyOffsets ??= new long[UnstampedKeyTableCapacity];
        if (_unstampedKeyCount == UnstampedKeyTableCapacity)
        {
            // Offsets are tabled in allocation order, so the slot overwritten is the oldest (K1).
            _unstampedKeyFloor = Math.Max(_unstampedKeyFloor, _unstampedKeyOffsets[_unstampedKeyNext]);
        }
        else
        {
            _unstampedKeyCount++;
        }

        _unstampedKeys[_unstampedKeyNext] = entry.Key;
        _unstampedKeyOffsets[_unstampedKeyNext] = offset;
        _unstampedKeyNext = (_unstampedKeyNext + 1) % UnstampedKeyTableCapacity;
    }

    /// <summary>
    /// The unstamped high a reader owning <c>[low, high)</c> must replay past (K2): the
    /// floor, and every tabled record whose key it owns. Called under <see cref="_stateGate"/>.
    /// </summary>
    private long UnstampedHighForRangeLocked(string? lowInclusive, string? highExclusive)
    {
        var high = _unstampedKeyFloor;
        for (var i = 0; i < _unstampedKeyCount; i++)
        {
            if (_unstampedKeyOffsets![i] > high && SplitBoundary.Owns(_unstampedKeys![i]!, lowInclusive, highExclusive))
            {
                high = _unstampedKeyOffsets[i];
            }
        }

        return high;
    }

    private static bool IsKeyLocalUnstamped(in WalRecord entry) =>
        entry.Op is MutationKind.Set or MutationKind.Delete or MutationKind.Tombstone
        && entry.EndExclusiveKey is null
        && entry.MatchedKeys is null
        && entry.Key is not null;

    /// <summary>Reads <see cref="UnstampedHighForRangeLocked"/> under the lock; test seam.</summary>
    internal long UnstampedHighForRangeForTesting(string? lowInclusive, string? highExclusive)
    {
        lock (_stateGate)
        {
            return UnstampedHighForRangeLocked(lowInclusive, highExclusive);
        }
    }
}
