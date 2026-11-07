namespace Orleans.Lattice.Storage.AzureTable;

/// <summary>
/// The batches on one shard whose phase-2 commit failed on this provider instance,
/// held until the next reconciliation reclaims any it can prove dead.
/// </summary>
/// <remarks>
/// A batch reaches phase 2 only after its entry rows landed, so a failed commit
/// leaves those rows behind with no manifest row. Reconciliation finds such a batch
/// only above <c>TAIL</c>; one another writer has committed past - the batch a
/// stale writer's refused commit leaves (issue #5810) - sits below it, unreadable
/// and never rolled forward, and would otherwise stay in storage for good. The set
/// is filled only on the failure path, so the steady path pays nothing for it.
/// </remarks>
internal sealed class WalShardFailedBatches
{
    private readonly object _gate = new();
    private List<(long Start, long EndInclusive)>? _batches;

    /// <summary>Records a batch whose commit failed.</summary>
    internal void Add(long start, long endInclusive)
    {
        lock (_gate)
        {
            (_batches ??= new List<(long, long)>()).Add((start, endInclusive));
        }
    }

    /// <summary>Removes and returns every recorded batch, or <see langword="null"/> when there is none.</summary>
    internal List<(long Start, long EndInclusive)>? TakeAll()
    {
        lock (_gate)
        {
            var taken = _batches;
            _batches = null;
            return taken;
        }
    }
}
