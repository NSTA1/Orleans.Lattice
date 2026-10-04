namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The pure, deterministic decision of the receiver-side cross-tree barrier
/// (<see cref="Grains.LatticeCrossTreeReceiverGrain"/>): when a replicated
/// cross-tree atomic write may become visible on this cluster, and with which
/// single verdict. Extracted from
/// <c>LatticeCrossTreeReceiverGrain.NotifyTerminalAsync</c> so the rule the grain
/// runs is the rule the cross-cluster Coyote model
/// (<c>CrossTreeReceiverBarrierModel</c>) and the TLA+ module
/// (<c>spec/atomic-commit/AtomicCommitCrossCluster.tla</c>, action
/// <c>ReceiverNotify</c>) are checked against, exactly like
/// <see cref="SagaCoordinatorCore"/> and <see cref="TerminalArrivalTally"/>.
/// <para>
/// The barrier holds every participating tree back until the last one's
/// terminal has arrived, then flips them all with one verdict. Deciding early
/// - on a subset of the wait set - lets one tree finalise while a sibling's
/// prepares are still in flight, which is a partial cross-tree view the
/// authoring cluster never exposes.
/// </para>
/// <para>
/// The core owns no <c>Task</c>/<c>await</c>, no timers and no Orleans runtime
/// types, and allocates nothing: it takes the grain's persisted collections by
/// reference and reads them.
/// </para>
/// </summary>
internal static class CrossTreeReceiverBarrier
{
    /// <summary>
    /// Reports whether every tree in the frozen wait set has delivered its
    /// terminal, which is the only condition under which the barrier may decide.
    /// </summary>
    /// <typeparam name="TArrival">The per-tree arrival record type.</typeparam>
    /// <param name="waitSet">The frozen, canonical wait set.</param>
    /// <param name="arrived">The terminals recorded so far, keyed by tree id.</param>
    /// <returns>
    /// <see langword="true"/> when every wait-set tree has an arrival recorded.
    /// </returns>
    public static bool IsComplete<TArrival>(List<string> waitSet, Dictionary<string, TArrival> arrived)
    {
        ArgumentNullException.ThrowIfNull(waitSet);
        ArgumentNullException.ThrowIfNull(arrived);

        foreach (var tree in waitSet)
        {
            if (!arrived.ContainsKey(tree))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// The barrier's single global verdict: commit if and only if every arrived
    /// tree's terminal committed. Meaningful once <see cref="IsComplete"/> holds.
    /// </summary>
    /// <param name="arrived">The terminals recorded, keyed by tree id.</param>
    /// <returns><see langword="true"/> to commit every tree; otherwise abort.</returns>
    public static bool CommitsAll(Dictionary<string, CrossTreeReceiverTerminal> arrived)
    {
        ArgumentNullException.ThrowIfNull(arrived);

        foreach (var terminal in arrived.Values)
        {
            if (!terminal.Committed)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Reports whether a later terminal's wait set is the same set (ignoring
    /// order and duplicates) as the frozen one, so configuration drift between
    /// two terminals of one operation can neither shrink nor grow the barrier.
    /// The frozen set is already de-duplicated, so equality holds iff every
    /// frozen tree is in <paramref name="incoming"/> and every incoming tree is
    /// frozen. Both sets are tiny, so linear scans beat allocating a set.
    /// </summary>
    /// <param name="frozen">The canonical wait set frozen on the first terminal.</param>
    /// <param name="incoming">The wait set a later terminal carries.</param>
    /// <returns><see langword="true"/> when the two are the same set.</returns>
    public static bool WaitSetMatches(List<string> frozen, IReadOnlyList<string> incoming)
    {
        ArgumentNullException.ThrowIfNull(frozen);
        ArgumentNullException.ThrowIfNull(incoming);

        foreach (var tree in frozen)
        {
            if (!OrdinalStrings.Contains(incoming, tree)) return false;
        }

        foreach (var tree in incoming)
        {
            if (!frozen.Contains(tree)) return false;
        }

        return true;
    }
}
