namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Verified core for the WAL fall-off boundary: whether the write-ahead log has
/// been trimmed past an offset a leaf replaying from <c>checkpoint</c> still
/// needs. Both sites that make the decision route through it - the fall-off-log
/// detector's trim trigger (<c>LatticeFallOffLogDetector.ClassifyAsync</c>) and
/// the activation's cold-replay guard (<c>BPlusLeafGrain.ReplayWalSinceCheckpointCoreAsync</c>)
/// - so they cannot disagree, which each site's comment used to demand in prose.
/// <para>
/// The boundary is <c>tail &gt; checkpoint + 1</c>, not <c>tail &gt; checkpoint</c>:
/// the checkpoint is the last offset already read, so the first offset still
/// needed is <c>checkpoint + 1</c>, and the coverage-gated GC routinely trims the
/// checkpoint entry itself. A checkpoint of <c>-1</c>, the "nothing read"
/// sentinel, is never reported as lost. A checkpoint of <c>0</c> is a real read
/// position (issue #4433): every caller reads it through
/// <c>BPlusLeafGrain.GetPersistedCheckpointForPartition</c>, which reports an
/// unassigned scalar <c>0</c> as <c>-1</c> (issue #2703), so a <c>0</c> that
/// reaches here was durably recorded and offset <c>1</c> is still needed.
/// </para>
/// <para>
/// What this predicate proves is bounded by the checkpoint its caller passes. It
/// is a correct loss test only when the caller's projection already holds every
/// owned entry at or below that checkpoint; a cold rebuild's empty cache needs
/// offset 0 whatever the checkpoint says (issue #4450).
/// </para>
/// </summary>
internal static class WalFallOffCore
{
    /// <summary>
    /// Whether the WAL, whose oldest readable offset is <paramref name="tail"/>,
    /// has lost an offset a replay resuming after <paramref name="checkpoint"/>
    /// needs.
    /// </summary>
    /// <param name="checkpoint">The last offset the replay has already read.</param>
    /// <param name="tail">The oldest offset the WAL can still return.</param>
    /// <returns><see langword="true"/> when the first needed offset has been trimmed.</returns>
    public static bool IsPrefixLost(long checkpoint, long tail) =>
        checkpoint >= 0 && tail > checkpoint + 1;
}
