namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Strict ingest for plain last-writer-wins replicated writes (issue #5684). A
/// peer's write can reach a receiver as a plain replicated row: a steady-state
/// last-writer-wins entry, a repair re-ship, or an atomic batch a re-seed copy
/// re-appended as plain merges. Each such row is judged at the write choke point
/// as an atomic entry is, so a value a strict tree's policy refuses is diverted
/// and never stored.
/// </summary>
/// <remarks>
/// <para>
/// <b>Cost.</b> <see cref="AppliesAsync"/> is asked once per replicated batch.
/// With the default interceptor, with strict ingest off, or for a tree that is not
/// strict, it answers <c>false</c> from cached state and the rows are never
/// visited: no per-row work and no allocation. For a strict tree each row costs
/// one interceptor call; the filtered list is allocated only when a row is
/// diverted, and then once per batch.
/// </para>
/// <para>
/// <b>Verbatim storage.</b> An accepted row keeps its replicated bytes: a
/// transformed value is not applied, so a strict receiver stores what its peer
/// stores and regions converge byte for byte. Tombstones and already-expired
/// values carry nothing to judge.
/// </para>
/// </remarks>
internal static class ReplicatedWriteJudgement
{
    /// <summary>The operation a plain replicated row is judged as.</summary>
    internal const LatticeOperation Operation = LatticeOperation.Write | LatticeOperation.Replication;

    /// <summary>
    /// Whether the rows of a plain replicated batch to the logical tree
    /// <paramref name="treeId"/> must be judged.
    /// </summary>
    public static ValueTask<bool> AppliesAsync(
        ILatticeWriteInterceptor interceptor, string treeId, CancellationToken cancellationToken) =>
        LatticeWriteInterceptorEnforcement.Skips(interceptor)
            ? new ValueTask<bool>(false)
            : interceptor.JudgesReplicatedWritesAsync(treeId, cancellationToken);

    /// <summary>
    /// Judges one plain replicated value. Returns <c>false</c> when the
    /// interceptor diverted it, <c>true</c> when it is to be stored verbatim, and
    /// throws <see cref="LatticeWriteRejectedException"/> on a rejection.
    /// </summary>
    public static async ValueTask<bool> AdmitsAsync(
        ILatticeWriteInterceptor interceptor,
        string treeId,
        string key,
        byte[] value,
        TimeSpan? ttl,
        CancellationToken cancellationToken)
    {
        var request = new LatticeWriteRequest(treeId, key, value, Operation, ttl);
        var decision = await interceptor.OnWriteAsync(in request, cancellationToken);
        return decision.Kind switch
        {
            LatticeWriteDecisionKind.Accept or LatticeWriteDecisionKind.AcceptTransformed => true,
            LatticeWriteDecisionKind.DeadLetter => false,
            _ => throw new LatticeWriteRejectedException(
                treeId, Operation, key, decision.Reason ?? "Rejected by the write interceptor."),
        };
    }

    /// <summary>
    /// The time left before an absolute expiry, or <c>null</c> when the value
    /// does not expire. A non-positive result means the value has already expired.
    /// </summary>
    public static TimeSpan? RemainingTtl(long expiresAtTicks, long nowTicks) =>
        expiresAtTicks > 0 ? TimeSpan.FromTicks(expiresAtTicks - nowTicks) : null;

    /// <summary>
    /// Judges every value row of <paramref name="items"/> and returns the rows to
    /// apply: the same list when nothing was diverted, otherwise a new list
    /// without the diverted rows.
    /// </summary>
    public static async ValueTask<IReadOnlyList<ApplyMergeItem>> FilterAsync(
        ILatticeWriteInterceptor interceptor,
        string treeId,
        IReadOnlyList<ApplyMergeItem> items,
        long nowTicks,
        CancellationToken cancellationToken)
    {
        List<ApplyMergeItem>? kept = null;
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            var ttl = RemainingTtl(item.ExpiresAtTicks, nowTicks);
            var judged = !item.IsTombstone && item.Value is not null && (ttl is null || ttl > TimeSpan.Zero);
            if (!judged || await AdmitsAsync(interceptor, treeId, item.Key, item.Value!, ttl, cancellationToken))
            {
                kept?.Add(item);
                continue;
            }

            if (kept is null)
            {
                kept = new List<ApplyMergeItem>(items.Count - 1);
                for (var j = 0; j < i; j++)
                {
                    kept.Add(items[j]);
                }
            }
        }

        return kept is null ? items : kept;
    }
}
