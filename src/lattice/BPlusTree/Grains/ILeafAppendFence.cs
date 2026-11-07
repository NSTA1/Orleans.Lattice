namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A leaf activation's side of the WAL append fence (epic #5605), as the
/// commit-log writer sees it. The writer asks it for the stamp to append under at
/// each partition, and reports back every outcome, so the leaf claims its epoch
/// eagerly, re-presents a claim a restarted partition lost, records the unstamped
/// appends reported to it, and stops when it has been superseded.
/// </summary>
internal interface ILeafAppendFence
{
    /// <summary>
    /// The stamp to append under at <paramref name="partition"/>, claiming the
    /// activation's epoch there first if it has not been raised yet. Returns
    /// <see langword="null"/> when the activation's view of the fence gate is
    /// closed, so the append goes through the existing, unstamped method. Completes
    /// synchronously once the partition is claimed.
    /// </summary>
    /// <exception cref="LeafRetiredException">The activation has been superseded (<see cref="LeafRetiredException.ActivationSuperseded"/>).</exception>
    ValueTask<WalAppendStamp?> StampForAsync(IWalShardGrain shard, int partition, CancellationToken cancellationToken);

    /// <summary>A stamped append at <paramref name="partition"/> was admitted; records the reported unstamped watermark.</summary>
    void OnStampedAppended(int partition, long unstampedWatermark);

    /// <summary>
    /// A stamped append under <paramref name="stamp"/> at <paramref name="partition"/> was
    /// admitted: as <see cref="OnStampedAppended(int, long)"/>, and the watermark covers the
    /// donor closes the stamp carried (#5837).
    /// </summary>
    void OnStampedAppended(int partition, long unstampedWatermark, in WalAppendStamp stamp) =>
        OnStampedAppended(partition, unstampedWatermark);

    /// <summary>
    /// An append admitted under <paramref name="routing"/> is starting (#5837, rule D):
    /// returns the ticket <see cref="EndAppend"/> takes, so a split's drain waits for every
    /// append that started before its narrow.
    /// </summary>
    int BeginAppend(in WalRoutingStamp routing) => 0;

    /// <summary>The append <see cref="BeginAppend"/> started has completed, or failed.</summary>
    void EndAppend(int ticket)
    {
    }

    /// <summary>
    /// The activation's append of <paramref name="entry"/> landed at
    /// <paramref name="offset"/> in <paramref name="partition"/>; called before
    /// <see cref="EndAppend"/> (#5837, rule D).
    /// </summary>
    void NoteOwnAppend(int partition, in WalRecord entry, long offset)
    {
    }

    /// <summary>
    /// An append of <paramref name="entry"/> failed without reporting its offset, so it
    /// may or may not have landed (#5837, rule D).
    /// </summary>
    void NoteFailedAppend(in WalRecord entry)
    {
    }

    /// <summary>
    /// The activation appended unstamped at <paramref name="partition"/>, up to
    /// <paramref name="highestOffset"/>; it reports its own unstamped appends to
    /// itself, as if the shard had (<c>ReadPositionHonestNoSelfReport</c>).
    /// </summary>
    void OnUnstampedAppended(int partition, long highestOffset);

    /// <summary>
    /// The partition had not loaded the activation's entry: the next
    /// <see cref="StampForAsync"/> re-presents the claim, which loads it.
    /// </summary>
    void OnUnloaded(int partition, WalAppendStamp stamp);

    /// <summary>
    /// A stamped append was rejected as stale: the activation stops and
    /// deactivates. Returns the exception the writer throws.
    /// </summary>
    LeafRetiredException OnStale(WalAppendStamp stamp);

    /// <summary>
    /// The refusal for a write admitted before this activation's latest hand-off close
    /// (#5664 (a)), or <see langword="null"/> to append it. <paramref name="admissionMark"/>
    /// is <see cref="WalRoutingStamp.AdmissionMark"/>: 0 when the write's admission was
    /// not captured. Read after the stamp was taken, so a close that raised the stamp's
    /// epoch is always seen. The refusal is a self-fence: the caller re-admits the write,
    /// and span admission forwards a key the close's split moved.
    /// </summary>
    LeafRetiredException? OnAdmitted(long admissionMark);

    /// <summary>
    /// A stamped method failed with the definite "method unknown" failure of a
    /// silo running an older build: the activation closes its own view of the
    /// gate, and the writer re-issues the append unstamped.
    /// </summary>
    void OnStampedMethodUnknown();

    /// <summary>
    /// The activation could not keep a claim without being superseded - every
    /// re-presentation after an unloaded-entry rejection was rejected again - so it
    /// closes its own view of the gate for a backoff and appends unstamped meanwhile.
    /// It is not superseded, so it does not deactivate.
    /// </summary>
    void OnClaimsExhausted();

    /// <summary>
    /// A whole append call failed with a definite refusal that allocated nothing at any
    /// partition (<see cref="WalFencedAppendDispatch.IsUnallocatedRefusal"/>; #5708), so
    /// the activation has no own record of it to apply and may release its count. Called
    /// off the grain turn, so an implementation must be thread-safe.
    /// </summary>
    void OnRefusedUnallocated();
}
