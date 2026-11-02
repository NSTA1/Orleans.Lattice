namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A tree's range-delete fence (epic #5605, member 2; design note section 2, "Range
/// delete: fenced at the tree"). One activation per physical tree, keyed by its id.
/// A range delete opens an intent here before any shard appends, which opens a
/// range-scoped marker on every shard root and hands out the tree's range-delete
/// generation; every shard's walk is stamped with it. When every walk has returned,
/// or the intent's deadline has passed, the close re-claims a higher generation at
/// every WAL partition, so one re-claim fences every shard's walk, records the heads
/// in every marker and closes them.
/// </summary>
/// <remarks>
/// <para>
/// <b>The open check is atomic with the close.</b> The grain is non-reentrant and
/// <see cref="GetOpenIntentGenerationAsync"/> is neither
/// <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> nor
/// <see cref="Orleans.Concurrency.ReadOnlyAttribute"/>, so the check and the
/// generation it hands out come from one state, in turns that never interleave with
/// a close; and a close marks its intent closing, durably, before its first
/// re-claim. No caller caches either answer.
/// </para>
/// <para>
/// It owns one reminder per tree, registered while any intent is open, whose tick
/// closes every intent past its deadline, so a fan-out whose stateless caller died
/// is closed all the same. Called only while the caller's append-fence gate is open.
/// </para>
/// </remarks>
[Alias(TypeAliases.ITreeRangeDeleteFenceGrain)]
internal interface ITreeRangeDeleteFenceGrain : IGrainWithStringKey
{
    /// <summary>
    /// Opens intent <paramref name="intentId"/> over <c>[start, end)</c>, opening a
    /// range-scoped marker on each of <paramref name="shards"/>, and returns the tree's
    /// range-delete generation every walk of the delete is stamped with. Idempotent for
    /// an intent already open.
    /// </summary>
    /// <param name="intentId">The range delete's operation id.</param>
    /// <param name="startInclusive">The inclusive start of the range.</param>
    /// <param name="endExclusive">The exclusive end of the range.</param>
    /// <param name="deadlineUtcTicks">When the reminder may close the intent; liveness only.</param>
    /// <param name="shards">Every physical shard index of the tree the delete fans out to.</param>
    Task<long> OpenIntentAsync(Guid intentId, string startInclusive, string endExclusive, long deadlineUtcTicks, int[] shards);

    /// <summary>
    /// The current range-delete generation when intent <paramref name="intentId"/> is
    /// open and not closing, otherwise <see langword="null"/>. Asked only by a walk
    /// whose stamped append was rejected: it re-stamps under its own intent only (rule
    /// 3), and otherwise goes on unstamped.
    /// </summary>
    Task<long?> GetOpenIntentGenerationAsync(Guid intentId);

    /// <summary>
    /// Closes intent <paramref name="intentId"/>: marks it closing, re-claims a higher
    /// generation at every WAL partition, records the heads in every marker it opened,
    /// closes them, and forgets it. Idempotent; a no-op for an unknown intent.
    /// </summary>
    Task CloseIntentAsync(Guid intentId);

    /// <summary>
    /// Retires this fence when its tree is purged (#5668): raises the range-delete
    /// initiator's routing floor to at least every generation it has handed out or
    /// claimed, then clears its state and unregisters its reminder. Any open intent is
    /// dropped with it: the shards whose markers it opened are purged. Called only by
    /// the tree's purge, before its registry entry is removed; idempotent.
    /// </summary>
    Task RetireForPurgeAsync();

    /// <summary>
    /// The highest range-delete generation this fence has handed out or claimed, which
    /// a purge's retirement covers with every initiator's routing floor before it
    /// deletes any row (#5668). 0 for a fence that never held one.
    /// </summary>
    Task<long> GetRetiringGenerationAsync();

    /// <summary>
    /// Claims this fence's current range-delete generation at every WAL partition,
    /// seeding it above the range-delete initiator's routing floor first if it has none,
    /// and returns the claim heads, indexed by partition (#5668). Driven by a re-created
    /// incarnation's shard roots before their markers read initialised, so a straddling
    /// walk of the purged incarnation, stamped with the range-delete initiator, is below
    /// heads they record. Idempotent: a re-claim of the same generation is accepted again.
    /// </summary>
    Task<long[]> EnsureInitialisedAsync();
}
