namespace Orleans.Lattice.Api.Data;

/// <summary>
/// Transport-agnostic read-write facade over a cluster's lattice data plane.
/// Every transport binding (the gRPC service now, a future surface later) is a
/// thin adapter over this single surface, so the write and read semantics are
/// written and tested once and no transport concern leaks into the data logic.
/// </summary>
/// <remarks>
/// <para>
/// Every operation obtains the cluster grain via
/// <c>GetGrain&lt;ILattice&gt;(treeId)</c> and calls the <b>same</b> public
/// <see cref="ILattice"/> method the in-cluster client uses, so the
/// authorization enforcement wired at the cluster grain fires automatically once
/// the caller identity flows on the ambient <see cref="LatticeCredentialContext"/>.
/// The facade adds no authorization path of its own.
/// </para>
/// <para>
/// <b>Fail-closed.</b> An unresolved / anonymous caller is default-denied by the
/// access gate: mutations throw <see cref="LatticeAuthorizationDeniedException"/>,
/// a point read of a hidden key reports absent, and a range read prunes to the
/// (possibly empty) authorized subset.
/// </para>
/// <para>
/// <b>Scope.</b> Exposes point writes / deletes, a non-atomic bulk write,
/// single-tree atomic batches, cross-tree atomic batches, point reads, a
/// single-page bounded range read, and typed CRDT operations (counter, set,
/// flags, version vector, multi-value register, sequence, and map). A live
/// streaming scan / change feed is intentionally out of scope.
/// </para>
/// </remarks>
public interface ILatticeDataApi
{
    /// <summary>
    /// Writes <paramref name="value"/> at <paramref name="key"/> on
    /// <paramref name="treeId"/>. Throws
    /// <see cref="LatticeAuthorizationDeniedException"/> when the caller may not
    /// write the key (an anonymous caller included); nothing is persisted before
    /// the throw.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The entry key to write.</param>
    /// <param name="value">The value bytes to store.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes <paramref name="key"/> on <paramref name="treeId"/>. Returns
    /// <see langword="true"/> when a live value existed and was removed. Throws
    /// <see cref="LatticeAuthorizationDeniedException"/> when the caller may not
    /// delete the key.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The entry key to delete.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> DeleteAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits <paramref name="batch"/> (upserts and deletes) all-or-nothing on
    /// <paramref name="treeId"/>, keyed by <paramref name="operationId"/> for
    /// idempotent retry. Throws
    /// <see cref="LatticeAuthorizationDeniedException"/> when the caller may not
    /// write / delete any targeted key - every leg is authorized before any
    /// apply, so a single denied leg aborts the whole batch with nothing
    /// persisted.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="batch">The upserts and deletes to commit atomically.</param>
    /// <param name="operationId">Stable idempotency key. Must be non-empty and must not contain <c>'/'</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SetManyAtomicAsync(
        string treeId,
        DataAtomicBatch batch,
        string operationId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits <paramref name="batches"/> across every named tree
    /// all-or-nothing, keyed by <paramref name="operationId"/>. Returns
    /// <see cref="CrossTreeAtomicWriteOutcome.Committed"/> when every tree's
    /// batch committed, or
    /// <see cref="CrossTreeAtomicWriteOutcome.PreconditionFailed"/> when a guard
    /// aborted the batch with nothing committed. Throws
    /// <see cref="LatticeAuthorizationDeniedException"/> when the caller may not
    /// write / delete any targeted key on any participating tree - every leg is
    /// authorized before any apply.
    /// </summary>
    /// <param name="batches">Per-tree slices to commit atomically. Tree ids must be distinct and non-empty.</param>
    /// <param name="operationId">Required cross-tree idempotency key. Must not contain <c>'/'</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<CrossTreeAtomicWriteOutcome> SetManyAtomicCrossTreeAsync(
        IReadOnlyList<DataTreeBatch> batches,
        string operationId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Writes <paramref name="upserts"/> on <paramref name="treeId"/>
    /// <b>non-atomically</b>, fanning the batch out per shard and committing each
    /// shard slice independently. This is the cheap bulk-write path: it skips the
    /// all-or-nothing atomic-write saga, the mandatory idempotency journal, and
    /// the cross-shard barrier, so it is the right default for loading many
    /// independent keys where per-key durability is acceptable.
    /// </summary>
    /// <remarks>
    /// <b>Not atomic.</b> A partial failure may leave some keys written and
    /// others not, with no compensating rollback and no idempotency key to make a
    /// retry a safe no-op. Use <see cref="SetManyAtomicAsync"/> when all-or-nothing
    /// semantics are required. Authorization is enforced per key exactly as the
    /// atomic batch does: a caller who may not write any targeted key is denied
    /// with <see cref="LatticeAuthorizationDeniedException"/>.
    /// </remarks>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="upserts">The key / value pairs to write. An empty list is a no-op.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SetManyAsync(
        string treeId,
        IReadOnlyList<DataEntry> upserts,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the value at <paramref name="key"/> on <paramref name="treeId"/>.
    /// A key the caller may not read (an anonymous caller included) reports
    /// absent (<see cref="DataReadResult.Found"/> is <see langword="false"/>),
    /// never a value.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The entry key to read.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<DataReadResult> GetAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads one page of the bounded range described by <paramref name="request"/>,
    /// in ascending key order, pruned to the caller's authorized key subset. An
    /// unauthorized (or anonymous) caller reads back an empty page.
    /// </summary>
    /// <param name="request">Range scope, page size, and optional continuation token.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<DataRangePage> ReadRangeAsync(DataRangeRequest request, CancellationToken cancellationToken = default);

    // --- Typed CRDT operations (issue #1361) -------------------------------
    //
    // Each verb translates a semantic delta into the correct encoded CRDT delta
    // and routes it through the same fail-closed authorized write path as
    // SetAsync - the caller never hand-encodes CRDT bytes. Every write throws
    // LatticeAuthorizationDeniedException when the caller may not write the key.
    // The tree's per-key merge mode is authoritative: on a replicated tree a
    // verb whose mode does not match the tree's enrolled mode is rejected by the
    // engine, and an OR-Map verb on a tree with no registered map shape faults -
    // both surface as clean typed failures at the transport edge.

    /// <summary>
    /// Increments the PN-counter at <paramref name="key"/> by
    /// <paramref name="amount"/> on behalf of <paramref name="replicaId"/>. A
    /// PN-counter converges by summing every replica's own increments and
    /// decrements, so concurrent counts from many clusters all survive the merge.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the counter is stored under.</param>
    /// <param name="replicaId">Stable id of the writer whose per-replica tally this increment lands on.</param>
    /// <param name="amount">The non-negative amount to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default);

    /// <summary>
    /// Decrements the PN-counter at <paramref name="key"/> by
    /// <paramref name="amount"/> on behalf of <paramref name="replicaId"/>.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the counter is stored under.</param>
    /// <param name="replicaId">Stable id of the writer whose per-replica tally this decrement lands on.</param>
    /// <param name="amount">The non-negative amount to subtract.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CounterDecrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default);

    /// <summary>Reads the converged total of the PN-counter at <paramref name="key"/> (0 when absent or unreadable).</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the counter is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<long> CounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds <paramref name="element"/> to the OR-Set at <paramref name="key"/> on
    /// behalf of <paramref name="replicaId"/>. An OR-Set converges add-wins under
    /// observed-remove: a concurrent add and remove of the same element keeps it.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the set is stored under.</param>
    /// <param name="element">The opaque element bytes to add.</param>
    /// <param name="replicaId">Stable id of the writer performing the add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes <paramref name="element"/> from the OR-Set at <paramref name="key"/>.
    /// Observed-remove: only the adds this caller has observed are retracted, so a
    /// concurrent unobserved add survives.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the set is stored under.</param>
    /// <param name="element">The opaque element bytes to remove.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SetRemoveAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default);

    /// <summary>Reads the live members of the OR-Set at <paramref name="key"/> (empty when absent or unreadable).</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the set is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<byte[]>> SetGetAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Enables the enable-wins (OR) flag at <paramref name="key"/> on behalf of
    /// <paramref name="replicaId"/>. Under concurrent enable / disable the enable
    /// wins, so presence is add-wins.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the flag is stored under.</param>
    /// <param name="replicaId">Stable id of the writer performing the enable.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task OrFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default);

    /// <summary>Disables the enable-wins (OR) flag at <paramref name="key"/> by retracting the enables this caller has observed.</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the flag is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task OrFlagDisableAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>Reads whether the enable-wins (OR) flag at <paramref name="key"/> is currently enabled (false when absent or unreadable).</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the flag is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> OrFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>Enables the remove-wins (RW) flag at <paramref name="key"/> on behalf of <paramref name="replicaId"/>.</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the flag is stored under.</param>
    /// <param name="replicaId">Stable id of the writer performing the enable.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task RwFlagEnableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Disables the remove-wins (RW) flag at <paramref name="key"/> on behalf of
    /// <paramref name="replicaId"/>. Under concurrent enable / disable the disable
    /// wins, so a revoke is never silently resurrected by a concurrent re-enable.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the flag is stored under.</param>
    /// <param name="replicaId">Stable id of the writer performing the disable.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task RwFlagDisableAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default);

    /// <summary>Reads whether the remove-wins (RW) flag at <paramref name="key"/> is currently enabled (false when absent or unreadable).</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the flag is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> RwFlagGetAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Increments the grow-only (G) counter at <paramref name="key"/> by
    /// <paramref name="amount"/> on behalf of <paramref name="replicaId"/>. A
    /// G-counter only ever increases and converges by summing every replica's
    /// own increments, so concurrent counts from many clusters all survive the
    /// merge.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the counter is stored under.</param>
    /// <param name="replicaId">Stable id of the writer whose per-replica tally this increment lands on.</param>
    /// <param name="amount">The non-negative amount to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task GCounterIncrementAsync(string treeId, string key, string replicaId, long amount, CancellationToken cancellationToken = default);

    /// <summary>Reads the converged total of the grow-only (G) counter at <paramref name="key"/> (0 when absent or unreadable).</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the counter is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<long> GCounterGetAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds <paramref name="element"/> to the grow-only (G) set at
    /// <paramref name="key"/>. A G-Set is add-only and converges by set union:
    /// the add is idempotent and needs no replica context, so concurrent adds
    /// from any number of writers all survive the merge. There is no remove
    /// operation by design - use an OR-Set when removal is required.
    /// Adds <paramref name="element"/> to the remove-wins (RW) set at
    /// <paramref name="key"/> on behalf of <paramref name="replicaId"/>. An
    /// RW-Set converges remove-wins: a concurrent add and remove of the same
    /// element keeps it out, so a revoke is never silently resurrected by a
    /// concurrent re-add.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the set is stored under.</param>
    /// <param name="element">The opaque element bytes to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task GSetAddAsync(string treeId, string key, byte[] element, CancellationToken cancellationToken = default);

    /// <summary>Reads the members of the grow-only (G) set at <paramref name="key"/> (empty when absent or unreadable).</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the set is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<byte[]>> GSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default);
    /// <param name="replicaId">Stable id of the writer performing the add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task RwSetAddAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes <paramref name="element"/> from the remove-wins (RW) set at
    /// <paramref name="key"/> on behalf of <paramref name="replicaId"/>. A
    /// remove-wins remove mints a fresh surviving dot, so it requires
    /// <paramref name="replicaId"/> just as the add does; the remove dominates
    /// any concurrent add that has not observed it.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the set is stored under.</param>
    /// <param name="element">The opaque element bytes to remove.</param>
    /// <param name="replicaId">Stable id of the writer performing the remove.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task RwSetRemoveAsync(string treeId, string key, byte[] element, string replicaId, CancellationToken cancellationToken = default);

    /// <summary>Reads the live members of the remove-wins (RW) set at <paramref name="key"/> (empty when absent or unreadable).</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the set is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<byte[]>> RwSetGetAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Advances the version vector at <paramref name="key"/> for
    /// <paramref name="replicaId"/> (a causal "tick"). The vector converges by
    /// per-replica maximum, tracking who has seen what so concurrency is
    /// detectable.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the vector is stored under.</param>
    /// <param name="replicaId">Stable id of the writer whose entry is advanced.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task VersionVectorTickAsync(string treeId, string key, string replicaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the version vector at <paramref name="key"/> as a map of replica id
    /// to that replica's clock, formatted <c>"wallClockTicks:counter"</c> (empty
    /// when absent or unreadable).
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the vector is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyDictionary<string, string>> VersionVectorGetAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the multi-value register at <paramref name="key"/> to
    /// <paramref name="value"/> on behalf of <paramref name="replicaId"/>.
    /// Concurrent sets from different replicas survive as distinct values rather
    /// than one silently overwriting the other; read them back with
    /// <see cref="RegisterGetAsync"/> and resolve the conflict in the application.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the register is stored under.</param>
    /// <param name="replicaId">Stable id of the writer performing the set.</param>
    /// <param name="value">The opaque value bytes to set.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task RegisterSetAsync(string treeId, string key, string replicaId, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the multi-value register at <paramref name="key"/> as its current set
    /// of concurrent values - one value normally, more than one only while
    /// concurrent writes are unresolved (empty when absent or unreadable).
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the register is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<byte[]>> RegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Advances the monotone max register at <paramref name="key"/> towards
    /// <paramref name="value"/> - the high-water-mark primitive that keeps the
    /// greatest value ever seen. The opaque data API orders candidates by their
    /// raw value bytes (unsigned lexicographic), so a write that is not strictly
    /// greater than the current value is a durable no-op. Concurrent writes from
    /// different clusters converge on the single greatest value.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the register is stored under.</param>
    /// <param name="value">The opaque candidate value bytes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MaxRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>Reads the current value of the monotone max register at <paramref name="key"/> (null when absent or unreadable).</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the register is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<byte[]?> MaxRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Advances the monotone min register at <paramref name="key"/> towards
    /// <paramref name="value"/> - the low-water-mark primitive that keeps the
    /// smallest value ever seen. The opaque data API orders candidates by their
    /// raw value bytes (unsigned lexicographic), so a write that is not strictly
    /// smaller than the current value is a durable no-op. Concurrent writes from
    /// different clusters converge on the single smallest value.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the register is stored under.</param>
    /// <param name="value">The opaque candidate value bytes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MinRegisterSetAsync(string treeId, string key, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>Reads the current value of the monotone min register at <paramref name="key"/> (null when absent or unreadable).</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the register is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<byte[]?> MinRegisterGetAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Inserts <paramref name="value"/> at zero-based <paramref name="index"/> in
    /// the RGA sequence at <paramref name="key"/> on behalf of
    /// <paramref name="replicaId"/>. Concurrent inserts converge on a
    /// deterministic order; a later remove tombstones the node.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the sequence is stored under.</param>
    /// <param name="index">The zero-based position to insert at (clamped to the current length).</param>
    /// <param name="replicaId">Stable id of the writer performing the insert.</param>
    /// <param name="value">The opaque element bytes to insert.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SequenceInsertAtAsync(string treeId, string key, int index, string replicaId, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>Removes (tombstones) the element at zero-based <paramref name="index"/> in the RGA sequence at <paramref name="key"/>.</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the sequence is stored under.</param>
    /// <param name="index">The zero-based position to remove.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SequenceRemoveAtAsync(string treeId, string key, int index, CancellationToken cancellationToken = default);

    /// <summary>Reads the RGA sequence at <paramref name="key"/> as its ordered list of live elements (empty when absent or unreadable).</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the sequence is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<byte[]>> SequenceGetAsync(string treeId, string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Puts <paramref name="value"/> under map field <paramref name="field"/> in
    /// the OR-Map at <paramref name="key"/> on behalf of
    /// <paramref name="replicaId"/>. Map keys follow add-wins observed-remove and
    /// each field holds a multi-value register, so concurrent puts to the same
    /// field survive as concurrent values. The tree must have a registered OR-Map
    /// shape (a string-keyed, multi-value-register map); a verb against a tree
    /// with no such shape faults cleanly.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the map is stored under.</param>
    /// <param name="field">The map field to write.</param>
    /// <param name="replicaId">Stable id of the writer performing the put.</param>
    /// <param name="value">The opaque value bytes to store under the field.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MapSetAsync(string treeId, string key, string field, string replicaId, byte[] value, CancellationToken cancellationToken = default);

    /// <summary>Removes map field <paramref name="field"/> from the OR-Map at <paramref name="key"/> (observed-remove).</summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the map is stored under.</param>
    /// <param name="field">The map field to remove.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task MapRemoveAsync(string treeId, string key, string field, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the OR-Map at <paramref name="key"/> as a map of field to that
    /// field's current concurrent values (empty when absent or unreadable). A
    /// field normally has one value, more than one only while concurrent writes
    /// to that field are unresolved.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="key">The key the map is stored under.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyDictionary<string, IReadOnlyList<byte[]>>> MapGetAsync(string treeId, string key, CancellationToken cancellationToken = default);
}
