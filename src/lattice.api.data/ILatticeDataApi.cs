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
/// <b>Scope.</b> v1 exposes point writes / deletes, single-tree atomic batches,
/// cross-tree atomic batches, point reads, and a single-page bounded range read.
/// A live streaming scan / change feed is intentionally out of scope.
/// </para>
/// </remarks>
internal interface ILatticeDataApi
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
}
