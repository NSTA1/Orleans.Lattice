namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// Transport-agnostic, read-only facade over the per-peer replication link
/// status a replication-enabled cluster tracks: for each replicated tree and
/// each peer region, in each direction, the outstanding backlog, the error
/// streak, the time since the last successful contact, the in-flight batch
/// count, and a derived <see cref="ReplicationLinkHealth"/>. Every transport
/// binding (the gRPC service and its public client) adapts over this single
/// surface. Deliberately separate from <see cref="ILatticeReplicationControl"/>,
/// which is unchanged and still owns enrolment
/// (<see cref="ILatticeReplicationControl.GetReplicationConfigAsync"/>); this
/// facade does not repeat it.
/// </summary>
/// <remarks>
/// <para>
/// <b>Authorization.</b> Requires the same capability
/// <see cref="ILatticeReplicationControl.GetReplicationConfigAsync"/> requires -
/// <see cref="LatticeOperation.Replication"/> over the whole tree - and fails
/// closed: a row is reported only for a tree the caller holds that capability
/// over, so a caller is never told that a tree it may not manage exists. A tree
/// filter naming a tree the caller may not manage yields an empty page, exactly
/// as a tree with no replication links does.
/// </para>
/// <para>
/// <b>Tree ids.</b> Each row carries the tree's <b>effective</b> id - the id the
/// cluster stores it under, and the id
/// <see cref="ILatticeReplicationControl.GetReplicationConfigAsync"/> reports the
/// same tree by, so the two reports join on tree id. A default-tenant tree is
/// reported by its bare name (an app tree as <c>a/{app}/{tree}</c>); under an
/// asserted, non-default tenant the caller's own trees are reported
/// tenant-qualified, as <c>t/{tenant}/{name}</c> (an app tree as
/// <c>t/{tenant}/a/{app}/{tree}</c>). Reporting the qualified id discloses nothing
/// new: it names only a tree the caller is already authorized to manage. A tree
/// filter is a tenant-local name, scoped to the caller's tenant exactly as the
/// enrolment verbs scope theirs, so either the tenant-local name or the qualified
/// id a report returned selects the same tree.
/// </para>
/// <para>
/// <b>Scope and freshness.</b> The report reflects the whole local cluster
/// (every active silo), read on demand from in-memory telemetry. It is a
/// point-in-time observation, not a durable record: it carries no cursor
/// position, pending batch count or error class, because the cluster does not
/// track them per link.
/// </para>
/// </remarks>
public interface ILatticeReplicationStatus
{
    /// <summary>
    /// Reads one page of per-peer replication link status, ordered by tree id,
    /// then peer region id, then direction. Page through the whole report by
    /// passing each page's <see cref="ReplicationPeerStatusPage.ContinuationToken"/>
    /// back in <see cref="ReplicationPeerStatusQuery.ContinuationToken"/> until it
    /// is <see langword="null"/>. A page can hold fewer rows than the page size
    /// and still carry a continuation.
    /// </summary>
    /// <param name="query">The filters, page size and continuation. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The page, including the local region id.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="query"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="ReplicationPeerStatusQuery.PageSize"/> is negative.</exception>
    /// <exception cref="ArgumentException"><see cref="ReplicationPeerStatusQuery.ContinuationToken"/> is malformed.</exception>
    Task<ReplicationPeerStatusPage> GetPeerStatusAsync(
        ReplicationPeerStatusQuery query,
        CancellationToken cancellationToken = default);
}
