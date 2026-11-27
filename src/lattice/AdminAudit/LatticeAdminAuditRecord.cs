using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// One durable admin audit record: who (<see cref="ActorSubjectId"/>) attempted
/// which privileged administrative <see cref="Action"/> on which target
/// (<see cref="TargetScope"/>, <see cref="TargetKind"/>, <see cref="TargetId"/>),
/// and with what <see cref="Outcome"/> and <see cref="ReasonCode"/>. Records live in
/// the reserved <c>sys-admin-audit</c> tree.
/// </summary>
/// <remarks>
/// <para>
/// <b>No state copy.</b> A record never stores the changed state. It names the
/// anchor record the action writes (<see cref="AnchorTreeId"/>,
/// <see cref="AnchorKey"/>) and, once committed, the revision the call produced
/// (<see cref="CommittedRevisionHlc"/>, <see cref="AnchorValueHash"/>), so a reader
/// joins to the anchor tree's durable per-key history
/// (<see cref="ILattice.ScanEntryHistoryAsync(string, HybridLogicalClock?, HybridLogicalClock?, int, string?, CancellationToken)"/>)
/// for the state itself.
/// </para>
/// <para>
/// <b>No secret material.</b> Every field is an identifier, an enumeration, a
/// hash, a timestamp, or an allow-listed scalar in <see cref="Detail"/>. Rule
/// bodies, manifests, consent, credentials and exception messages are never
/// recorded.
/// </para>
/// <para>
/// <b>Identity.</b> <see cref="OperationId"/> identifies one facade call. The
/// intent (<see cref="LatticeAdminAuditOutcome.Pending"/>) and its final record
/// share it, and the final record's key is derived from it and from
/// <see cref="TimestampUtc"/>, so a call is finalized at most once.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.LatticeAdminAuditRecord)]
[Immutable]
public sealed record LatticeAdminAuditRecord
{
    /// <summary>The identifier of the facade call this record attributes.</summary>
    [Id(0)]
    public required string OperationId { get; init; }

    /// <summary>The record's outcome; only <see cref="LatticeAdminAuditOutcome.Committed"/> claims a change.</summary>
    [Id(1)]
    public LatticeAdminAuditOutcome Outcome { get; init; }

    /// <summary>The administrative action attempted.</summary>
    [Id(2)]
    public LatticeAdminAuditAction Action { get; init; }

    /// <summary>The authenticated subject the facade authorized (or refused); never a client-asserted id.</summary>
    [Id(3)]
    public required string ActorSubjectId { get; init; }

    /// <summary>The validated active tenant the call acted as, or <see langword="null"/>.</summary>
    [Id(4)]
    public string? ActorTenant { get; init; }

    /// <summary>How the actor holds its authority.</summary>
    [Id(5)]
    public LatticeAdminAuditActorKind ActorKind { get; init; }

    /// <summary>The partition the record is filed under: <c>cluster</c>, or <c>t/{tenant}</c> for a tenant's own administration.</summary>
    [Id(6)]
    public required string TargetScope { get; init; }

    /// <summary>What kind of object <see cref="TargetId"/> names.</summary>
    [Id(7)]
    public LatticeAdminAuditTargetKind TargetKind { get; init; }

    /// <summary>The identifier of the target object, or <see langword="null"/> when the call names none.</summary>
    [Id(8)]
    public string? TargetId { get; init; }

    /// <summary>The tree holding the anchor record the action writes, or <see langword="null"/> for a call that writes nothing (a read).</summary>
    [Id(9)]
    public string? AnchorTreeId { get; init; }

    /// <summary>The key of the anchor record within <see cref="AnchorTreeId"/>.</summary>
    [Id(10)]
    public string? AnchorKey { get; init; }

    /// <summary>
    /// The anchor's version observed before the commit, or <see langword="null"/>
    /// when the anchor did not exist or its tree does not expose versions.
    /// </summary>
    [Id(11)]
    public HybridLogicalClock? AnchorBaseVersion { get; init; }

    /// <summary>
    /// A content hash (xxHash64) of the anchor value observed before the commit,
    /// <c>0</c> when the anchor did not exist.
    /// </summary>
    [Id(12)]
    public long AnchorBaseHash { get; init; }

    /// <summary>
    /// On <see cref="LatticeAdminAuditOutcome.Committed"/>, a content hash
    /// (xxHash64) of the anchor value the call left, the same hash
    /// <see cref="EntryRevision.ValueHash"/> carries (<c>0</c> for a delete).
    /// </summary>
    [Id(13)]
    public long AnchorValueHash { get; init; }

    /// <summary>
    /// On <see cref="LatticeAdminAuditOutcome.Committed"/>, the anchor version the
    /// call produced, when the anchor tree exposes it: the join key into the anchor's
    /// history. <see langword="null"/> otherwise.
    /// </summary>
    [Id(14)]
    public HybridLogicalClock? CommittedRevisionHlc { get; init; }

    /// <summary>A stable code from <see cref="LatticeAdminAuditReasonCodes"/>, or <see langword="null"/> for a plain commit.</summary>
    [Id(15)]
    public string? ReasonCode { get; init; }

    /// <summary>Allow-listed, per-action scalar parameters (for example a rule's effect or a tenant's new status). Never a value or a secret.</summary>
    [Id(16)]
    public IReadOnlyDictionary<string, string>? Detail { get; init; }

    /// <summary>The replica identity of the region that authored the record (empty on a host without replication).</summary>
    [Id(17)]
    public string OriginRegion { get; init; } = string.Empty;

    /// <summary>When the record (for a mutation, its intent) was written.</summary>
    [Id(18)]
    public DateTimeOffset TimestampUtc { get; init; }

    /// <summary>When the record reached its final outcome, or <see langword="null"/> while it is <see cref="LatticeAdminAuditOutcome.Pending"/>.</summary>
    [Id(19)]
    public DateTimeOffset? CompletedUtc { get; init; }
}
