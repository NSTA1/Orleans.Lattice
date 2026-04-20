namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Per-shard state for the online shadow-forwarding primitive used by online
/// <c>ResizeAsync</c> (and any future online copy between physical trees).
/// While this state is non-null on a <see cref="ShardRootState"/>, every
/// accepted mutation is mirrored in parallel to the corresponding shard on
/// <see cref="DestinationPhysicalTreeId"/>, so that a subsequent atomic
/// alias swap at the registry layer can hand off read and write traffic
/// to the destination tree with zero data loss.
/// <para>
/// <b>Correctness relies on LWW commutativity.</b> Every write carries an
/// HLC timestamp; concurrent shadow-forwards and background drain writes
/// converge to the same final state regardless of interleaving because
/// the highest HLC wins on every key. This is why the primitive does not
/// require a durable shadow-retry queue or two-phase commit.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShadowForwardState)]
internal sealed class ShadowForwardState
{
    /// <summary>
    /// Physical tree ID of the destination tree. Each mutation on the source
    /// shard is mirrored to <c>{DestinationPhysicalTreeId}/{shardIndex}</c>
    /// where <c>shardIndex</c> matches the source shard's own index — the
    /// destination tree is constrained to share the source's
    /// <c>ShardMap</c> so this projection is identity.
    /// </summary>
    [Id(0)] public string DestinationPhysicalTreeId { get; set; } = "";

    /// <summary>
    /// Current phase of the per-shard shadow-forwarding lifecycle. See
    /// <see cref="BPlusTree.ShadowForwardPhase"/> for semantics.
    /// </summary>
    [Id(1)] public ShadowForwardPhase Phase { get; set; }

    /// <summary>
    /// Coordinator-supplied operation ID. Used for idempotent re-entry of
    /// <c>BeginShadowForwardAsync</c>, <c>MarkDrainedAsync</c>, and
    /// <c>EnterRejectingAsync</c> across coordinator crash-resume. A phase
    /// transition against a different <see cref="OperationId"/> is refused,
    /// preventing a stale coordinator from interfering with a newer
    /// operation.
    /// </summary>
    [Id(2)] public string OperationId { get; set; } = "";

    /// <summary>
    /// User-visible logical tree ID (the name the caller passed to
    /// <c>ILattice</c>) for which this shard is participating in an online
    /// copy. Stamped here so that, when the shard later transitions to
    /// <see cref="ShadowForwardPhase.Rejecting"/>, the thrown
    /// <see cref="StaleTreeRoutingException"/> can carry the correct
    /// logical name even though the shard's own grain key is keyed on the
    /// physical tree ID. Empty on pre-existing state; a blank value
    /// falls back to the shard's physical tree ID in error messages.
    /// </summary>
    [Id(3)] public string LogicalTreeId { get; set; } = "";
}
