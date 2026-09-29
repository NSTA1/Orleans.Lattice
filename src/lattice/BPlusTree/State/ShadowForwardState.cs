namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Per-shard state for the online shadow-forwarding primitive used by online
/// <c>ResizeAsync</c> (and any future online copy between physical trees).
/// While this state is non-null on a <see cref="ShardRootState"/>, each
/// last-writer-wins mutation (not a typed CRDT delta or a bulk append) is
/// mirrored in parallel to the shard with the same index on
/// <see cref="DestinationPhysicalTreeId"/>, so that a subsequent atomic
/// alias swap at the registry layer can hand off read and write traffic
/// to the destination tree.
/// <para>
/// <b>Resolution is last-writer-wins.</b> The destination keeps the highest-HLC
/// version of each key, which is why the primitive does not require a durable
/// shadow-retry queue or two-phase commit. Drained entries keep their source
/// HLC timestamps, but a live forward is stamped by the destination leaf's own
/// clock (see the shadow-forward notes on <c>ShardRootGrain</c>), so which
/// version survives can depend on which of the two arrives first.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShadowForwardState)]
internal sealed class ShadowForwardState
{
    /// <summary>
    /// Physical tree ID of the destination tree. Each mirrored mutation on the
    /// source shard goes to <c>{DestinationPhysicalTreeId}/{shardIndex}</c>,
    /// where <c>shardIndex</c> is the source shard's own index. The destination
    /// is registered with the source's pinned shard count, routing map and split
    /// allocation mark, so the same-index projection lands every key on the
    /// shard that owns it on both trees.
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
