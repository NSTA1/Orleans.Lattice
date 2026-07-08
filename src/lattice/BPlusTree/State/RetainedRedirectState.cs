namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Per-shard state marking this shard's physical tree as <em>retained but
/// superseded</em> by a shadow-cutover restore. While this state is non-null,
/// a logical-alias-routed operation on this shard is refused with a
/// <see cref="StaleTreeRoutingException"/> pointing at
/// <see cref="DestinationPhysicalTreeId"/>, so a stale
/// <see cref="Orleans.Concurrency.StatelessWorkerAttribute"/> routing activation
/// that still caches the pre-restore alias self-heals onto the destination
/// tree instead of silently serving pre-restore data.
/// <para>
/// Unlike <see cref="ShadowForwardState"/> this primitive never forwards
/// writes: the retained tree is a frozen point-in-time snapshot kept only so a
/// restore can be reverted, so mirroring live writes into it would corrupt the
/// revert target. The redirect fires only for traffic that arrived via the
/// logical alias (identified by the
/// <c>LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey</c> marker);
/// direct-physical access (e.g. a revert reading the retained tree by its
/// physical ID) and internal maintenance keep reading the retained tree.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.RetainedRedirectState)]
[Immutable]
internal sealed class RetainedRedirectState
{
    /// <summary>
    /// Physical tree ID that now owns the logical alias this shard's tree used
    /// to answer. A refused logical-routed operation carries this ID in the
    /// thrown <see cref="StaleTreeRoutingException.DestinationPhysicalTreeId"/>
    /// for diagnostics; the caller re-resolves the alias via the registry.
    /// </summary>
    [Id(0)] public string DestinationPhysicalTreeId { get; init; } = "";

    /// <summary>
    /// Restore operation ID that installed this redirect. Used for idempotent
    /// re-marking and to gate <c>ClearRetainedRedirectAsync</c> so a stale
    /// coordinator cannot clear a newer operation's redirect.
    /// </summary>
    [Id(1)] public string OperationId { get; init; } = "";

    /// <summary>
    /// User-visible logical tree ID whose alias was redirected. Stamped into
    /// the thrown <see cref="StaleTreeRoutingException.LogicalTreeId"/> so the
    /// caller refreshes the correct alias even though this shard's grain key
    /// only encodes the physical tree ID. Falls back to the physical tree ID
    /// when empty.
    /// </summary>
    [Id(2)] public string LogicalTreeId { get; init; } = "";
}
