namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// The durable half of a WAL placement move's fence (issue #4525): a record, kept
/// in the <see cref="WalPlacementPin"/> beside the partition's placement, that a
/// move is copying the partition's log away from
/// <see cref="SourceProviderKey"/>.
/// <para>
/// The in-memory fence a <c>QuiesceForMoveAsync</c> raises dies with its
/// activation. This record does not: every activation of the source reads it
/// with the pin and comes up fenced while it is held, so losing the fenced
/// activation can no longer let a fresh one acknowledge an append the copy never
/// saw. The record is removed only by the placement flip that ends the move, by
/// the move coordinator when it aborts, or - once <see cref="LeaseExpiresUtcTicks"/>
/// has passed - by the next activation of the source, which is what keeps a
/// partition from staying fenced after its coordinator dies. The flip requires
/// the record to still be held by its own move, so a fence released by expiry
/// can never be followed by a cutover that strands what the source accepted
/// after the release.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.WalMoveFence)]
[Immutable]
internal sealed record WalMoveFence
{
    /// <summary>
    /// The identity of the move that holds the fence. A move renews and flips
    /// only against its own identity, so a fence that was released and then
    /// raised again by a different move is never mistaken for the original.
    /// </summary>
    [Id(0)] public string MoveId { get; init; } = "";

    /// <summary>
    /// The provider key the partition was placed on when the fence was raised.
    /// Only an activation that resolves this key is fenced; once the placement
    /// has moved on, a leftover record is inert.
    /// </summary>
    [Id(1)] public string SourceProviderKey { get; init; } = "";

    /// <summary>
    /// The UTC instant (in <see cref="DateTime.Ticks"/>) after which the fence
    /// may be released by the next activation of the source. Only liveness
    /// depends on it: safety rests on the flip requiring the fence to be held.
    /// </summary>
    [Id(2)] public long LeaseExpiresUtcTicks { get; init; }
}
