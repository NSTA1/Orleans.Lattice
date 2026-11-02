namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A stamped WAL append was rejected because the routing generation it was admitted
/// under has been re-claimed by a hand-off close (epic #5605, member 2; design note
/// section 5): the write was admitted before the close and not allocated in time, so
/// it must be re-admitted under the current routing, never re-stamped at the leaf.
/// Nothing was allocated. A leaf path that applies before it appends (the batched
/// CRDT apply) has applied in memory, and the shard root's retry applies again on the
/// same activation; that is safe because a CRDT join is idempotent (review N1).
/// </summary>
/// <remarks>
/// <para>
/// Internal to the seam retry: raised by a leaf's stamped write and caught by the
/// shard root step that admitted it, which re-runs its admission (a shard routing
/// generation: <c>ThrowIfRejectedForKey</c> and the stale-routing retry) or asks the
/// tree's range-delete fence whether its own intent is still open (the range-delete
/// generation). It never reaches a public caller and is never an inner exception of
/// anything public.
/// </para>
/// <para>
/// Derives directly from <see cref="Exception"/>, so the same-silo deep copy needs no
/// registered copier.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(TypeAliases.WalRoutingGenerationStaleException)]
internal sealed class WalRoutingGenerationStaleException : Exception
{
    /// <summary>Creates the exception for a write admitted under <paramref name="generation"/> of <paramref name="initiator"/>.</summary>
    /// <param name="initiator">The initiator whose generation was refused.</param>
    /// <param name="generation">The generation the write was admitted under.</param>
    /// <param name="refusal">The partition's refusal threshold, or 0 when it was not read.</param>
    public WalRoutingGenerationStaleException(WalRoutingInitiator initiator, long generation, long refusal = 0)
        : base($"The routing generation {generation} of {initiator.Kind} {initiator.Index} was refused (threshold {refusal}); the write is re-admitted under the current routing.")
    {
        Initiator = initiator;
        Generation = generation;
        Refusal = refusal;
    }

    /// <summary>Creates an empty exception; for serialization.</summary>
    public WalRoutingGenerationStaleException()
    {
    }

    /// <summary>The initiator whose generation was re-claimed.</summary>
    [Id(0)]
    public WalRoutingInitiator Initiator { get; set; }

    /// <summary>The generation the write was admitted under.</summary>
    [Id(1)]
    public long Generation { get; set; }

    /// <summary>
    /// The partition's refusal threshold: the generation it holds for the initiator,
    /// which a re-admission must reach. 0 when it was not read. A shard root whose
    /// marker is still at or below it raises its marker above it (#5663), so a marker
    /// behind a partition's durable row cannot refuse every write to the deadline.
    /// </summary>
    [Id(2)]
    public long Refusal { get; set; }
}
