namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// A stamped WAL append was rejected because the routing generation it was admitted
/// under has been re-claimed by a hand-off close (epic #5605, member 2; design note
/// section 5): the write was admitted before the close and not allocated in time, so
/// it must be re-admitted under the current routing, never re-stamped at the leaf.
/// Nothing was allocated and nothing was applied.
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
    public WalRoutingGenerationStaleException(WalRoutingInitiator initiator, long generation)
        : base($"The routing generation {generation} of {initiator.Kind} {initiator.Index} was re-claimed by a hand-off close; the write is re-admitted under the current routing.")
    {
        Initiator = initiator;
        Generation = generation;
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
}
