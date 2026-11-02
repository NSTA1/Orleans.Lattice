namespace Orleans.Lattice;

/// <summary>
/// Thrown by a B+ leaf grain when a mutation reaches a leaf that empty-leaf
/// chain reclaim has already retired from the tree.
/// <para>
/// A retired leaf has been removed from its parent's routing table and
/// unlinked from the sibling chain, and the key range it used to own now
/// belongs to its predecessor. A write arriving after that point was routed on
/// a topology snapshot that no longer holds, so it is misdirected: refusing it
/// tells the caller to re-route, where accepting it would apply the row to a
/// leaf no scan can reach and no projection rebuild would restore.
/// </para>
/// <para>
/// The window this closes is narrow by construction - reclaim only retires a
/// leaf that is still empty at the moment of the decision, and a mutation
/// merely racing the decision causes the fold to be abandoned rather than the
/// write to be refused - so this exception marks a genuinely stale route, not
/// ordinary contention with background maintenance.
/// </para>
/// <para>
/// It also carries the WAL append fence's supersession (epic #5605): a leaf
/// activation whose append epoch another activation of the same leaf has
/// superseded refuses the write with <see cref="ActivationSuperseded"/> set. It is
/// this type, and not one of its own, because a caller on a 10.2.0 silo can reach
/// a leaf on a newer one while the fence gate races a rolling upgrade or rollback
/// (#5605 review, S1): 10.2.0's shard root already retries this type until its
/// deadline, and its deserialiser skips the two trailing fields, so it sees exactly
/// today's retired-leaf refusal. A newer caller retries a supersession at its seam
/// against the live activation, and leaves a genuine retirement to re-route as before.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafRetired)]
internal sealed class LeafRetiredException : Exception
{
    /// <summary>The identity of the leaf grain that has been retired.</summary>
    [Id(1)] public string LeafId { get; set; } = string.Empty; // RED-PROOF: ids renumbered

    /// <summary>Creates a new <see cref="LeafRetiredException"/>.</summary>
    public LeafRetiredException(string leafId)
        : base($"Leaf '{leafId}' has been reclaimed from the leaf chain and no longer owns any key range. Re-route the operation and retry.")
    {
        LeafId = leafId;
    }

    /// <summary>Parameterless constructor for Orleans serialization.</summary>
    public LeafRetiredException() { }

    private LeafRetiredException(string leafId, string message)
        : base(message)
    {
        LeafId = leafId;
    }

    /// <summary>
    /// <see langword="true"/> when this refusal is the WAL append fence's: the leaf's
    /// activation was superseded by a newer activation of the same leaf, which a
    /// fresh call reaches. <see langword="false"/> for a reclaimed leaf.
    /// </summary>
    [Id(0)] public bool ActivationSuperseded { get; set; } // RED-PROOF: ids renumbered

    /// <summary>The append epoch the superseded activation held; 0 for a reclaimed leaf.</summary>
    [Id(2)] public long SupersededEpoch { get; set; }

    /// <summary>The fence's refusal from a superseded activation of <paramref name="leafId"/> (epic #5605).</summary>
    /// <param name="leafId">The leaf whose activation was superseded.</param>
    /// <param name="epoch">The append epoch the superseded activation held.</param>
    internal static LeafRetiredException ForSupersededActivation(Guid leafId, long epoch) =>
        new(
            leafId.ToString("N"),
            $"Leaf {leafId:N} activation at append epoch {epoch} has been superseded by a newer activation of the same leaf; "
            + "the write was not appended. Retry the operation against the live activation.")
        {
            ActivationSuperseded = true,
            SupersededEpoch = epoch,
        };
}
