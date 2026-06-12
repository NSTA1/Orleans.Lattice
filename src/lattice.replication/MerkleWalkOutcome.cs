namespace Orleans.Lattice.Replication;

/// <summary>
/// The result of a read-only Merkle-walk drift-localisation pass: whether the
/// walk localised the divergence, how many leaves it narrowed to, the depth it
/// reached, why it aborted (if it did), and how many digest bytes it inspected.
/// This is an in-process result type and is not sent over the wire.
/// </summary>
public readonly record struct MerkleWalkOutcome
{
    /// <summary>
    /// <see langword="true"/> when the walk narrowed the mismatch to at least
    /// one diverging leaf.
    /// </summary>
    public bool Localised { get; init; }

    /// <summary>The number of diverging leaves the walk localised.</summary>
    public int LeavesLocalised { get; init; }

    /// <summary>
    /// The deepest level reached in the internal-node tree (<c>0</c> at the
    /// shard root).
    /// </summary>
    public int DepthReached { get; init; }

    /// <summary>
    /// Why the walk aborted, or <see cref="MerkleWalkAbortReason.None"/> when it
    /// did not abort.
    /// </summary>
    public MerkleWalkAbortReason AbortReason { get; init; }

    /// <summary>The total number of digest hash bytes the walk inspected.</summary>
    public long BytesInspected { get; init; }

    /// <summary>An outcome denoting no divergence was localised and no abort occurred.</summary>
    public static MerkleWalkOutcome NotLocalised => default;
}
