namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Verified core for the write-ahead log's per-shard sequence-number
/// (offset) allocation. The WAL shard grain assigns every appended entry a
/// dense, strictly-ascending offset by reading a monotone counter and
/// advancing it by one; <see cref="Assign"/> is that read-and-advance step as
/// a single pure function, so both the production append paths
/// (<c>WalShardGrain</c>) and the Coyote model
/// (<c>WalOffsetContiguityModel</c>) execute the identical logic.
/// <para>
/// The correctness of the dense-offset invariant does not live inside this
/// function - a read-then-advance is only atomic if the caller performs it
/// under a mutual-exclusion region. The shard grain calls <see cref="Assign"/>
/// while holding its internal state gate, so two concurrently interleaved
/// <c>[AlwaysInterleave]</c> append turns cannot observe the same counter
/// value. The load-bearing property the model checks is exactly that the
/// read and the advance are one indivisible step: if a schedule can slip
/// between the read and the write of <paramref name="nextOffset"/>, two
/// entries are handed the same offset and a batch's offsets stop being
/// contiguous. See <see cref="WalMoveFenceCore"/>, which guards the same
/// state-gate region against a shard-move quiesce racing the assignment.
/// </para>
/// </summary>
internal static class WalOffsetAllocationCore
{
    /// <summary>
    /// Reads the next unassigned offset from <paramref name="nextOffset"/> and
    /// advances the counter by one, returning the offset the caller should
    /// stamp on the entry. The caller MUST hold the shard's state gate so the
    /// read and advance are indivisible; assigning entries in call order then
    /// yields a dense, strictly-ascending offset run (<c>entry[i+1] ==
    /// entry[i] + 1</c>) with no duplicates and no gaps.
    /// </summary>
    /// <param name="nextOffset">The monotone next-offset counter, advanced in place.</param>
    /// <returns>The offset to assign to the current entry.</returns>
    public static long Assign(ref long nextOffset)
    {
        var offset = nextOffset;
        nextOffset = offset + 1;
        return offset;
    }
}
