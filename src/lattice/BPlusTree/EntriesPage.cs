using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A page of key-value entries returned from a shard's B+ tree scan.
/// Used for paginated entry enumeration to avoid loading all entries into memory.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.EntriesPage)]
internal sealed record EntriesPage
{
    /// <summary>The key-value entries in this page, in sorted key order.</summary>
    [Id(0)] public required List<KeyValuePair<string, byte[]>> Entries { get; init; }

    /// <summary>Whether more pages are available after this one.</summary>
    [Id(1)] public required bool HasMore { get; init; }

    /// <summary>
    /// Virtual slots whose entries were filtered out of <c>Entries</c> by
    /// the source shard because the slot has been (or is being) moved to
    /// another physical shard by an adaptive split. <c>null</c> or
    /// empty when no such filtering occurred. Strongly-consistent scans
    /// (<c>ILattice.EntriesAsync</c>) use this signal to query the new owner
    /// for the missing slots before completing.
    /// </summary>
    [Id(2)] public int[]? MovedAwaySlots { get; init; }

    /// <summary>
    /// The position the next request must resume from when this page stopped on
    /// the shard's per-call work bound rather than because it filled or reached
    /// the end of the range. <c>null</c> when the walk stopped for any other
    /// reason, in which case the caller resumes from the last entry as it always
    /// did.
    /// <para>
    /// It is a <em>leaf boundary</em>, not a returned key, which is what makes an
    /// empty page representable: a run of leaves whose entries are all
    /// tombstoned, TTL-expired, moved away by an adaptive split, or rejected by a
    /// pushed-down predicate contributes nothing to <see cref="Entries"/>, so
    /// there is no last entry to resume from and the walk would otherwise have to
    /// run to the end of the chain (issue 1992).
    /// </para>
    /// <para>
    /// A forward page reports the last visited leaf's exclusive high bound, which
    /// is exactly where the next leaf begins, so the next request resumes
    /// <em>at or after</em> it and skips nothing. A reverse page reports that
    /// leaf's inclusive low bound, and the next request resumes
    /// <em>strictly before</em> it. Either way the boundary lies strictly beyond
    /// the position the emitting call started from, so a caller looping until
    /// <see cref="HasMore"/> is <see langword="false"/> cannot re-issue an
    /// identical request.
    /// </para>
    /// </summary>
    [Id(3)] public string? ResumeFromKey { get; init; }
}
