namespace Orleans.Lattice;

/// <summary>
/// Controls how much of an LWW (last-writer-wins) byte value a durable history
/// view retains per revision row. CRDT revisions are always stored as their
/// author delta regardless of this mode (the delta <em>is</em> the compact
/// history), so the mode only governs <see cref="MutationKind.Set"/> value bytes.
/// <para>
/// The mode is a <b>live-tunable policy</b>, not part of the projection's code
/// identity: changing it never trips a view rebuild. The view maintainer reads
/// the current mode from the source tree's registry configuration at drain time
/// and shapes each emitted revision row accordingly, so a change is absorbed
/// forward (already-written rows keep their stamped shape; new rows adopt the new
/// mode).
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.HistoryRetentionMode)]
public enum HistoryRetentionMode
{
    /// <summary>
    /// The default for LWW values: store the revision's metadata only - a
    /// content hash and the byte length - and <b>not</b> the value bytes. Tiny
    /// and bounded; the full bytes for a revision still inside the TTL-pinned
    /// source WAL window can be fetched lazily by the read path.
    /// </summary>
    MetadataOnly = 0,

    /// <summary>
    /// Store the full value bytes for every revision (still TTL-bounded). Use for
    /// trees that need point-in-time values directly from the history view rather
    /// than a lazy source-WAL fetch.
    /// </summary>
    FullValue = 1,

    /// <summary>
    /// Store full value bytes for revisions that are still recent at apply time
    /// (within the configured hybrid full-value window) and metadata only for
    /// older revisions. Bounds full-byte storage to the recent tail while keeping
    /// an unbounded metadata-only timeline behind it.
    /// </summary>
    Hybrid = 2,
}
