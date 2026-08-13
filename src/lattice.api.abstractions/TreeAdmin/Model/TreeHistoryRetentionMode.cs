namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// How much of an LWW (last-writer-wins) byte value a tree's durable-history
/// revision rows retain per revision. Mirrors the core engine's own
/// history-retention mode as a transport-agnostic value the tree-administration
/// facade can accept and return without the abstractions package taking a
/// dependency on the core library.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeHistoryRetentionMode)]
public enum TreeHistoryRetentionMode
{
    /// <summary>
    /// The default: store a revision's metadata only (content hash and byte
    /// length) and not the value bytes. Tiny and bounded; the full bytes for a
    /// revision still inside the TTL-pinned source window can be fetched lazily.
    /// </summary>
    MetadataOnly = 0,

    /// <summary>
    /// Store the full value bytes for every revision (still TTL-bounded). Use for
    /// trees that need point-in-time values directly from the history rather than
    /// a lazy source fetch.
    /// </summary>
    FullValue = 1,

    /// <summary>
    /// Store full value bytes for revisions still recent at apply time (within the
    /// configured window) and metadata only for older revisions. Bounds full-byte
    /// storage to the recent tail while keeping an unbounded metadata-only timeline.
    /// </summary>
    Hybrid = 2,
}
