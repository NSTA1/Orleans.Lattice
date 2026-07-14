namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the <c>lattice_state_get_physical_shard_count</c> tool.
/// A dedicated payload (rather than a bare integer) so the tool can report both
/// the count and whether the tree existed at all: when the tree is unknown
/// <see cref="PhysicalShardCount"/> is <see langword="null"/> and
/// <see cref="TreeExists"/> is <see langword="false"/>.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record PhysicalShardCountResult
{
    /// <summary>The tree id that was queried.</summary>
    public required string TreeId { get; init; }

    /// <summary>
    /// The number of physical shards currently owning virtual slots for the
    /// tree, or <see langword="null"/> when the tree does not exist.
    /// </summary>
    public int? PhysicalShardCount { get; init; }

    /// <summary>
    /// Whether the tree exists. <see langword="false"/> is the typed not-found
    /// signal that distinguishes an unknown tree from a tree with zero shards.
    /// </summary>
    public bool TreeExists => PhysicalShardCount is not null;
}

/// <summary>
/// Structured result of the <c>lattice_state_cancel_scan</c> tool: an
/// acknowledgement that the server-side snapshot cursor release was requested.
/// The underlying operation is best-effort and idempotent, so the tool always
/// reports success - an empty, unknown, already-drained, or already-closed
/// cursor token is a tolerated no-op rather than a fault.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record ScanCancellationResult
{
    /// <summary>The tree the cursor was opened against.</summary>
    public required string TreeId { get; init; }

    /// <summary>
    /// Always <see langword="true"/>: the release was requested and the
    /// best-effort, idempotent operation completed without faulting.
    /// </summary>
    public bool Acknowledged { get; init; } = true;
}
