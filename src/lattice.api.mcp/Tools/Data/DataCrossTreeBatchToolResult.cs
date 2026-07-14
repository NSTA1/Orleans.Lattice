namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the <c>data_set_many_atomic_cross_tree</c> tool.
/// <see cref="Committed"/> is <see langword="true"/> when every participating
/// tree's batch committed, <see langword="false"/> when a guard predicate aborted
/// the batch with nothing committed (a precondition miss reported as a value, not
/// an exception). A caller denied any targeted key is rejected before any apply.
/// </summary>
public sealed record DataCrossTreeBatchToolResult
{
    /// <summary>The cross-tree idempotency key the batch was committed under.</summary>
    public required string OperationId { get; init; }

    /// <summary>
    /// The terminal outcome name: <c>Committed</c> when every tree's batch
    /// committed, or <c>PreconditionFailed</c> when a guard aborted the batch.
    /// </summary>
    public required string Outcome { get; init; }

    /// <summary><see langword="true"/> when the outcome was a full cross-tree commit.</summary>
    public bool Committed { get; init; }
}
