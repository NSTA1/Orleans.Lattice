namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the <c>data_set_many_atomic</c> single-tree atomic-batch
/// tool. The batch is acknowledged as committed only when every leg was
/// authorized and applied all-or-nothing; a single denied leg aborts the whole
/// batch before this result is produced, with nothing persisted.
/// </summary>
public sealed record DataAtomicBatchToolResult
{
    /// <summary>Logical tree the batch was committed on.</summary>
    public required string TreeId { get; init; }

    /// <summary>The idempotency key the batch was committed under.</summary>
    public required string OperationId { get; init; }

    /// <summary>Always <see langword="true"/> when the tool returns a result - the batch committed atomically.</summary>
    public bool Committed { get; init; } = true;
}
