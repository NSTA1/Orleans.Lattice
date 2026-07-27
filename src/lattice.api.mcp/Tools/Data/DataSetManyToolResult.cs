namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the <c>data_set_many</c> non-atomic bulk-write tool. The
/// write is acknowledged only when every targeted key was authorized and the
/// per-shard fan-out completed; because the write is <b>not</b> atomic there is
/// no idempotency key echoed and no all-or-nothing guarantee - a mid-flight
/// failure surfaces as a fault rather than this result.
/// </summary>
public sealed record DataSetManyToolResult
{
    /// <summary>Logical tree the values were written to.</summary>
    public required string TreeId { get; init; }

    /// <summary>The number of key / value pairs submitted for writing.</summary>
    public required int Count { get; init; }

    /// <summary>Always <see langword="true"/> when the tool returns a result - the fan-out completed.</summary>
    public bool Committed { get; init; } = true;
}
