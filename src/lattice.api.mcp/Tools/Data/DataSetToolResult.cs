namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the <c>data_set</c> point-write tool. The write is
/// acknowledged as committed only when the underlying facade returned without an
/// authorization denial; a caller who may not write the key is rejected before
/// this result is produced and nothing is persisted.
/// </summary>
public sealed record DataSetToolResult
{
    /// <summary>Logical tree the value was written to.</summary>
    public required string TreeId { get; init; }

    /// <summary>The key that was written.</summary>
    public required string Key { get; init; }

    /// <summary>Always <see langword="true"/> when the tool returns a result - the write committed.</summary>
    public bool Committed { get; init; } = true;
}
