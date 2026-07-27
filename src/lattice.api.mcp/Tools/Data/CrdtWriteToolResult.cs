namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured acknowledgement of a typed-CRDT write tool. A CRDT write returns no
/// payload of its own; this result confirms the delta was authorized and merged
/// into the key. A denied write throws rather than returning this result, so its
/// presence always means the mutation was applied.
/// </summary>
public sealed record CrdtWriteToolResult
{
    /// <summary>Logical tree the CRDT lives on.</summary>
    public required string TreeId { get; init; }

    /// <summary>The key the CRDT delta was applied to.</summary>
    public required string Key { get; init; }

    /// <summary>Always <see langword="true"/> when the tool returns a result - the delta merged.</summary>
    public bool Committed { get; init; } = true;
}
