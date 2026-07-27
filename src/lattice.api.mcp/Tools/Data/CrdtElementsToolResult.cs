namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of an OR-Set, MV-Register, or Sequence read tool: the
/// current member / value bytes, each base64-encoded in the tool's JSON
/// structured content. An OR-Set read is unordered; an MV-Register read carries
/// one value normally and more than one only while concurrent writes are
/// unresolved; a Sequence read preserves collaborative insertion order. An absent
/// or unreadable key yields an empty list.
/// </summary>
public sealed record CrdtElementsToolResult
{
    /// <summary>Logical tree the CRDT lives on.</summary>
    public required string TreeId { get; init; }

    /// <summary>The CRDT key.</summary>
    public required string Key { get; init; }

    /// <summary>The current element / value bytes (base64-encoded in JSON structured content).</summary>
    public required IReadOnlyList<byte[]> Elements { get; init; }
}
