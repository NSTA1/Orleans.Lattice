using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the <c>lattice_treeadmin_schema_list_dead_letters</c>
/// tool: the strict-mode dead-letter entries retained for a single tree.
/// </summary>
/// <remarks>
/// <para>
/// The facade streams the entries as an <see cref="IAsyncEnumerable{T}"/> so a
/// large queue enumerates with bounded memory; the tool drains that stream into
/// this record so it emits a single authoritative object shape. A dedicated record
/// (rather than a bare list return) avoids the MCP SDK's list-return shape
/// mismatch, where the text block renders the raw array (<c>[]</c>) but the
/// structured block wraps it under a synthetic <c>result</c> property
/// (<c>{"result":[]}</c>); returning an object makes both emitted copies identical,
/// exactly as the auth membership results do.
/// </para>
/// <para>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes. Each entry is
/// the reused <see cref="LatticeSchemaDeadLetterEntry"/> facade DTO.
/// </para>
/// </remarks>
public sealed record SchemaDeadLetterListResult
{
    /// <summary>The tree whose dead-letter entries were listed.</summary>
    public required string TreeId { get; init; }

    /// <summary>
    /// The strict-mode dead-letter entries retained for the tree. Empty when the
    /// tree has diverted no items.
    /// </summary>
    public required IReadOnlyList<LatticeSchemaDeadLetterEntry> Entries { get; init; }
}
