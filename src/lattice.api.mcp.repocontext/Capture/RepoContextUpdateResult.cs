namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of the <c>repocontext_update</c> tool: the key that was patched and
/// a summary of how many scalar fields and tags the CRDT merge applied.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextUpdateResult
{
    /// <summary>The full repository-context key of the patched record.</summary>
    public required string Key { get; init; }

    /// <summary>The record family the key addresses (for example <c>File</c> or <c>Memory</c>).</summary>
    public required string Kind { get; init; }

    /// <summary>The number of scalar fields the patch set to a new last-writer-wins value.</summary>
    public required int FieldsUpdated { get; init; }

    /// <summary>The number of tags the patch added to the record's add-wins set.</summary>
    public required int TagsAdded { get; init; }

    /// <summary>The number of tags the patch removed from the record's add-wins set.</summary>
    public required int TagsRemoved { get; init; }

    /// <summary>The number of knowledge-linking edges the patch added to the record.</summary>
    public int LinksAdded { get; init; }

    /// <summary>The number of knowledge-linking edges the patch removed from the record.</summary>
    public int LinksRemoved { get; init; }
}
