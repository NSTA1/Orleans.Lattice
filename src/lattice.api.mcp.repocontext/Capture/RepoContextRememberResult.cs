namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of the <c>repocontext_remember</c> tool: the key the memory entry
/// was filed under, whether the write created a new entry or merged into an
/// existing one, and the entry's expiry after the write.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextRememberResult
{
    /// <summary>The full repository-context key the memory entry was written to.</summary>
    public required string Key { get; init; }

    /// <summary>The repository the entry belongs to.</summary>
    public required string RepoId { get; init; }

    /// <summary>The topic bucket the entry was filed under.</summary>
    public required string Topic { get; init; }

    /// <summary>The per-topic identifier of the entry (server-generated when the caller omitted one).</summary>
    public required string Id { get; init; }

    /// <summary>Whether the write created a new entry (<see langword="true"/>) or merged into an existing one.</summary>
    public required bool Created { get; init; }

    /// <summary>Whether the entry carries a finite expiry after the write.</summary>
    public required bool Expires { get; init; }

    /// <summary>The entry's absolute expiry in UTC <see cref="DateTime.Ticks"/>, or <c>0</c> when it never expires.</summary>
    public required long ExpiresAtTicks { get; init; }

    /// <summary>The number of knowledge-linking edges the write added to the entry.</summary>
    public int LinksAdded { get; init; }

    /// <summary>The number of knowledge-linking edges the write removed from the entry.</summary>
    public int LinksRemoved { get; init; }
}
