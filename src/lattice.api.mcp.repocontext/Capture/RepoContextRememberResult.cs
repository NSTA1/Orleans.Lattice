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

    /// <summary>
    /// Whether the entry carries a finite expiry after the write, or
    /// <see langword="null"/> when the expiry was <b>not evaluated</b> because the
    /// post-commit read that reports it faulted.
    /// <para>
    /// Nullable for the same reason <see cref="RepoContextEntryView.Expires"/> is:
    /// the expiry is read back after the durable write, so it can be unavailable on a
    /// call whose write nonetheless succeeded. Reporting <see langword="false"/> there
    /// would present an unmeasured value as a measured one, which is indistinguishable
    /// from "this entry is durable" and is exactly the wrong direction to guess in.
    /// </para>
    /// </summary>
    public bool? Expires { get; init; }

    /// <summary>The entry's absolute expiry as an ISO-8601 UTC timestamp (round-trip "O" format), <see langword="null"/> when it never expires or when the expiry was not evaluated (see <see cref="Expires"/>).</summary>
    public string? ExpiresAtUtc { get; init; }

    /// <summary>The number of knowledge-linking edges the write added to the entry.</summary>
    public int LinksAdded { get; init; }

    /// <summary>The number of knowledge-linking edges the write removed from the entry.</summary>
    public int LinksRemoved { get; init; }
}
