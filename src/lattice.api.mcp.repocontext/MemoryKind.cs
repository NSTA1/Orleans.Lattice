namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The kind of an agent-authored <see cref="MemoryRecord"/>. Captured once when
/// the record is created and treated as immutable record identity rather than a
/// mutable CRDT scalar.
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.MemoryKind)]
internal enum MemoryKind
{
    /// <summary>Kind not specified (the default for a never-classified record).</summary>
    Unspecified = 0,

    /// <summary>A durable decision with rationale (an architectural or design choice).</summary>
    Decision,

    /// <summary>A free-form note or observation about the codebase.</summary>
    Note,

    /// <summary>Short-lived working memory an agent accumulates across a task.</summary>
    Memory,
}
