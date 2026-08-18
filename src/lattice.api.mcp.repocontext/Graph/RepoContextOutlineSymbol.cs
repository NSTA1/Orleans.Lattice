namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One declared symbol in a <see cref="RepoContextOutlineResult"/>: its
/// fully-qualified name, structural kind, declaration signature, and 1-based line
/// span. Together the entries form a file's skeleton without its bodies.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextOutlineSymbol
{
    /// <summary>The symbol's fully-qualified name.</summary>
    public required string FullyQualifiedName { get; init; }

    /// <summary>The symbol's structural kind (for example <c>Type</c>, <c>Method</c>, <c>Property</c>).</summary>
    public required string Kind { get; init; }

    /// <summary>The declaration signature, or the empty string when none was recorded.</summary>
    public required string Signature { get; init; }

    /// <summary>The 1-based start line of the symbol's span, or 0 when unknown.</summary>
    public required long StartLine { get; init; }

    /// <summary>The 1-based end line of the symbol's span, or 0 when unknown.</summary>
    public required long EndLine { get; init; }
}
