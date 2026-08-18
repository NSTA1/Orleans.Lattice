namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of the <c>repocontext_outline</c> tool: the structural skeleton of a
/// single source file - one entry per declared symbol with its kind, signature, and
/// line span - together with the token cost of reading the whole file, so an agent
/// can decide what to read at a fraction of a full-file read's token budget.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextOutlineResult
{
    /// <summary>The repository the file belongs to.</summary>
    public required string RepoId { get; init; }

    /// <summary>The repository-relative path of the outlined file.</summary>
    public required string Path { get; init; }

    /// <summary>
    /// Whether a stored file node exists at <see cref="Path"/>. When
    /// <see langword="false"/>, <see cref="Symbols"/> is empty and
    /// <see cref="FullReadTokenCount"/> is <see langword="null"/>.
    /// </summary>
    public required bool Exists { get; init; }

    /// <summary>
    /// The token cost of reading the whole file under the configured tokenizer
    /// profile: the indexed per-file count when the file node carries one, otherwise
    /// a bounded count of the stored content projection, or <see langword="null"/>
    /// when neither is available (the file was not content-processed).
    /// </summary>
    public required int? FullReadTokenCount { get; init; }

    /// <summary>
    /// The declared symbols in ascending start-line order (ties broken by
    /// fully-qualified name); empty when the file declares none or does not exist.
    /// </summary>
    public required IReadOnlyList<RepoContextOutlineSymbol> Symbols { get; init; }
}
