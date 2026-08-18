namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The server-derived <em>reason</em> vocabulary attached to every
/// <see cref="RepoContextSearchHit"/> so an agent can tell <b>why</b> a record
/// ranked. A reason is a short, machine-readable tag derived solely from stored,
/// server-side state - the matched vector's canonical source key on the semantic
/// path, and the projected record's own fields on the keyword path - never from
/// wire-supplied query text echoed back verbatim. The set is deterministic and
/// ordinal-ordered, and the number of reasons per hit is bounded by
/// <see cref="MaxReasons"/> so the additive output stays cheap in tokens.
/// <para>
/// <b>Vocabulary.</b> Semantic hits emit <see cref="Semantic"/>, the matched chunk
/// kind (<see cref="ChunkSymbol"/> or <see cref="ChunkFile"/>), and, for a symbol
/// vector, <c>symbol:&lt;fqName&gt;</c>. Keyword hits emit, in a fixed
/// high-signal-first order, <see cref="PathNameMatch"/>, <c>symbol:&lt;fqName&gt;</c>,
/// <c>tag:&lt;tag&gt;</c> (one per matched tag, in ordinal order),
/// <see cref="TopicMatch"/>, <see cref="ContentMatch"/>, and
/// <see cref="KeyMatch"/>.
/// </para>
/// </summary>
internal static class RepoContextSearchReasons
{
    /// <summary>
    /// The maximum number of reasons attached to a single hit. Enforced
    /// deterministically on both paths so the additive surface never balloons the
    /// tool's output tokens; excess lower-signal reasons are dropped in order.
    /// </summary>
    internal const int MaxReasons = 6;

    /// <summary>Reason emitted on every semantic hit: the vector index answered.</summary>
    internal const string Semantic = "semantic";

    /// <summary>Reason marking a semantic hit whose matched vector is a symbol vector.</summary>
    internal const string ChunkSymbol = "chunk:symbol";

    /// <summary>Reason marking a semantic hit whose matched vector is a file-chunk vector.</summary>
    internal const string ChunkFile = "chunk:file";

    /// <summary>Reason marking a keyword hit that matched on the record's file/package path.</summary>
    internal const string PathNameMatch = "path-name-match";

    /// <summary>Reason marking a keyword hit that matched on the record's memory topic.</summary>
    internal const string TopicMatch = "topic-match";

    /// <summary>Reason marking a keyword hit that matched inside a scored content field (body text, signature, title, ...).</summary>
    internal const string ContentMatch = "content-match";

    /// <summary>Reason marking a keyword hit that matched on the record's key.</summary>
    internal const string KeyMatch = "key-match";

    /// <summary>Prefix for the reason naming the matched symbol's fully-qualified name.</summary>
    internal const string SymbolPrefix = "symbol:";

    /// <summary>Prefix for the reason naming a matched tag.</summary>
    internal const string TagPrefix = "tag:";

    private static readonly string[] SemanticOnly = { Semantic };
    private static readonly string[] SemanticFile = { Semantic, ChunkFile };
    private static readonly string[] SemanticSymbol = { Semantic, ChunkSymbol };

    /// <summary>
    /// Builds the reasons for a semantic hit from the canonical source key of the
    /// matched vector. A symbol vector yields <see cref="Semantic"/>,
    /// <see cref="ChunkSymbol"/>, and <c>symbol:&lt;fqName&gt;</c>; a file-chunk
    /// vector yields <see cref="Semantic"/> and <see cref="ChunkFile"/>; any other
    /// (or an unparseable) source key yields <see cref="Semantic"/> alone. The key
    /// is parsed server-side, so no wire-supplied text is echoed.
    /// </summary>
    /// <param name="sourceKey">The matched vector's store-of-record source key.</param>
    /// <returns>The ordered, capped reasons for the hit; never <see langword="null"/>.</returns>
    internal static IReadOnlyList<string> ForSemantic(string? sourceKey)
    {
        if (string.IsNullOrEmpty(sourceKey) || !RepoContextKeys.TryParse(sourceKey, out var parsed))
        {
            return SemanticOnly;
        }

        switch (parsed.Kind)
        {
            case RepoContextRecordKind.Symbol:
                var fqName = parsed.FullyQualifiedName;
                return string.IsNullOrEmpty(fqName)
                    ? SemanticSymbol
                    : new[] { Semantic, ChunkSymbol, SymbolPrefix + fqName };

            case RepoContextRecordKind.File:
            case RepoContextRecordKind.Content:
                return SemanticFile;

            default:
                return SemanticOnly;
        }
    }
}
