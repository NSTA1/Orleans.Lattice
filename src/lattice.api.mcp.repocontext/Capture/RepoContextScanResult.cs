namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One page of the ordered range walked by the <c>repocontext_scan</c> tool: the
/// projected entries in ascending key order plus an opaque
/// <see cref="ContinuationToken"/> that resumes the scan immediately after the
/// last returned entry.
/// <para>
/// TTL-expired and tombstoned entries are never yielded, so a page only ever
/// contains live records. When <see cref="HasMore"/> is <see langword="false"/>
/// the range is exhausted and <see cref="ContinuationToken"/> is
/// <see langword="null"/>.
/// </para>
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextScanResult
{
    /// <summary>The projected entries on this page, in ascending key order.</summary>
    public required IReadOnlyList<RepoContextEntryView> Entries { get; init; }

    /// <summary>
    /// An opaque token that resumes the scan immediately after the last entry on
    /// this page, or <see langword="null"/> when the range is exhausted. Pass it
    /// back verbatim as the next call's <c>continuationToken</c>.
    /// </summary>
    public string? ContinuationToken { get; init; }

    /// <summary>Whether more entries remain beyond this page.</summary>
    public required bool HasMore { get; init; }
}
