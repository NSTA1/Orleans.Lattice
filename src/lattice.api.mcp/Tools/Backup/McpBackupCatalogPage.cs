namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_backup_list</c> tool: one page of
/// compact manifest projections ordered by backup id, plus the
/// <see cref="NextPageToken"/> continuation cursor to pass back for the next
/// page. <see cref="NextPageToken"/> is <see langword="null"/> on the final page.
/// This cursor-paged tool is the MCP-appropriate mapping of the facade's
/// unbounded backup stream: it never materialises the whole catalog in one call.
/// </summary>
internal sealed record McpBackupCatalogPage
{
    /// <summary>The backup manifests on this page, ordered by backup id.</summary>
    public IReadOnlyList<McpBackupManifest> Entries { get; init; } = Array.Empty<McpBackupManifest>();

    /// <summary>
    /// The continuation cursor for the next page, or <see langword="null"/> when
    /// this is the last page.
    /// </summary>
    public string? NextPageToken { get; init; }
}
