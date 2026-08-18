namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A machine-readable acknowledgement that the <c>repocontext_context</c> tool
/// <b>suppressed</b> content the caller already holds rather than delivering (and
/// charging for) it again. The tool emits one acknowledgement per suppressed thing,
/// so a caller can reconcile exactly what its receipts and possession claims saved:
/// a suppressed content <b>unit</b> carries <see cref="Kind"/> <c>"pointer"</c>,
/// <c>"span"</c>, or <c>"outline"</c> with the matching <see cref="Receipt"/>, while a
/// whole file suppressed by a possession claim carries <see cref="Kind"/>
/// <c>"file"</c> with the possessed <see cref="ContentHash"/> and no receipt.
/// <para>
/// A reused acknowledgement never counts against the request's <c>top</c> file budget
/// or its token budget - that is the whole point of reuse economics - so the presence
/// of an acknowledgement here is the caller's proof it was not charged twice.
/// </para>
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans grain
/// message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextReuseAck
{
    /// <summary>The repository-relative path of the file the suppressed content belongs to.</summary>
    public required string Path { get; init; }

    /// <summary>
    /// What was suppressed: a <c>"pointer"</c>, <c>"span"</c>, or <c>"outline"</c> unit
    /// (matched by <see cref="Receipt"/>), or a whole <c>"file"</c> (matched by a
    /// possession claim against <see cref="ContentHash"/>).
    /// </summary>
    public required string Kind { get; init; }

    /// <summary>
    /// The opaque receipt that matched, for a suppressed content unit; <see langword="null"/>
    /// for a whole-file suppression, which is matched by possession rather than by receipt.
    /// </summary>
    public string? Receipt { get; init; }

    /// <summary>
    /// The content hash of the file version that was suppressed. Always set for a
    /// <c>"file"</c> acknowledgement (the possessed version); set for a unit
    /// acknowledgement when the file version was known, otherwise <see langword="null"/>.
    /// </summary>
    public string? ContentHash { get; init; }

    /// <summary>
    /// The fully-qualified name of the declared symbol, for a suppressed <c>"outline"</c>
    /// unit; <see langword="null"/> otherwise.
    /// </summary>
    public string? Symbol { get; init; }
}
