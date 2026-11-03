namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A compact, agent-friendly projection of a backup manifest for MCP structured
/// content. It carries the identifying and catalog fields an agent reasons about
/// (id, name, capture time, kind, scope, chain base, and set grouping) rather
/// than the full self-describing manifest - the topology snapshot, per-key
/// descriptors, provenance, and content descriptors are deliberately omitted so
/// a <c>tools/list</c> result stays small and legible.
/// </summary>
internal sealed record McpBackupManifest
{
    /// <summary>The content-addressed backup id (the catalog key).</summary>
    public required string Id { get; init; }

    /// <summary>The human-readable backup name recorded on the manifest.</summary>
    public required string Name { get; init; }

    /// <summary>The wall-clock time the backup was captured.</summary>
    public required DateTimeOffset CreatedAtUtc { get; init; }

    /// <summary>Whether the backup is <c>Full</c> or <c>Incremental</c>.</summary>
    public required string Kind { get; init; }

    /// <summary>The captured tree id.</summary>
    public required string TreeId { get; init; }

    /// <summary>The scope extent: <c>WholeTree</c>, <c>Prefix</c>, or <c>Key</c>.</summary>
    public required string ScopeKind { get; init; }

    /// <summary>
    /// The exact key or key prefix the scope covers, or <see langword="null"/>
    /// for a whole-tree scope.
    /// </summary>
    public string? KeyOrPrefix { get; init; }

    /// <summary>
    /// The base backup id this incremental is layered on, or
    /// <see langword="null"/> for a full backup.
    /// </summary>
    public string? BaseBackupId { get; init; }

    /// <summary>The number of content-addressed artifacts the backup references.</summary>
    public required int ArtifactCount { get; init; }

    /// <summary>
    /// The id of the backup set this backup was captured as a member of, or
    /// <see langword="null"/> when it was captured on its own.
    /// </summary>
    public string? SetId { get; init; }

    /// <summary>
    /// The human-readable name of the enclosing backup set, or
    /// <see langword="null"/> when the backup is not a set member.
    /// </summary>
    public string? SetName { get; init; }

    /// <summary>
    /// The id of the cluster that authored the capture, or
    /// <see langword="null"/> for a manifest captured before the stamp existed.
    /// </summary>
    public string? CapturingClusterId { get; init; }
}
