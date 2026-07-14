namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_backup_restore</c> and
/// <c>lattice_backup_revert_restore</c> tools: the applied backup and target, the
/// resolved idempotency key, the base-first replayed chain, the entry count, and
/// - for a shadow-cutover restore - the physical tree ids retained for revert.
/// The full shape is returned so an agent can round-trip it back into
/// <c>lattice_backup_revert_restore</c> to undo a shadow-cutover.
/// </summary>
internal sealed record McpRestoreResult
{
    /// <summary>The backup id restored.</summary>
    public required string BackupId { get; init; }

    /// <summary>The tree restored into.</summary>
    public required string TargetTreeId { get; init; }

    /// <summary>The restore mode applied: <c>InPlace</c> or <c>ShadowCutover</c>.</summary>
    public required string Mode { get; init; }

    /// <summary>The resolved idempotency key.</summary>
    public required string OperationId { get; init; }

    /// <summary>The base-first ordered chain of backup ids that were replayed.</summary>
    public IReadOnlyList<string> ManifestChain { get; init; } = Array.Empty<string>();

    /// <summary>The number of entries installed.</summary>
    public required long EntriesApplied { get; init; }

    /// <summary>The physical tree id the alias now resolves to (shadow-cutover only), or <see langword="null"/>.</summary>
    public string? ShadowPhysicalTreeId { get; init; }

    /// <summary>The physical tree id retained for revert (shadow-cutover only), or <see langword="null"/>.</summary>
    public string? PreviousPhysicalTreeId { get; init; }
}
