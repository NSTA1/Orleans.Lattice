namespace Orleans.Lattice.Backup;

/// <summary>
/// A request to capture a backup <i>set</i>: one full backup per scope in
/// <see cref="Scopes"/>, grouped under a single <see cref="Name"/>. Each scope
/// selects a region of a distinct tree (whole-tree, per-prefix, or per-key).
/// <para>
/// When <see cref="CrossTreeConsistent"/> is <c>true</c> and the set covers more
/// than one tree, the capture selects a single causal fence and captures every
/// tree as of that fence, so a cross-tree atomic write is never torn across the
/// set boundary. A single-tree set (or a set with the flag left <c>false</c>)
/// issues no extra coordination and keeps the cheap per-tree cut.
/// </para>
/// </summary>
public sealed record LatticeBackupSetCaptureRequest
{
    /// <summary>Initializes a new <see cref="LatticeBackupSetCaptureRequest"/>.</summary>
    /// <param name="name">The human-readable set name recorded on every member manifest and the set manifest. Must not be <c>null</c> or empty.</param>
    /// <param name="scopes">The per-tree scopes to capture. Must not be <c>null</c>, must be non-empty, and every scope must name a distinct tree.</param>
    /// <param name="crossTreeConsistent">
    /// Whether to capture every tree at a single cross-tree causal fence. Ignored
    /// (no coordination is paid) when <paramref name="scopes"/> covers a single
    /// tree. Defaults to <c>false</c>.
    /// </param>
    /// <param name="pageSize">The raw-entry drain page size for each member capture. Must be positive. Defaults to <see cref="LatticeBackupCaptureRequest.DefaultPageSize"/>.</param>
    /// <exception cref="ArgumentException"><paramref name="name"/> is <c>null</c> or empty, <paramref name="scopes"/> is empty, or two scopes name the same tree.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="scopes"/> is <c>null</c> or contains a <c>null</c> scope.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="pageSize"/> is not positive.</exception>
    public LatticeBackupSetCaptureRequest(
        string name,
        IReadOnlyList<BackupScopeSelector> scopes,
        bool crossTreeConsistent = false,
        int pageSize = LatticeBackupCaptureRequest.DefaultPageSize)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(scopes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);
        if (scopes.Count == 0)
        {
            throw new ArgumentException("A backup set must cover at least one tree.", nameof(scopes));
        }

        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var scope in scopes)
        {
            ArgumentNullException.ThrowIfNull(scope);
            if (!seen.Add(scope.TreeId))
            {
                throw new ArgumentException(
                    $"A backup set cannot contain two scopes for the same tree ('{scope.TreeId}').",
                    nameof(scopes));
            }
        }

        Name = name;
        Scopes = scopes;
        CrossTreeConsistent = crossTreeConsistent;
        PageSize = pageSize;
    }

    /// <summary>The human-readable set name recorded on every member and the set manifest.</summary>
    public string Name { get; init; }

    /// <summary>The per-tree scopes to capture, one per distinct tree.</summary>
    public IReadOnlyList<BackupScopeSelector> Scopes { get; init; }

    /// <summary>Whether to capture every tree at a single cross-tree causal fence.</summary>
    public bool CrossTreeConsistent { get; init; }

    /// <summary>The raw-entry drain page size for each member capture.</summary>
    public int PageSize { get; init; }
}
