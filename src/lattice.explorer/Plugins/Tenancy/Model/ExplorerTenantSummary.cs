namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// A tenant as it appears in a list: its id, its lifecycle state, and whether it
/// is the reserved default tenant that owns un-prefixed trees.
/// <para>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/>, so a tenant list is one array rather than one
/// object per row.
/// </para>
/// </summary>
/// <param name="TenantId">The tenant id. Never <see langword="null"/> for a mapped value.</param>
/// <param name="Status">The tenant's lifecycle state.</param>
/// <param name="IsDefault">
/// <see langword="true"/> for the reserved default tenant, which cannot be
/// suspended, deleted, or have its admin subjects or grants edited.
/// </param>
public readonly record struct ExplorerTenantSummary(
    string TenantId,
    ExplorerTenantLifecycle Status,
    bool IsDefault);
