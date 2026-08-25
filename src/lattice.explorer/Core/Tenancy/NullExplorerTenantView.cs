namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The inactive <see cref="IExplorerTenantView"/> used when tenant scoping is not
/// enabled. It performs no scoping: <see cref="IsActive"/> is
/// <see langword="false"/> so callers skip the seam entirely, and every member is
/// a no-op that preserves the caller's input. This is what guarantees a
/// non-tenant cluster's Explorer is byte-for-byte unchanged.
/// </summary>
internal sealed class NullExplorerTenantView : IExplorerTenantView
{
    /// <summary>The shared inactive instance.</summary>
    public static NullExplorerTenantView Instance { get; } = new();

    private NullExplorerTenantView()
    {
    }

    /// <inheritdoc />
    public bool IsActive => false;

    /// <inheritdoc />
    public ExplorerTenantId? ActiveTenant => null;

    /// <inheritdoc />
    public ValueTask<ExplorerTenantVisibility> ResolveEffectiveVisibilityAsync(
        CancellationToken cancellationToken = default) =>
        new(ExplorerTenantVisibility.AllTenants);

    /// <inheritdoc />
    public bool IsVisible(ExplorerTenantVisibility effectiveVisibility, string treeId) => true;

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<TItem>> ScopeAsync<TItem>(
        IReadOnlyList<TItem> items,
        Func<TItem, string> treeIdSelector,
        CancellationToken cancellationToken = default) =>
        new(items);
}
