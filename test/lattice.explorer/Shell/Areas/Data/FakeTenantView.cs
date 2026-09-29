using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Data;

/// <summary>
/// An active tenant view that scopes a listing to the trees its tenant owns, by
/// the same <c>t/</c> ownership rule the real view applies.
/// </summary>
internal sealed class FakeTenantView(string tenant) : IExplorerTenantView
{
    /// <inheritdoc />
    public bool IsActive => true;

    /// <inheritdoc />
    public ExplorerTenantId? ActiveTenant { get; } = new ExplorerTenantId(tenant);

    /// <inheritdoc />
    public ValueTask<ExplorerTenantVisibility> ResolveEffectiveVisibilityAsync(CancellationToken cancellationToken = default) =>
        new(ExplorerTenantVisibility.ActiveTenant);

    /// <inheritdoc />
    public bool IsVisible(ExplorerTenantVisibility effectiveVisibility, string treeId) =>
        ExplorerTenantTrees.IsOwnedBy(treeId, ActiveTenant!.Value);

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<TItem>> ScopeAsync<TItem>(
        IReadOnlyList<TItem> items,
        Func<TItem, string> treeIdSelector,
        CancellationToken cancellationToken = default) =>
        new([.. items.Where(item => IsVisible(ExplorerTenantVisibility.ActiveTenant, treeIdSelector(item)))]);
}
