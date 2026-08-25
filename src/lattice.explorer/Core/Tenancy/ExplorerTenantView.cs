namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The active, fail-closed <see cref="IExplorerTenantView"/>. The default read
/// path scopes to the caller's active tenant
/// (<see cref="IExplorerTenantContext.ActiveTenant"/>); the all-tenant path is
/// admitted only when the caller has requested
/// <see cref="ExplorerTenantVisibility.AllTenants"/> and
/// <see cref="IExplorerTenantOperatorGate.IsPlatformOperatorAsync"/> validates the
/// caller as a platform operator. Any other combination - a non-operator
/// requesting all-tenant, an anonymous caller, or no active tenant - falls through
/// to the active-tenant scope, so a tenant can never see another tenant's data.
/// </summary>
internal sealed class ExplorerTenantView(
    IExplorerTenantContext context,
    IExplorerTenantOperatorGate operatorGate) : IExplorerTenantView
{
    private readonly IExplorerTenantContext _context =
        context ?? throw new ArgumentNullException(nameof(context));

    private readonly IExplorerTenantOperatorGate _operatorGate =
        operatorGate ?? throw new ArgumentNullException(nameof(operatorGate));

    /// <inheritdoc />
    public bool IsActive => true;

    /// <inheritdoc />
    public ExplorerTenantId? ActiveTenant => _context.ActiveTenant;

    /// <inheritdoc />
    public async ValueTask<ExplorerTenantVisibility> ResolveEffectiveVisibilityAsync(
        CancellationToken cancellationToken = default)
    {
        if (_context.RequestedVisibility == ExplorerTenantVisibility.AllTenants &&
            await _operatorGate.IsPlatformOperatorAsync(cancellationToken).ConfigureAwait(false))
        {
            return ExplorerTenantVisibility.AllTenants;
        }

        // Fail-closed default: a non-operator all-tenant request, or any
        // active-tenant request, resolves to the caller's own tenant.
        return ExplorerTenantVisibility.ActiveTenant;
    }

    /// <inheritdoc />
    public bool IsVisible(ExplorerTenantVisibility effectiveVisibility, string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        if (effectiveVisibility == ExplorerTenantVisibility.AllTenants)
        {
            return true;
        }

        // Active-tenant scope: visible only when owned by the caller's active
        // tenant. No active tenant established -> nothing is visible.
        return _context.ActiveTenant is { } tenant && ExplorerTenantTrees.IsOwnedBy(treeId, tenant);
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<TItem>> ScopeAsync<TItem>(
        IReadOnlyList<TItem> items,
        Func<TItem, string> treeIdSelector,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(treeIdSelector);

        var effective = await ResolveEffectiveVisibilityAsync(cancellationToken).ConfigureAwait(false);
        if (effective == ExplorerTenantVisibility.AllTenants)
        {
            // Operator all-tenant view: return the list unchanged (no allocation).
            return items;
        }

        // Active-tenant scope with no active tenant established: fail closed to
        // nothing rather than leaking unattributable data.
        if (_context.ActiveTenant is not { } tenant)
        {
            return Array.Empty<TItem>();
        }

        // Filter to the caller's own tenant. The result list is allocated only
        // when at least one item is kept; a fully-filtered page returns the
        // shared empty array.
        List<TItem>? kept = null;
        for (var i = 0; i < items.Count; i++)
        {
            var item = items[i];
            if (ExplorerTenantTrees.IsOwnedBy(treeIdSelector(item), tenant))
            {
                kept ??= new List<TItem>(items.Count);
                kept.Add(item);
            }
        }

        return kept ?? (IReadOnlyList<TItem>)Array.Empty<TItem>();
    }
}
