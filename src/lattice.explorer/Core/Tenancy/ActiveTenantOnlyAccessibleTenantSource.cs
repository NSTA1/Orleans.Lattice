namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The fail-closed default <see cref="IExplorerAccessibleTenantSource"/>: the
/// caller can reach the tenant they are already scoped to, and nothing else.
/// </summary>
/// <remarks>
/// The navigation core cannot enumerate a cluster's tenants - that is the
/// administrative surface's job, and it registers its own source before
/// <see cref="ExplorerTenantServiceCollectionExtensions.AddExplorerTenantView"/>
/// so the real list wins the <c>TryAdd</c>. Until one does, reporting only the
/// established tenant means the shell offers no way to reach a tenant it cannot
/// prove the caller may reach, and a remembered tenant that is not the current
/// one is abandoned with an explanation rather than silently restored.
/// </remarks>
internal sealed class ActiveTenantOnlyAccessibleTenantSource(IExplorerTenantContext context)
    : IExplorerAccessibleTenantSource
{
    private readonly IExplorerTenantContext _context =
        context ?? throw new ArgumentNullException(nameof(context));

    private ExplorerTenantId? _cachedFor;
    private ExplorerTenantId[] _cached = Array.Empty<ExplorerTenantId>();

    /// <inheritdoc />
    public ValueTask<IReadOnlyList<ExplorerTenantId>> GetAccessibleTenantsAsync(
        CancellationToken cancellationToken = default)
    {
        var active = _context.ActiveTenant;
        if (active is not { } tenant)
        {
            return new ValueTask<IReadOnlyList<ExplorerTenantId>>(Array.Empty<ExplorerTenantId>());
        }

        // One single-element array per distinct tenant rather than one per call:
        // this is asked on every tenant-control refresh, and the answer only
        // changes when the caller's scope does.
        if (_cachedFor != active)
        {
            _cachedFor = active;
            _cached = [tenant];
        }

        return new ValueTask<IReadOnlyList<ExplorerTenantId>>(_cached);
    }
}
