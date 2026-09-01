namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The default <see cref="IExplorerTenantScopeNotices"/>: a single mutable slot,
/// registered per Blazor circuit so one connection's outcome is never announced
/// on another's.
/// </summary>
internal sealed class ExplorerTenantScopeNotices : IExplorerTenantScopeNotices
{
    /// <inheritdoc />
    public ExplorerTenantScopeNotice? Current { get; private set; }

    /// <inheritdoc />
    public void Publish(ExplorerTenantScopeNotice notice)
    {
        ArgumentNullException.ThrowIfNull(notice);
        Current = notice;
    }

    /// <inheritdoc />
    public void Clear() => Current = null;
}
