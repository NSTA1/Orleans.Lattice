using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The circuit's <see cref="ILatticeActiveTenantProvider"/>: the tenant every call
/// the circuit makes asserts, read from its <see cref="IExplorerTenantContext"/>
/// each time a call starts.
/// </summary>
/// <remarks>
/// <para>
/// It asserts the context's active tenant, and nothing when there is none. It
/// also asserts nothing for the reserved default tenant, because a call with no
/// assertion is exactly a default-tenant call: the cluster resolves an absent
/// assertion to the default tenant without consulting the caller's membership,
/// whereas asserting <c>default</c> would be re-validated as a tenant the caller
/// administers and refused for an operator who does not. The header is therefore
/// sent only when it changes what the cluster does.
/// </para>
/// <para>
/// Registered scoped by
/// <see cref="ExplorerTenantServiceCollectionExtensions.AddExplorerTenantView"/>,
/// so it reads one circuit's context and no other. With tenancy off it is not
/// registered at all and no call carries the header.
/// </para>
/// </remarks>
/// <param name="context">The circuit's tenant context.</param>
internal sealed class ExplorerActiveTenantProvider(IExplorerTenantContext context) : ILatticeActiveTenantProvider
{
    private readonly IExplorerTenantContext _context = context ?? throw new ArgumentNullException(nameof(context));

    /// <inheritdoc />
    public string? AssertedTenant =>
        _context.ActiveTenant is { } tenant
        && !string.Equals(tenant.Value, ExplorerTenantTrees.DefaultTenantId, StringComparison.Ordinal)
            ? tenant.Value
            : null;
}
