using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Schema;

internal sealed partial class LatticeSchemaControl
{
    public async Task<LatticeSchemaRuntimeRegistration> GetRegistryAsync(
        string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        treeId = await _tenantResolver.ResolveEffectiveTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeReadAsync(treeId, cancellationToken).ConfigureAwait(false);
        return await RequireRuntimeRegistry().GetAsync(treeId).ConfigureAwait(false);
    }

    public async Task RegisterSchemaAsync(
        string treeId, LatticeSchemaRuntimeRegistration registration, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(registration);
        treeId = await _tenantResolver.ResolveEffectiveTreeIdAsync(treeId, cancellationToken).ConfigureAwait(false);
        await _authorizer.AuthorizeManageAsync(treeId, cancellationToken).ConfigureAwait(false);
        await RequireRuntimeRegistry().RegisterAsync(treeId, registration).ConfigureAwait(false);
    }

    private SchemaRuntimeRegistry RequireRuntimeRegistry() => _runtimeRegistry is { IsSupported: true } registry ? registry
        : throw new InvalidOperationException(
            "Runtime schema registration requires AddLatticeSchemaVersioning with the standard registry on every serving host.");
}
