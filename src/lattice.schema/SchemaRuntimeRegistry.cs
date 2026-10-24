namespace Orleans.Lattice.Schema;

/// <summary>
/// Resolves a tree's durable catalog on every use, avoiding silo-local registration
/// state and stale caches after restart or scale-out.
/// </summary>
internal sealed class SchemaRuntimeRegistry(
    IGrainFactory grains,
    ILatticeSchemaRegistry hostRegistry,
    ILatticeValueTransformRegistry transforms,
    IEnumerable<ILatticeValueTransform> installedTransforms)
{
    internal bool IsSupported => hostRegistry is LatticeSchemaRegistry;

    internal async Task<LatticeSchemaRuntimeRegistration> GetAsync(string treeId)
    {
        using var origin = LatticeAccessGateContext.EnterSystemOrigin();
        var runtime = await grains.GetGrain<ILatticeSchemaRuntimeRegistryGrain>(treeId).GetAsync().ConfigureAwait(false);
        var host = (hostRegistry as LatticeSchemaRegistry)?.Describe() ?? new();
        return SchemaRuntimeCatalog.Snapshot(SchemaRuntimeCatalog.Merge(host, runtime, transforms, enforceMergedLimit: false) with
        {
            SupportedTransformIds = installedTransforms.Select(t => t.Id).Order(StringComparer.Ordinal).ToArray(),
        });
    }

    internal async Task RegisterAsync(string treeId, LatticeSchemaRuntimeRegistration registration)
    {
        if (!IsSupported)
            throw new InvalidOperationException("Runtime registration requires the standard schema registry; a custom host registry remains host-provisioned.");
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        SchemaConstants.ThrowIfReservedTree(treeId, nameof(treeId));
        using var origin = LatticeAccessGateContext.EnterSystemOrigin();
        await grains.GetGrain<ILatticeSchemaRuntimeRegistryGrain>(treeId).RegisterAsync(registration).ConfigureAwait(false);
    }

    internal async Task<ILatticeSchemaRegistry> ResolveAsync(string treeId) =>
        IsSupported ? SchemaRuntimeCatalog.Build(await GetAsync(treeId).ConfigureAwait(false), transforms) : hostRegistry;

    internal async Task ValidateTargetAsync(string treeId, LatticeSchemaVersionConfig target,
        LatticeSchemaVersionConfig? current = null)
    {
        if (!IsSupported) return;
        var catalog = await GetAsync(treeId).ConfigureAwait(false);
        if (catalog.Descriptors.Length == 0 && catalog.Upcasters.Length == 0)
            return; // Preserve the host's original stamping-only deployment mode.
        var registry = SchemaRuntimeCatalog.Build(catalog, transforms);
        if (!registry.TryGetDescriptor(target.SchemaId, target.TargetVersion, out _))
            throw new InvalidOperationException("Register the target schema descriptor before configuring this version.");
        if (current is { } previous && (previous.SchemaId != target.SchemaId
            || target.TargetVersion < previous.TargetVersion
            || !registry.CanUpcast(target.SchemaId, previous.TargetVersion, target.TargetVersion)))
            throw new InvalidOperationException("The target must advance within the same schema family through a registered migration path.");
    }
}
