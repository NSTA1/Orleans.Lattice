using Orleans.Runtime;

namespace Orleans.Lattice.Schema;

internal sealed class LatticeSchemaRuntimeRegistryGrain(
    IGrainContext context,
    [PersistentState("schema-registry", LatticeOptions.StorageProviderName)]
    IPersistentState<SchemaRuntimeRegistryState> state,
    ILatticeSchemaRegistry hostRegistry,
    ILatticeValueTransformRegistry transforms) : IGrainBase, ILatticeSchemaRuntimeRegistryGrain
{
    IGrainContext IGrainBase.GrainContext => context;

    private void EnsureOrigin() => LatticeInternalOriginContext.EnsureInternalGrainOrigin(
        context.ActivationServices, context.GrainId.Key.ToString()!, LatticeOperation.SchemaAdmin);

    public Task<LatticeSchemaRuntimeRegistration> GetAsync()
    {
        EnsureOrigin();
        return Task.FromResult(SchemaRuntimeCatalog.Snapshot(state.State.Registration));
    }

    public async Task RegisterAsync(LatticeSchemaRuntimeRegistration registration)
    {
        EnsureOrigin();
        var host = (hostRegistry as LatticeSchemaRegistry)?.Describe() ?? new();
        var effective = SchemaRuntimeCatalog.Merge(host, state.State.Registration, transforms, enforceMergedLimit: false);
        var merged = SchemaRuntimeCatalog.Merge(effective, registration, transforms, enforceMergedLimit: false);
        var hostDescriptors = host.Descriptors.Select(d => (d.SchemaId, d.Version)).ToHashSet();
        var hostUpcasters = host.Upcasters.Select(h => (h.SchemaId, h.FromVersion)).ToHashSet();
        var runtime = new LatticeSchemaRuntimeRegistration
        {
            Descriptors = merged.Descriptors.Where(d => !hostDescriptors.Contains((d.SchemaId, d.Version))).ToArray(),
            Upcasters = merged.Upcasters.Where(h => !hostUpcasters.Contains((h.SchemaId, h.FromVersion))).ToArray(),
        };
        SchemaRuntimeCatalog.ValidateSize(runtime);
        var previous = state.State.Registration;
        state.State.Registration = SchemaRuntimeCatalog.Snapshot(runtime);
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Registration = previous;
            throw;
        }
    }
}
