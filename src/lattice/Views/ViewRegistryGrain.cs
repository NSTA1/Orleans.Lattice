using Orleans.Runtime;

namespace Orleans.Lattice.Views;

/// <summary>
/// Default <see cref="IViewRegistryGrain"/>. A single cluster-wide activation
/// keyed by <see cref="IViewRegistryGrain.SingletonKey"/> persists the set of
/// runtime-created views to durable grain state so they can be re-hydrated after
/// a silo restart. See <see cref="IViewRegistryGrain"/> for the contract.
/// </summary>
internal sealed class ViewRegistryGrain(
    IGrainContext context,
    [PersistentState("view-registry", LatticeOptions.StorageProviderName)]
    IPersistentState<ViewRegistryState> state) : IGrainBase, IViewRegistryGrain
{
    /// <inheritdoc />
    IGrainContext IGrainBase.GrainContext => context;

    /// <inheritdoc />
    public async Task RegisterAsync(RuntimeViewRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);

        if (state.State.Registrations.TryGetValue(registration.ViewName, out var existing)
            && existing == registration)
        {
            return;
        }

        state.State.Registrations[registration.ViewName] = registration;
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public async Task UnregisterAsync(string viewName)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);

        if (state.State.Registrations.Remove(viewName))
        {
            await state.WriteStateAsync();
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<RuntimeViewRegistration>> ListAsync() =>
        Task.FromResult<IReadOnlyList<RuntimeViewRegistration>>(
            state.State.Registrations.Values.ToArray());
}
