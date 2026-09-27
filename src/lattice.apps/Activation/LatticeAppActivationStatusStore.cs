using Orleans.Serialization;

namespace Orleans.Lattice.Apps;

/// <summary>
/// <see cref="IAppActivationStatusStore"/> over the reserved
/// <see cref="AppActivationTreeNames.StatusTree"/> tree, keyed <c>{tenant}/{slug}</c> like the
/// registry. Every access runs system-origin because the tree is a reserved control-plane tree.
/// </summary>
internal sealed class LatticeAppActivationStatusStore : IAppActivationStatusStore
{
    private readonly IGrainFactory _grainFactory;
    private readonly Serializer<AppActivationStatus> _serializer;

    public LatticeAppActivationStatusStore(IGrainFactory grainFactory, Serializer<AppActivationStatus> serializer)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        _grainFactory = grainFactory;
        _serializer = serializer;
    }

    private ILattice Tree => _grainFactory.GetGrain<ILattice>(AppActivationTreeNames.StatusTree);

    public async Task<AppActivationStatus?> GetAsync(TenantId tenant, AppSlug slug, CancellationToken cancellationToken)
    {
        var key = AppRegistryTreeNames.ComposeKey(tenant, slug);
        byte[]? bytes;
        using (LatticeSystemOrigin.Enter())
        {
            bytes = await Tree.GetAsync(key, cancellationToken).ConfigureAwait(false);
        }

        return bytes is null ? null : _serializer.Deserialize(bytes);
    }

    public async Task SetAsync(AppActivationStatus status, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(status);
        var key = AppRegistryTreeNames.ComposeKey(status.Tenant, status.Slug);
        var bytes = _serializer.SerializeToArray(status);
        using (LatticeSystemOrigin.Enter())
        {
            await Tree.SetAsync(key, bytes, cancellationToken).ConfigureAwait(false);
        }
    }
}
