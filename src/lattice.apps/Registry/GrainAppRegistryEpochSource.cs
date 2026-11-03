namespace Orleans.Lattice.Apps;

/// <summary>
/// The <see cref="IAppRegistryEpochSource"/> backed by the single cluster-wide
/// <see cref="IAppRegistryEpochGrain"/> activation. A host with no grain factory has no other silo to
/// stay current with, so it answers a constant local epoch and advancing it is a no-op.
/// </summary>
internal sealed class GrainAppRegistryEpochSource : IAppRegistryEpochSource
{
    private static readonly Task<AppRegistryEpoch> LocalEpoch = Task.FromResult(new AppRegistryEpoch(Guid.Empty, 0));

    private readonly IGrainFactory? _grainFactory;
    private IAppRegistryEpochGrain? _grain;

    /// <summary>Initializes a new <see cref="GrainAppRegistryEpochSource"/>.</summary>
    /// <param name="grainFactory">The silo's grain factory, or null when the host has no cluster.</param>
    public GrainAppRegistryEpochSource(IGrainFactory? grainFactory)
    {
        _grainFactory = grainFactory;
    }

    private IAppRegistryEpochGrain Grain =>
        _grain ??= _grainFactory!.GetGrain<IAppRegistryEpochGrain>(IAppRegistryEpochGrain.GrainKey);

    /// <inheritdoc />
    public Task<AppRegistryEpoch> GetAsync(CancellationToken cancellationToken) =>
        _grainFactory is null ? LocalEpoch : Grain.GetAsync().WaitAsync(cancellationToken);

    /// <inheritdoc />
    public Task<AppRegistryEpoch> AdvanceAsync(CancellationToken cancellationToken) =>
        _grainFactory is null ? LocalEpoch : Grain.AdvanceAsync().WaitAsync(cancellationToken);
}
