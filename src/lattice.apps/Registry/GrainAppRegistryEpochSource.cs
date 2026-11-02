namespace Orleans.Lattice.Apps;

/// <summary>
/// The <see cref="IAppRegistryEpochSource"/> backed by the single cluster-wide
/// <see cref="IAppRegistryEpochGrain"/> activation.
/// </summary>
internal sealed class GrainAppRegistryEpochSource : IAppRegistryEpochSource
{
    private readonly IGrainFactory _grainFactory;
    private IAppRegistryEpochGrain? _grain;

    /// <summary>Initializes a new <see cref="GrainAppRegistryEpochSource"/>.</summary>
    /// <param name="grainFactory">The silo's grain factory.</param>
    /// <exception cref="ArgumentNullException"><paramref name="grainFactory"/> is null.</exception>
    public GrainAppRegistryEpochSource(IGrainFactory grainFactory)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        _grainFactory = grainFactory;
    }

    private IAppRegistryEpochGrain Grain =>
        _grain ??= _grainFactory.GetGrain<IAppRegistryEpochGrain>(IAppRegistryEpochGrain.GrainKey);

    /// <inheritdoc />
    public Task<AppRegistryEpoch> GetAsync(CancellationToken cancellationToken) =>
        Grain.GetAsync().WaitAsync(cancellationToken);

    /// <inheritdoc />
    public Task<AppRegistryEpoch> AdvanceAsync(CancellationToken cancellationToken) =>
        Grain.AdvanceAsync().WaitAsync(cancellationToken);
}
