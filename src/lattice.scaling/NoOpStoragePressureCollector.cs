namespace Orleans.Lattice.Scaling;

/// <summary>
/// Default <see cref="IStoragePressureCollector"/> used until the storage-axis
/// change (#1187) provides a real one. Reports a zero, not-over-threshold
/// <see cref="StoragePressure"/> (an empty account list and no rebalance
/// recommendation), so the storage axis contributes nothing to the combined
/// <see cref="ScalingSignal"/>. Registered via
/// <see cref="Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.TryAddSingleton{TService, TImplementation}(Microsoft.Extensions.DependencyInjection.IServiceCollection)"/>
/// so #1187 can substitute its own implementation without editing this file.
/// </summary>
internal sealed class NoOpStoragePressureCollector : IStoragePressureCollector
{
    /// <inheritdoc />
    public ValueTask<StoragePressure> CollectAsync(CancellationToken cancellationToken)
        => ValueTask.FromResult(default(StoragePressure));
}
