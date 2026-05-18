using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="ILatticeBootstrapCoordinator"/> implementation.
/// Resolves the per-tree
/// <see cref="ILatticeBootstrapCoordinatorGrain"/> activation by tree
/// name and forwards every call to it. The grain holds the actual
/// state machine and in-progress gate; the cluster-wide single
/// activation per tree id is what makes the bootstrap mutually
/// exclusive across silos.
/// </summary>
internal sealed class LatticeBootstrapCoordinator(IGrainFactory grainFactory) : ILatticeBootstrapCoordinator
{
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public Task<LatticeBootstrapState> GetStateAsync(string treeName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        cancellationToken.ThrowIfCancellationRequested();
        return _grainFactory
            .GetGrain<ILatticeBootstrapCoordinatorGrain>(treeName)
            .GetStateAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task<BootstrapCoordinatorStatus> GetStatusAsync(string treeName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        cancellationToken.ThrowIfCancellationRequested();
        return _grainFactory
            .GetGrain<ILatticeBootstrapCoordinatorGrain>(treeName)
            .GetStatusAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task BootstrapAsync(
        string treeName,
        string sourceClusterId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        cancellationToken.ThrowIfCancellationRequested();
        return _grainFactory
            .GetGrain<ILatticeBootstrapCoordinatorGrain>(treeName)
            .BootstrapAsync(sourceClusterId, cancellationToken);
    }
}
