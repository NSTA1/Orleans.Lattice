using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="ISagaCompletionSource"/> backed by the cross-cluster saga
/// coordinator grain. Dials
/// <see cref="ICrossClusterSagaCoordinatorGrain.IsCompleteAsync"/> for the saga
/// id and treats any fault reaching the coordinator as "not yet complete" so
/// the fence primitive keeps shipping paused rather than resuming on an
/// unverified signal.
/// <para>
/// In a multi-cluster deployment the coordinator lives on the initiating
/// cluster; reaching it from a participant cluster is a transport concern owned
/// by the saga control wiring. This default resolves the coordinator grain
/// through the local grain factory, which is exact for single-cluster
/// composition and the primitive's tests; hosts whose coordinator is remote can
/// register an implementation that routes through their control channel.
/// </para>
/// </summary>
internal sealed class CoordinatorSagaCompletionSource(
    IGrainFactory grainFactory,
    ILogger<CoordinatorSagaCompletionSource>? logger = null) : ISagaCompletionSource
{
    private readonly IGrainFactory _grainFactory =
        grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    private readonly ILogger<CoordinatorSagaCompletionSource> _logger =
        logger ?? NullLogger<CoordinatorSagaCompletionSource>.Instance;

    /// <inheritdoc />
    public async Task<bool> IsSagaCompleteAsync(
        string sagaId, string coordinatorClusterId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(sagaId);
        cancellationToken.ThrowIfCancellationRequested();
        _ = coordinatorClusterId;

        try
        {
            var coordinator = _grainFactory.GetGrain<ICrossClusterSagaCoordinatorGrain>(sagaId);
            return await coordinator.IsCompleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Fail safe: an unverified coordinator keeps the shipping pause
            // engaged rather than risking a premature resume.
            _logger.LogWarning(ex,
                "Saga completion probe for '{SagaId}' failed; treating as not-yet-complete.",
                sagaId);
            return false;
        }
    }
}
