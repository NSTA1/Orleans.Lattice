using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Production <see cref="IComputePressureCollector"/>: assembles the
/// cluster-aggregate <see cref="ComputePressure"/> from the cluster runtime
/// statistics (activation and host-resource dimensions) and the local WAL
/// saturation signal (dispatch dimension).
/// <para>
/// Cluster aggregation takes the worst-case (max) silo per dimension so a single
/// bottlenecked silo drives the scale-out decision. The WAL dispatch dimension
/// reads <see cref="IWalSaturationSignal.GetAggregateState"/>, the answering
/// silo's tree-aggregate saturation, mapped to its compute-bound contribution.
/// When no WAL signal is registered (the package added without core
/// <c>AddLattice</c>) the dispatch dimension is treated as healthy.
/// </para>
/// </summary>
internal sealed class ComputePressureCollector(
    IClusterRuntimeStatisticsSource runtimeSource,
    IOptions<LatticeScalingSignalOptions> options,
    IWalSaturationSignal? walSaturationSignal = null) : IComputePressureCollector
{
    private readonly IClusterRuntimeStatisticsSource _runtimeSource = runtimeSource;
    private readonly IOptions<LatticeScalingSignalOptions> _options = options;
    private readonly IWalSaturationSignal? _walSaturationSignal = walSaturationSignal;

    /// <inheritdoc />
    public async ValueTask<ComputePressure> CollectAsync(CancellationToken cancellationToken)
    {
        var snapshot = await _runtimeSource.SampleAsync(cancellationToken).ConfigureAwait(false);
        var target = _options.Value.ActivationWorkingSetTarget;

        var activation = 0d;
        var resource = 0d;
        var silos = snapshot.Silos;
        for (var i = 0; i < silos.Count; i++)
        {
            var sample = silos[i];
            var siloActivation = ComputePressureMath.NormaliseActivation(sample.ActivationCount, target);
            if (siloActivation > activation)
            {
                activation = siloActivation;
            }

            var siloResource = ComputePressureMath.NormaliseResource(sample);
            if (siloResource > resource)
            {
                resource = siloResource;
            }
        }

        var walState = _walSaturationSignal?.GetAggregateState() ?? WalSaturationState.Healthy;

        return new ComputePressure
        {
            Activation = activation,
            Resource = resource,
            WalDispatch = ComputePressureMath.MapWalDispatch(walState),
            WalSaturation = walState,
        };
    }
}
