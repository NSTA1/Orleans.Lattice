using Orleans.Lattice;
using VehicleFleetSimulator.AzureThroughput.Engine;

namespace VehicleFleetSimulator.AzureThroughput.Silo;

/// <summary>
/// In-silo implementation of <see cref="IBenchSaturationGate"/>, wiring the
/// shared ingest engine to the WAL-saturation state this silo actually
/// observes.
/// </summary>
/// <remarks>
/// <para>
/// The two halves come from different places and that is deliberate:
/// </para>
/// <list type="bullet">
/// <item><description><see cref="LastSaturatedUtc"/> reads
/// <see cref="BenchSaturationLogger"/>, which records the wall-clock of each
/// observed transition into <c>Saturated</c>. A point-in-time poll of the
/// signal would not do - by the shutdown boundary the tree has usually
/// recovered, so the engine needs the recent <i>history</i>, not the current
/// reading.</description></item>
/// <item><description><see cref="WaitForHealthyAsync"/> forwards straight to
/// <see cref="IWalSaturationSignal"/>, because that genuinely is a
/// current-state question.</description></item>
/// </list>
/// <para>
/// Both are meaningful here only because this process <i>is</i> a silo. The
/// multi-silo rig's producer is an Orleans client with no such in-process
/// observation and installs <see cref="NoOpBenchSaturationGate"/> instead;
/// see <see cref="IBenchSaturationGate"/> for why emulating this
/// client-side would be worse than declining to answer.
/// </para>
/// </remarks>
/// <param name="signal">This silo's live WAL-saturation signal.</param>
/// <param name="logger">Observer recording per-tree saturation transitions.</param>
internal sealed class SiloBenchSaturationGate(
    IWalSaturationSignal signal,
    BenchSaturationLogger logger) : IBenchSaturationGate
{
    /// <inheritdoc />
    public DateTimeOffset? LastSaturatedUtc(string treeId) => logger.LastSaturatedUtc(treeId);

    /// <inheritdoc />
    public Task WaitForHealthyAsync(string treeId, CancellationToken cancellationToken = default)
        => signal.WaitForHealthyAsync(treeId, cancellationToken);
}
