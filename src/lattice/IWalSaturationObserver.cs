namespace Orleans.Lattice;

/// <summary>
/// Extensibility hook invoked once per per-tree WAL saturation-state
/// transition. Intended for back-pressure consumers - ingest gateways,
/// stream-driven workloads, sidecar circuit breakers - that want to
/// react to a saturation regime before its failure tail surfaces to the
/// caller as a <see cref="TimeoutException"/> from
/// <see cref="ILattice.SetAsync(string, byte[], CancellationToken)"/>
/// or <see cref="ILattice.SetManyAsync(System.Collections.Generic.List{System.Collections.Generic.KeyValuePair{string, byte[]}}, CancellationToken)"/>.
/// <para>
/// Register observers in the silo DI container (for example via
/// <c>services.AddSingleton&lt;IWalSaturationObserver, MyObserver&gt;()</c>).
/// Observers are resolved as <see cref="System.Collections.Generic.IEnumerable{T}"/> so multiple
/// can coexist; exceptions thrown by one observer are logged and do not
/// short-circuit the others. The hook is zero-cost when no observer is
/// registered and the sampler is idle.
/// </para>
/// </summary>
/// <remarks>
/// <para><b>Threading and latency.</b>
/// <see cref="OnStateChangedAsync"/> runs on the silo-scoped sampler
/// thread, not on a grain scheduler, so observers do not add latency
/// to the <see cref="ILattice"/> write hot path. Long-running work is
/// still discouraged - the sampler awaits the fan-out before publishing
/// the next sample, so any blocking observer delays the transition
/// latency of the next state change. The canonical safe pattern is to
/// enqueue a copy of the
/// <see cref="WalSaturationStateChange"/> onto a
/// <c>System.Threading.Channels.Channel&lt;WalSaturationStateChange&gt;</c>
/// and drain it from a background <c>IHostedService</c>.
/// </para>
/// <para><b>Failure semantics.</b> Exceptions thrown by the observer
/// are caught, logged as a warning, and suppressed - the underlying
/// signal computation is not affected and other observers continue
/// running. A silent throw does not stop subsequent transitions from
/// firing.
/// </para>
/// <para><b>Polling alternative.</b> Consumers that prefer a polling
/// shape (for example a TCP read loop that checks "should I read right
/// now?" before each socket read) should use
/// <see cref="IWalSaturationSignal.GetCurrentState(string)"/> directly;
/// it costs one concurrent-dictionary lookup returning an
/// <see cref="WalSaturationState"/> enum with no allocation. The
/// push and poll surfaces are complementary - both read the same
/// per-tree cache populated by the sampler, so a host may register an
/// observer and call <c>GetCurrentState</c> at the same time without
/// drift.
/// </para>
/// </remarks>
public interface IWalSaturationObserver
{
    /// <summary>
    /// Invoked once per per-tree saturation-state transition.
    /// Implementations must treat <paramref name="change"/> as immutable
    /// and should complete quickly; long-running work belongs on a
    /// background queue drained by an <c>IHostedService</c>.
    /// </summary>
    /// <param name="change">Metadata describing the transition.</param>
    /// <param name="cancellationToken">Cancellation signal propagated
    /// from the sampler's lifecycle (typically the host's
    /// <c>StopAsync</c> token). Observers should respect it for any
    /// asynchronous work they start.</param>
    ValueTask OnStateChangedAsync(WalSaturationStateChange change, CancellationToken cancellationToken);
}
