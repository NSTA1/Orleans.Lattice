using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-silo record of which grain types have been proven unplaceable
/// cluster-wide while this silo is stopping, so callers stop sending calls that
/// can only fail (issue #5100).
/// <para>
/// When a whole cluster stops, every resident leaf keeps probing its
/// <see cref="ILeafReplayCoordinatorGrain"/> and flushing its
/// <see cref="IWalMaterialiserPinGrain"/> pins after the last compatible silo
/// has withdrawn those types. Each call is refused by placement and logged by
/// the Orleans messaging layer at Error level with a full stack trace, before
/// any Lattice code sees the fault, so the only lever is not to send it. One
/// 6-silo stop logged 124,000 such errors in 30 seconds.
/// </para>
/// <para>
/// <b>When it latches.</b> Only while the host is stopping
/// (<see cref="IHostApplicationLifetime.ApplicationStopping"/>), and only on the
/// catalog-withdrawal fault shape ("Known nodes with grain type: none"). That
/// shape is the cluster stating no
/// active silo hosts the type, so no live activation exists anywhere that a
/// skipped call could have reached. The narrower local-refusal shape ("Unable
/// to create local activation") deliberately does not latch: on a rolling stop
/// another silo may still host the type, and the next call may succeed there.
/// </para>
/// <para>
/// <b>It cannot outlive the stop.</b> A latched type is reported withdrawn only
/// while the host is still stopping, and a stopping host never resumes, so a
/// latch can never suppress calls on a healthy silo. A host with no
/// <see cref="IHostApplicationLifetime"/> never reports stopping and never
/// latches.
/// </para>
/// </summary>
internal sealed class SiloStopRoutingLatch
{
    private readonly Func<bool> _isStopping;
    private readonly ILogger? _logger;
    private int _withdrawn;

    /// <summary>
    /// DI constructor. The lifetime is resolved lazily, at the first stop check,
    /// so constructing the latch early in silo start-up cannot perturb the host.
    /// </summary>
    /// <param name="services">The silo service provider.</param>
    public SiloStopRoutingLatch(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        IHostApplicationLifetime? lifetime = null;
        var resolved = false;
        _isStopping = () =>
        {
            if (!resolved)
            {
                lifetime = services.GetService<IHostApplicationLifetime>();
                resolved = true;
            }

            return lifetime?.ApplicationStopping.IsCancellationRequested == true;
        };
        _logger = services.GetService<ILogger<SiloStopRoutingLatch>>();
    }

    /// <summary>Test constructor with an explicit stop signal.</summary>
    /// <param name="isStopping">Reports whether the host is stopping.</param>
    /// <param name="logger">Optional logger for the once-per-type latch record.</param>
    internal SiloStopRoutingLatch(Func<bool> isStopping, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(isStopping);
        _isStopping = isStopping;
        _logger = logger;
    }

    /// <summary>Whether the host has begun stopping.</summary>
    public bool IsStopping => _isStopping();

    /// <summary>
    /// Whether calls to <paramref name="grainType"/> should not be sent: the host
    /// is stopping and the type has been observed withdrawn cluster-wide.
    /// </summary>
    /// <param name="grainType">The grain type to check.</param>
    /// <returns><see langword="true"/> when a call can only fail.</returns>
    public bool IsWithdrawn(StopRoutedGrainType grainType)
        => (Volatile.Read(ref _withdrawn) & (int)grainType) != 0 && IsStopping;

    /// <summary>
    /// Records <paramref name="fault"/> from a call to <paramref name="grainType"/>,
    /// latching the type withdrawn when the host is stopping and the fault is the
    /// catalog-withdrawal shape. Never throws.
    /// </summary>
    /// <param name="grainType">The grain type the faulted call targeted.</param>
    /// <param name="fault">The fault the call raised.</param>
    /// <returns><see langword="true"/> when this call newly latched the type.</returns>
    public bool Observe(StopRoutedGrainType grainType, Exception? fault)
    {
        if (fault is null || grainType == StopRoutedGrainType.None)
            return false;

        try
        {
            if ((Volatile.Read(ref _withdrawn) & (int)grainType) != 0)
                return false;

            if (!IsCatalogWithdrawn(fault) || !IsStopping)
                return false;

            var bit = (int)grainType;
            var prior = Interlocked.Or(ref _withdrawn, bit);
            if ((prior & bit) != 0)
                return false;

            _logger?.LogInformation(
                "Grain type {GrainType} is no longer hosted by any active silo and this silo is stopping; further "
                + "calls to it from this silo are skipped until the silo exits (issue #5100). The fault that "
                + "established it: {Fault}",
                grainType,
                fault.Message);
            return true;
        }
        catch
        {
            // An observation must never replace the fault the caller is handling.
            return false;
        }
    }

    /// <summary>
    /// Whether <paramref name="exception"/>, or any exception it wraps, is the
    /// placement fault Orleans raises when no active silo in the cluster hosts
    /// the target grain type. Matched on the message fragment rather than a
    /// runtime type, as the pin path's routing predicate already is, so it is
    /// testable with a plain exception.
    /// <para>
    /// Only "Known nodes with grain type: none" qualifies. The leading "No active
    /// nodes are compatible with grain" also appears when silos DO host the type
    /// but at an incompatible interface version, as on a rolling upgrade; that
    /// fault names the hosting silos and must not latch.
    /// </para>
    /// </summary>
    /// <param name="exception">The exception to classify.</param>
    /// <returns><see langword="true"/> for the catalog-withdrawal shape.</returns>
    internal static bool IsCatalogWithdrawn(Exception exception)
    {
        for (var ex = exception; ex is not null; ex = ex.InnerException)
        {
            if (ex.Message is { } message &&
                message.Contains("Known nodes with grain type: none", StringComparison.Ordinal))
            {
                return true;
            }

            if (ex is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    if (IsCatalogWithdrawn(inner))
                        return true;
                }
            }
        }

        return false;
    }
}
