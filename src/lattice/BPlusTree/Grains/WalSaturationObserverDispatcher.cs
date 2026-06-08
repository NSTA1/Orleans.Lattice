using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Internal fan-out helper that invokes every registered
/// <see cref="IWalSaturationObserver"/> for each per-tree saturation
/// transition observed by the silo-scoped sampler. Observer exceptions
/// are caught, logged, and suppressed so a faulty observer cannot
/// short-circuit the remaining observers or the sampler's next tick.
/// The dispatcher is a silo-scoped singleton registered by
/// <c>AddLattice</c>; when no observers are registered,
/// <see cref="HasObservers"/> is <c>false</c> and the sampler
/// short-circuits the dispatch call entirely.
/// <para>
/// Mirrors the shape of <see cref="MutationObserverDispatcher"/> but
/// for the saturation back-pressure surface; the two dispatchers are
/// independent (an observer registered against one does not see events
/// from the other).
/// </para>
/// </summary>
internal sealed class WalSaturationObserverDispatcher
{
    private readonly IWalSaturationObserver[] _observers;
    private readonly ILogger<WalSaturationObserverDispatcher> _logger;

    /// <summary>
    /// Initialises the dispatcher with the DI-provided observers. The
    /// enumerable is materialised once at construction - observers are
    /// expected to be singletons registered at silo start.
    /// </summary>
    public WalSaturationObserverDispatcher(
        IEnumerable<IWalSaturationObserver> observers,
        ILogger<WalSaturationObserverDispatcher> logger)
    {
        ArgumentNullException.ThrowIfNull(observers);
        ArgumentNullException.ThrowIfNull(logger);

        _observers = observers as IWalSaturationObserver[] ?? [.. observers];
        _logger = logger;
    }

    /// <summary>
    /// <c>true</c> when at least one <see cref="IWalSaturationObserver"/>
    /// is registered. Hot paths check this before building a
    /// <see cref="WalSaturationStateChange"/> so the per-transition
    /// allocation is elided when no observer is installed.
    /// </summary>
    public bool HasObservers => _observers.Length > 0;

    /// <summary>
    /// Invokes every registered observer with the supplied transition.
    /// Each observer is awaited in registration order; exceptions thrown
    /// by one observer are logged and do not short-circuit subsequent
    /// observers. Returns a completed task synchronously when no
    /// observer is registered.
    /// </summary>
    public async ValueTask PublishAsync(WalSaturationStateChange change, CancellationToken cancellationToken = default)
    {
        if (_observers.Length == 0) return;

        for (var i = 0; i < _observers.Length; i++)
        {
            var observer = _observers[i];
            try
            {
                await observer.OnStateChangedAsync(change, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "IWalSaturationObserver {ObserverType} threw for tree {TreeId} ({Previous} -> {New}); continuing.",
                    observer.GetType().FullName, change.TreeId, change.PreviousState, change.NewState);
            }
        }
    }
}
