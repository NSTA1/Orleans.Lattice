using Microsoft.Extensions.Logging;

namespace Orleans.Lattice;

/// <summary>
/// Internal fan-out helper that invokes every registered
/// <see cref="IMutationObserver"/> for each durably-committed mutation.
/// Observer exceptions are caught, logged, and suppressed so a faulty
/// observer cannot short-circuit the remaining observers or the write
/// path. The dispatcher is a silo-scoped singleton registered by
/// <c>AddLattice</c>; when no observers are registered,
/// <see cref="HasObservers"/> is <c>false</c> and callers short-circuit
/// the dispatch call entirely.
/// </summary>
internal sealed class MutationObserverDispatcher
{
    private readonly IMutationObserver[] _observers;
    private readonly ILogger<MutationObserverDispatcher> _logger;

    /// <summary>
    /// Initialises the dispatcher with the DI-provided observers.
    /// The enumerable is materialised once at construction — observers are
    /// expected to be singletons registered at silo start.
    /// </summary>
    public MutationObserverDispatcher(
        IEnumerable<IMutationObserver> observers,
        ILogger<MutationObserverDispatcher> logger)
    {
        ArgumentNullException.ThrowIfNull(observers);
        ArgumentNullException.ThrowIfNull(logger);

        _observers = observers as IMutationObserver[] ?? [.. observers];
        _logger = logger;
    }

    /// <summary>
    /// <c>true</c> when at least one <see cref="IMutationObserver"/> is
    /// registered. Hot paths check this before building a
    /// <see cref="LatticeMutation"/> so the allocation is elided when no
    /// observer is installed.
    /// </summary>
    public bool HasObservers => _observers.Length > 0;

    /// <summary>
    /// Invokes every registered observer with the supplied mutation.
    /// Each observer is awaited in registration order; exceptions thrown
    /// by one observer are logged and do not short-circuit subsequent
    /// observers. Returns a completed task synchronously when no observer
    /// is registered.
    /// </summary>
    public async Task PublishAsync(LatticeMutation mutation, CancellationToken cancellationToken = default)
    {
        if (_observers.Length == 0) return;

        for (var i = 0; i < _observers.Length; i++)
        {
            var observer = _observers[i];
            try
            {
                await observer.OnMutationAsync(mutation, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "IMutationObserver {ObserverType} threw for tree {TreeId} key {Key} ({Kind}); continuing.",
                    observer.GetType().FullName, mutation.TreeId, mutation.Key, mutation.Kind);
            }
        }
    }
}
