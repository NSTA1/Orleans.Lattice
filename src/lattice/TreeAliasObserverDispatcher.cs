using Microsoft.Extensions.Logging;

namespace Orleans.Lattice;

/// <summary>
/// Internal fan-out helper that invokes every registered
/// <see cref="ITreeAliasObserver"/> for each effective physical-identity
/// change of a logical tree. Observer exceptions are caught, logged, and
/// suppressed so a faulty observer cannot short-circuit the remaining
/// observers or the registry's alias-mutation path. The dispatcher is a
/// silo-scoped singleton registered by <c>AddLattice</c>; when no observer
/// is registered, <see cref="HasObservers"/> is <c>false</c> and the
/// registry short-circuits the dispatch call entirely, so a deployment
/// without the replication package pays nothing.
/// <para>
/// Mirrors the shape of <see cref="MutationObserverDispatcher"/> but for the
/// tree-alias control-plane surface; the two dispatchers are independent (an
/// observer registered against one does not see events from the other).
/// </para>
/// </summary>
internal sealed class TreeAliasObserverDispatcher
{
    private readonly ITreeAliasObserver[] _observers;
    private readonly ILogger<TreeAliasObserverDispatcher> _logger;

    /// <summary>
    /// Initialises the dispatcher with the DI-provided observers. The
    /// enumerable is materialised once at construction - observers are
    /// expected to be singletons registered at silo start.
    /// </summary>
    public TreeAliasObserverDispatcher(
        IEnumerable<ITreeAliasObserver> observers,
        ILogger<TreeAliasObserverDispatcher> logger)
    {
        ArgumentNullException.ThrowIfNull(observers);
        ArgumentNullException.ThrowIfNull(logger);

        _observers = observers as ITreeAliasObserver[] ?? [.. observers];
        _logger = logger;
    }

    /// <summary>
    /// <c>true</c> when at least one <see cref="ITreeAliasObserver"/> is
    /// registered. The registry checks this before building a
    /// <see cref="TreeAliasChange"/> so the allocation is elided when no
    /// observer is installed.
    /// </summary>
    public bool HasObservers => _observers.Length > 0;

    /// <summary>
    /// Invokes every registered observer with the supplied change. Each
    /// observer is awaited in registration order; exceptions thrown by one
    /// observer are logged and do not short-circuit subsequent observers.
    /// Returns a completed task synchronously when no observer is registered.
    /// </summary>
    public async Task PublishAsync(TreeAliasChange change, CancellationToken cancellationToken = default)
    {
        if (_observers.Length == 0) return;

        for (var i = 0; i < _observers.Length; i++)
        {
            var observer = _observers[i];
            try
            {
                await observer.OnTreeAliasChangedAsync(change, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "ITreeAliasObserver {ObserverType} threw for tree {TreeId} ({OldPhysical} -> {NewPhysical}); continuing.",
                    observer.GetType().FullName, change.TreeId, change.OldPhysicalTreeId, change.NewPhysicalTreeId);
            }
        }
    }
}
