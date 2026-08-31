using System.Diagnostics;
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
/// <para>
/// Each observer's inline callback is timed onto
/// <see cref="LatticeMetrics.ObserverDuration"/> so an operator can see how
/// much write latency a specific observer contributes. The measurement is
/// taken on the faulting path too - an observer that throws slowly is
/// exactly the misbehaviour the instrument exists to surface - and spans
/// only the callback itself: the warning this class logs for a faulting
/// observer is emitted after the measurement is recorded, so a slow log
/// sink is never billed to the observer.
/// </para>
/// </summary>
internal sealed class MutationObserverDispatcher
{
    private readonly IMutationObserver[] _observers;

    /// <summary>
    /// The frozen <see cref="LatticeMetrics.TagObserver"/> tag for each entry
    /// of <see cref="_observers"/>, positionally aligned. Built once at
    /// construction because the observer set is fixed for the silo's lifetime,
    /// so the timing path never materialises a type name per publish.
    /// </summary>
    private readonly KeyValuePair<string, object?>[] _observerTags;

    private readonly ILogger<MutationObserverDispatcher> _logger;

    /// <summary>
    /// Initialises the dispatcher with the DI-provided observers.
    /// The enumerable is materialised once at construction - observers are
    /// expected to be singletons registered at silo start.
    /// </summary>
    public MutationObserverDispatcher(
        IEnumerable<IMutationObserver> observers,
        ILogger<MutationObserverDispatcher> logger)
    {
        ArgumentNullException.ThrowIfNull(observers);
        ArgumentNullException.ThrowIfNull(logger);

        _observers = observers as IMutationObserver[] ?? [.. observers];
        _observerTags = BuildObserverTags(_observers);
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

        // Read once for the whole fan-out so the start-capture and the record
        // decision can never disagree: a listener attaching mid-loop would
        // otherwise record a measurement against a timestamp never taken.
        // When nothing is listening this is the only cost the instrument adds.
        var timed = LatticeMetrics.ObserverDuration.Enabled;

        for (var i = 0; i < _observers.Length; i++)
        {
            var observer = _observers[i];
            var startTimestamp = timed ? Stopwatch.GetTimestamp() : 0L;
            Exception? failure = null;
            try
            {
                await observer.OnMutationAsync(mutation, cancellationToken);
            }
            catch (Exception ex)
            {
                // Captured rather than logged here so the dispatcher's own
                // logging cost stays outside the measured window below - the
                // instrument attributes the observer's latency, not ours.
                failure = ex;
            }
            finally
            {
                if (timed)
                {
                    // Three tags hit the non-allocating Record overload; the
                    // observer tag is pre-built and the tenant tag is a frozen
                    // singleton for every non-tenant-scoped tree.
                    LatticeMetrics.ObserverDuration.Record(
                        Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds,
                        _observerTags[i],
                        new KeyValuePair<string, object?>(LatticeMetrics.TagTree, mutation.TreeId),
                        LatticeTenantLabel.ForTree(mutation.TreeId));
                }
            }

            if (failure is not null)
            {
                _logger.LogWarning(failure,
                    "IMutationObserver {ObserverType} threw for tree {TreeId} key {Key} ({Kind}); continuing.",
                    observer.GetType().FullName, mutation.TreeId, mutation.Key, mutation.Kind);
            }
        }
    }

    private static KeyValuePair<string, object?>[] BuildObserverTags(IMutationObserver[] observers)
    {
        if (observers.Length == 0)
        {
            return [];
        }

        var tags = new KeyValuePair<string, object?>[observers.Length];
        for (var i = 0; i < observers.Length; i++)
        {
            var type = observers[i].GetType();
            tags[i] = new KeyValuePair<string, object?>(LatticeMetrics.TagObserver, type.FullName ?? type.Name);
        }

        return tags;
    }
}
