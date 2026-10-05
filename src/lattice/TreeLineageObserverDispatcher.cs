namespace Orleans.Lattice;

/// <summary>
/// Fans a pending lineage change out to every registered
/// <see cref="ITreeLineageObserver"/> (issue #4537). Unlike
/// <see cref="TreeAliasObserverDispatcher"/>, it does not swallow failures: the
/// first observer that throws aborts the registry write, because an observer
/// that missed the notification would keep trusting the old lineage. A
/// silo-scoped singleton registered by <c>AddLattice</c>; with no observer
/// registered, <see cref="HasObservers"/> is <see langword="false"/> and the
/// registry skips the extra read it needs to detect a change.
/// </summary>
internal sealed class TreeLineageObserverDispatcher
{
    private readonly ITreeLineageObserver[] _observers;

    /// <summary>Initialises the dispatcher with the DI-provided observers.</summary>
    public TreeLineageObserverDispatcher(IEnumerable<ITreeLineageObserver> observers)
    {
        ArgumentNullException.ThrowIfNull(observers);
        _observers = observers as ITreeLineageObserver[] ?? [.. observers];
    }

    /// <summary><see langword="true"/> when at least one observer is registered.</summary>
    public bool HasObservers => _observers.Length > 0;

    /// <summary>
    /// Notifies every observer, in registration order, that
    /// <paramref name="treeId"/>'s lineage is about to change. Does nothing when
    /// the lineage is unchanged. An observer's exception propagates.
    /// </summary>
    public async Task NotifyChangingAsync(string treeId, Guid? currentLineage, Guid? nextLineage, CancellationToken cancellationToken = default)
    {
        if (_observers.Length == 0 || currentLineage == nextLineage)
        {
            return;
        }

        foreach (var observer in _observers)
        {
            await observer.OnLineageChangingAsync(treeId, currentLineage, nextLineage, cancellationToken);
        }
    }
}
