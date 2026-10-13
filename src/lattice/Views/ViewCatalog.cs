using System.Collections.Concurrent;

namespace Orleans.Lattice.Views;

/// <summary>
/// Thread-safe in-memory <see cref="IViewCatalog"/>. Backed by a
/// <see cref="ConcurrentDictionary{TKey,TValue}"/> keyed by view name, so
/// startup registration and runtime
/// <see cref="ILatticeViewFactory.CreateAsync(ILattice,string,LatticeViewDefinition,CancellationToken)"/>
/// calls can register concurrently with maintainer-grain reads.
/// </summary>
/// <remarks>
/// Reads are lock-free. Every write takes <see cref="_gate"/>, so a
/// <see cref="TryReplace"/> compare-and-swap can compare by reference: the
/// dictionary's own compare-and-swap overloads compare values with
/// <see cref="ViewRegistration"/>'s structural record equality, under which a
/// newer registration equal by value to a stale one would be clobbered.
/// </remarks>
internal sealed class ViewCatalog : IViewCatalog
{
    private readonly Lock _gate = new();
    private readonly ConcurrentDictionary<string, ViewRegistration> _views = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<ViewRegistration, ViewCatalogDurability> _durability = new(ReferenceEqualityComparer.Instance);

    /// <inheritdoc />
    public void Register(ViewRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (_gate)
        {
            if (_views.TryGetValue(registration.ViewName, out var previous) && !ReferenceEquals(previous, registration))
            {
                _durability.TryRemove(previous, out _);
            }

            _views[registration.ViewName] = registration;
        }
    }

    /// <inheritdoc />
    public ViewRegistration? TryGet(string viewName)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        return _views.TryGetValue(viewName, out var registration) ? registration : null;
    }

    /// <inheritdoc />
    public void Remove(string viewName)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        lock (_gate)
        {
            if (_views.TryRemove(viewName, out var removed))
            {
                _durability.TryRemove(removed, out _);
            }
        }
    }

    /// <inheritdoc />
    public bool TryReplace(ViewRegistration expected, ViewRegistration? replacement)
    {
        ArgumentNullException.ThrowIfNull(expected);
        lock (_gate)
        {
            if (!_views.TryGetValue(expected.ViewName, out var current) || !ReferenceEquals(current, expected))
            {
                return false;
            }

            _durability.TryRemove(expected, out _);
            if (replacement is null)
            {
                _views.TryRemove(expected.ViewName, out _);
            }
            else
            {
                _views[expected.ViewName] = replacement;
            }

            return true;
        }
    }

    /// <inheritdoc />
    public void MarkDurablyPending(ViewRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (_gate)
        {
            _durability[registration] = ViewCatalogDurability.PersistInFlight;
        }
    }

    /// <inheritdoc />
    public void ClearDurablyPending(ViewRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (_gate)
        {
            _durability.TryRemove(registration, out _);
        }
    }

    /// <inheritdoc />
    public void MarkDurablePersistFailed(ViewRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        lock (_gate)
        {
            // Only an entry still in flight and still bound moves to Failed: a
            // registration already replaced or removed must not be re-marked, or
            // its mark would outlive the entry.
            if (_durability.ContainsKey(registration)
                && _views.TryGetValue(registration.ViewName, out var current)
                && ReferenceEquals(current, registration))
            {
                _durability[registration] = ViewCatalogDurability.PersistFailed;
            }
        }
    }

    /// <inheritdoc />
    public ViewCatalogDurability GetDurability(ViewRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        return _durability.TryGetValue(registration, out var durability) ? durability : ViewCatalogDurability.Settled;
    }

    /// <inheritdoc />
    public IReadOnlyCollection<ViewRegistration> All() =>
        // ConcurrentDictionary.Values already returns a moment-in-time snapshot
        // (a ReadOnlyCollection over a freshly built List), which is exactly the
        // stable, immutable enumeration this contract promises. Every caller only
        // enumerates the result, so the previous trailing .ToArray() copied that
        // snapshot into a second array for nothing; returning the snapshot
        // directly preserves the semantics and removes one array allocation +
        // copy per call (the warm caller is ComputeViewWaitSet on the cross-tree
        // drain path).
        (IReadOnlyCollection<ViewRegistration>)_views.Values;
}