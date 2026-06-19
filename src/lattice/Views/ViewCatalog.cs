using System.Collections.Concurrent;

namespace Orleans.Lattice.Views;

/// <summary>
/// Thread-safe in-memory <see cref="IViewCatalog"/>. Backed by a
/// <see cref="ConcurrentDictionary{TKey,TValue}"/> keyed by view name, so
/// startup registration and runtime <see cref="ILatticeViewFactory.Create"/>
/// calls can register concurrently with maintainer-grain reads.
/// </summary>
internal sealed class ViewCatalog : IViewCatalog
{
    private readonly ConcurrentDictionary<string, ViewRegistration> _views = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public void Register(ViewRegistration registration)
    {
        ArgumentNullException.ThrowIfNull(registration);
        _views[registration.ViewName] = registration;
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
        _views.TryRemove(viewName, out _);
    }

    /// <inheritdoc />
    public IReadOnlyCollection<ViewRegistration> All() => _views.Values.ToArray();
}
