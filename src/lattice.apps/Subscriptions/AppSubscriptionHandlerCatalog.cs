namespace Orleans.Lattice.Apps;

/// <summary>
/// The per-silo set of host-registered subscription handlers, keyed by (app, subscription name).
/// Each handler is created lazily, once, the first time a routing table needs it; a pair registered
/// more than once is ambiguous and never resolves, so the app's subscription activation fails
/// rather than silently picking one.
/// </summary>
internal sealed class AppSubscriptionHandlerCatalog
{
    private readonly IServiceProvider _services;
    private readonly Dictionary<(AppSlug App, string Name), Entry> _entries = new();
    private readonly object _gate = new();

    /// <summary>Initializes the catalog from the DI-registered handler registrations.</summary>
    /// <param name="services">The provider handler factories are invoked with.</param>
    /// <param name="registrations">The registrations; <c>null</c> entries are ignored.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="registrations"/> is <c>null</c>.</exception>
    public AppSubscriptionHandlerCatalog(IServiceProvider services, IEnumerable<AppSubscriptionHandlerRegistration> registrations)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(registrations);
        _services = services;
        foreach (var registration in registrations)
        {
            if (registration is null)
                continue;
            var key = (registration.App, registration.SubscriptionName);
            if (_entries.TryGetValue(key, out var existing))
                existing.Duplicated = true;
            else
                _entries.Add(key, new Entry(registration.Factory));
        }
    }

    /// <summary>Resolves the handler for one subscription.</summary>
    /// <param name="app">The declaring app.</param>
    /// <param name="subscriptionName">The subscription name.</param>
    /// <param name="handler">The handler when exactly one is registered.</param>
    /// <param name="error">Why no handler resolved, otherwise <c>null</c>.</param>
    /// <returns><c>true</c> when a handler resolved.</returns>
    public bool TryResolve(AppSlug app, string subscriptionName, out IAppChangeFeedHandler? handler, out string? error)
    {
        handler = null;
        if (!_entries.TryGetValue((app, subscriptionName), out var entry))
        {
            error = $"No change-feed handler is registered for subscription '{subscriptionName}' of app '{app}'.";
            return false;
        }

        if (entry.Duplicated)
        {
            error = $"More than one change-feed handler is registered for subscription '{subscriptionName}' of app '{app}'.";
            return false;
        }

        lock (_gate)
        {
            try
            {
                entry.Handler ??= entry.Factory(_services)
                    ?? throw new InvalidOperationException("The handler factory returned null.");
            }
            catch (Exception ex)
            {
                error = $"The change-feed handler for subscription '{subscriptionName}' of app '{app}' could not be created: {ex.Message}";
                return false;
            }
        }

        handler = entry.Handler;
        error = null;
        return true;
    }

    private sealed class Entry(Func<IServiceProvider, IAppChangeFeedHandler> factory)
    {
        public Func<IServiceProvider, IAppChangeFeedHandler> Factory { get; } = factory;

        public bool Duplicated { get; set; }

        public IAppChangeFeedHandler? Handler { get; set; }
    }
}
