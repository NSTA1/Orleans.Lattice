using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The per-silo change-feed subscription runtime. It observes the core change-feed
/// (<see cref="IMutationObserver"/>) and delivers each committed mutation to the app handlers whose
/// activated subscriptions observe the mutated tree.
/// </summary>
/// <remarks>
/// <para>
/// <b>Routing table.</b> Activation work - resolving each enabled install's manifest through
/// <see cref="IAppSource"/>, compiling its subscriptions against the pinned ceiling with
/// <see cref="AppSubscriptionCompiler"/>, and resolving handlers - happens only when the registry
/// snapshot changes, on a coalesced background rebuild. It produces an immutable table from effective
/// tree id to routes. An install whose activation fails (unpinned ceiling, unresolvable manifest, an
/// uncovered cross-app scope, a cross-app target whose owning app is not installed, or a missing
/// handler) contributes no routes at all and its reasons are
/// logged and recorded.
/// </para>
/// <para>
/// <b>Dispatch.</b> The hook runs inline on the committing grain's write path, so for a mutation no
/// subscription observes it costs one snapshot read, one reference comparison and one dictionary
/// probe, and allocates nothing. A snapshot newer than the table schedules a rebuild and, until it
/// lands, each candidate route is re-checked against the current snapshot, so a disabled or
/// uninstalled app stops receiving events immediately rather than when the rebuild completes. A newly
/// enabled app starts receiving once the rebuild lands. Maintenance writes are never delivered.
/// </para>
/// <para>
/// <b>Isolation.</b> A handler that throws or faults is logged and skipped; the remaining handlers
/// still run and the write is never failed.
/// </para>
/// </remarks>
internal sealed class AppSubscriptionRouter : IMutationObserver
{
    private readonly IAppRegistryProjection _projection;
    private readonly IAppSource _source;
    private readonly AppTreeOwnershipLedger _ownership;
    private readonly AppSubscriptionHandlerCatalog _handlers;
    private readonly ILogger<AppSubscriptionRouter> _logger;
    private readonly SemaphoreSlim _rebuildLock = new(1, 1);

    private AppSubscriptionRoutingTable _table = AppSubscriptionRoutingTable.Empty;

    // Coalescing state for background rebuilds: 0 idle, 1 running, 2 running with a queued follow-up.
    private int _rebuildState;
    private Task _backgroundRebuild = Task.CompletedTask;

    /// <summary>Initializes a new <see cref="AppSubscriptionRouter"/>.</summary>
    /// <param name="projection">The warm app-registry projection.</param>
    /// <param name="source">The source enabled apps' manifests are resolved from.</param>
    /// <param name="handlers">The host-registered subscription handlers.</param>
    /// <param name="ownership">The tree ownership ledger naming the installed owners of cross-app subscription targets.</param>
    /// <param name="logger">The logger for activation and handler failures.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public AppSubscriptionRouter(
        IAppRegistryProjection projection,
        IAppSource source,
        AppSubscriptionHandlerCatalog handlers,
        AppTreeOwnershipLedger ownership,
        ILogger<AppSubscriptionRouter> logger)
    {
        ArgumentNullException.ThrowIfNull(projection);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(handlers);
        ArgumentNullException.ThrowIfNull(ownership);
        ArgumentNullException.ThrowIfNull(logger);
        _projection = projection;
        _source = source;
        _handlers = handlers;
        _ownership = ownership;
        _logger = logger;
    }

    /// <summary>The routing table currently in effect.</summary>
    internal AppSubscriptionRoutingTable Table => Volatile.Read(ref _table);

    /// <summary>
    /// The most recently scheduled background rebuild loop, or a completed task when none has been
    /// scheduled. Exposed so a test can await a dispatch-triggered rebuild deterministically.
    /// </summary>
    internal Task BackgroundRebuild => Volatile.Read(ref _backgroundRebuild);

    /// <inheritdoc />
    public Task OnMutationAsync(LatticeMutation mutation, CancellationToken cancellationToken)
    {
        var snapshot = _projection.Current;
        var table = Volatile.Read(ref _table);
        var current = ReferenceEquals(table.Snapshot, snapshot);
        if (!current)
            ScheduleRebuild();

        if (mutation.Category == MutationCategory.Maintenance || !table.TryGetRoutes(mutation.TreeId, out var routes))
            return Task.CompletedTask;

        for (var i = 0; i < routes.Length; i++)
        {
            var route = routes[i];
            if (!route.Matches(in mutation) || (!current && !route.IsStillEnabled(snapshot)))
                continue;

            Task delivery;
            try
            {
                delivery = route.Handler.HandleAsync(route.Context, mutation, cancellationToken);
            }
            catch (Exception ex)
            {
                LogHandlerFailure(ex, route);
                continue;
            }

            if (!delivery.IsCompletedSuccessfully)
                return CompleteDeliveryAsync(delivery, route, routes, i + 1, mutation, snapshot, current, cancellationToken);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Rebuilds the routing table from the current registry snapshot and awaits it. Exposed so a test,
    /// or a caller that has just enabled an app, can make the table current deterministically.
    /// </summary>
    /// <param name="cancellationToken">Cancels the rebuild.</param>
    /// <returns>The table in effect afterwards.</returns>
    internal async Task<AppSubscriptionRoutingTable> RefreshAsync(CancellationToken cancellationToken = default)
    {
        await RebuildOnceAsync(cancellationToken).ConfigureAwait(false);
        return Table;
    }

    // The slow path: a handler returned an incomplete (or faulted) task. Awaiting keeps delivery
    // sequential; the remaining routes resume on the captured context.
    private async Task CompleteDeliveryAsync(
        Task pending,
        AppSubscriptionRoute pendingRoute,
        AppSubscriptionRoute[] routes,
        int next,
        LatticeMutation mutation,
        CompiledAppRegistrySnapshot snapshot,
        bool current,
        CancellationToken cancellationToken)
    {
        try
        {
            await pending;
        }
        catch (Exception ex)
        {
            LogHandlerFailure(ex, pendingRoute);
        }

        for (var i = next; i < routes.Length; i++)
        {
            var route = routes[i];
            if (!route.Matches(in mutation) || (!current && !route.IsStillEnabled(snapshot)))
                continue;
            try
            {
                await route.Handler.HandleAsync(route.Context, mutation, cancellationToken);
            }
            catch (Exception ex)
            {
                LogHandlerFailure(ex, route);
            }
        }
    }

    private void LogHandlerFailure(Exception ex, AppSubscriptionRoute route) =>
        _logger.LogWarning(
            ex,
            "Change-feed handler for subscription '{Subscription}' of app '{App}' (tenant '{Tenant}') failed; the change was not delivered to it.",
            route.Context.Name,
            route.Context.App.Value,
            route.Context.Tenant.Value);

    private void ScheduleRebuild()
    {
        while (true)
        {
            var state = Volatile.Read(ref _rebuildState);
            switch (state)
            {
                case 0:
                    if (Interlocked.CompareExchange(ref _rebuildState, 1, 0) == 0)
                    {
                        // Activation resolves manifests; run it off the mutating grain's scheduler.
                        Volatile.Write(ref _backgroundRebuild, Task.Run(RunRebuildLoopAsync));
                        return;
                    }

                    break;
                case 1:
                    if (Interlocked.CompareExchange(ref _rebuildState, 2, 1) == 1)
                        return;
                    break;
                default:
                    return;
            }
        }
    }

    private async Task RunRebuildLoopAsync()
    {
        while (true)
        {
            try
            {
                await RebuildOnceAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to rebuild the app subscription routing table; the previous table remains in effect.");
            }

            if (Interlocked.CompareExchange(ref _rebuildState, 0, 1) == 1)
                return;
            Volatile.Write(ref _rebuildState, 1);
        }
    }

    private async Task RebuildOnceAsync(CancellationToken cancellationToken)
    {
        await _rebuildLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _projection.EnsureWarmAsync(cancellationToken).ConfigureAwait(false);
            var snapshot = _projection.Current;
            if (ReferenceEquals(Volatile.Read(ref _table).Snapshot, snapshot))
                return;

            var byTree = new Dictionary<string, List<AppSubscriptionRoute>>(StringComparer.Ordinal);
            var failures = new Dictionary<(TenantId Tenant, AppSlug App), IReadOnlyList<string>>();
            var activated = new List<AppSubscriptionRoute>();
            foreach (var record in snapshot.Records)
            {
                if (record.State != AppRegistryLifecycleState.Enabled)
                    continue;

                activated.Clear();
                var reasons = await ActivateAsync(record, activated, cancellationToken).ConfigureAwait(false);
                if (reasons is not null)
                {
                    failures[(record.Tenant, record.Slug)] = reasons;
                    _logger.LogWarning(
                        "Change-feed subscriptions of app '{App}' (tenant '{Tenant}') were not activated: {Reasons}",
                        record.Slug.Value,
                        record.Tenant.Value,
                        string.Join(" ", reasons));
                    continue;
                }

                foreach (var route in activated)
                {
                    if (!byTree.TryGetValue(route.Context.TreeId, out var list))
                        byTree.Add(route.Context.TreeId, list = []);
                    list.Add(route);
                }
            }

            var routes = new Dictionary<string, AppSubscriptionRoute[]>(byTree.Count, StringComparer.Ordinal);
            foreach (var (treeId, list) in byTree)
                routes.Add(treeId, list.ToArray());
            Volatile.Write(ref _table, new AppSubscriptionRoutingTable(snapshot, routes, failures));
        }
        finally
        {
            _rebuildLock.Release();
        }
    }

    private async Task<IReadOnlyList<string>?> ActivateAsync(
        AppRegistryRecord record,
        List<AppSubscriptionRoute> activated,
        CancellationToken cancellationToken)
    {
        if (!record.IsCeilingPinnedToVersion)
            return [$"The install ceiling was consented for version '{record.CeilingVersion}', not the installed '{record.Version}'."];

        try
        {
            var resolution = await _source.ResolveAsync(record.Slug, record.Version, cancellationToken).ConfigureAwait(false);
            if (!resolution.IsResolved || resolution.Manifest is not { } manifest)
                return resolution.Errors.Count == 0
                    ? [$"App '{record.Slug}' version '{record.Version}' could not be resolved ({resolution.Status})."]
                    : resolution.Errors.Select(static e => e.Message).ToArray();

            var owners = await _ownership.ResolveCrossAppOwnersAsync(manifest, record.Tenant, cancellationToken).ConfigureAwait(false);
            var compilation = AppSubscriptionCompiler.Compile(manifest, record.Tenant, record.Ceiling, owners);
            if (!compilation.Succeeded)
                return compilation.Denials.Select(static d => d.Message).ToArray();

            List<string>? missing = null;
            foreach (var context in compilation.Subscriptions)
            {
                if (_handlers.TryResolve(context.App, context.Name, out var handler, out var error))
                    activated.Add(new AppSubscriptionRoute(context, handler!, record.Revision));
                else
                    (missing ??= []).Add(error!);
            }

            return missing;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return [$"Subscription activation of app '{record.Slug}' failed: {ex.Message}"];
        }
    }
}
