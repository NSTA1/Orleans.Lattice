using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The Apps area's per-circuit probe of the caller's rights and apps, memoized so
/// the spine, Home, the palette and every Apps page share one set of calls.
/// </summary>
/// <remarks>
/// The probe runs detached from any one caller's token, so a waiter that gives up
/// (the directory's time box) does not poison the memo for the next one.
/// <see cref="Invalidate"/> drops the memo after a lifecycle change or sign-in.
/// </remarks>
/// <param name="facades">The circuit's facades.</param>
/// <param name="logger">Where a failing probe is reported.</param>
/// <param name="time">The clock a lifecycle change is recorded on.</param>
internal sealed class AppsAccess(AppsFacades facades, ILogger<AppsAccess>? logger = null, TimeProvider? time = null)
{
    /// <summary>How long after a lifecycle change an app's pages treat an unready read as settling rather than final.</summary>
    public static readonly TimeSpan SettlingWindow = TimeSpan.FromSeconds(30);

    private readonly TimeProvider _time = time ?? TimeProvider.System;
    private readonly Dictionary<string, DateTimeOffset> _changedAt = new(StringComparer.Ordinal);
    /// <summary>How many catalogue entries completions search, at most.</summary>
    public const int CompletionIndexSize = AvailableAppQuery.MaxPageSize;

    private readonly ILogger _logger = logger ?? NullLogger<AppsAccess>.Instance;
    private readonly object _gate = new();
    private Task<AppsAccessSnapshot>? _snapshot;
    private AppsAccessSnapshot? _last;
    private Task<ImmutableArray<AvailableAppSummary>>? _index;

    /// <summary>Raised after <see cref="Invalidate"/>, so a page can reload what it shows.</summary>
    public event Action? Changed;

    /// <summary>
    /// The most recent completed snapshot - the previous one while a re-probe after
    /// <see cref="Invalidate"/> is still running - or <see langword="null"/> before
    /// the first probe completes.
    /// </summary>
    public AppsAccessSnapshot? Current
    {
        get
        {
            var task = Volatile.Read(ref _snapshot);
            return task is { IsCompletedSuccessfully: true } ? task.Result : Volatile.Read(ref _last);
        }
    }

    /// <summary>The caller's snapshot, probing on first use.</summary>
    /// <param name="cancellationToken">Stops this caller waiting; the probe itself continues.</param>
    public Task<AppsAccessSnapshot> GetAsync(CancellationToken cancellationToken = default)
    {
        Task<AppsAccessSnapshot> task;
        lock (_gate)
        {
            task = _snapshot ??= ProbeAsync();
        }

        return task.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// The first catalogue page across every source, used to complete <c>app:</c>
    /// entries; empty unless the caller may browse the catalogue.
    /// </summary>
    /// <param name="cancellationToken">Stops this caller waiting.</param>
    public async Task<ImmutableArray<AvailableAppSummary>> GetCompletionIndexAsync(CancellationToken cancellationToken = default)
    {
        var snapshot = await GetAsync(cancellationToken).ConfigureAwait(false);
        if (!snapshot.CanBrowseCatalogue || facades.Catalog is not { } catalog)
        {
            return [];
        }

        Task<ImmutableArray<AvailableAppSummary>> task;
        lock (_gate)
        {
            task = _index ??= LoadIndexAsync(catalog);
        }

        return await task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Forgets the memoized probe and starts a fresh one at once, so every reader
    /// after a lifecycle change or sign-in sees the cluster's answer again.
    /// </summary>
    public void Invalidate() => Invalidate(null);

    /// <summary>
    /// Forgets the memoized probe after a lifecycle change to <paramref name="slug"/>,
    /// and records when it changed, so that app's pages re-read it and briefly treat an
    /// unready answer as settling (see <see cref="ChangedRecently"/>).
    /// </summary>
    /// <param name="slug">The app that changed, or <see langword="null"/> for a change to no one app, such as a sign-in.</param>
    public void Invalidate(string? slug)
    {
        lock (_gate)
        {
            _snapshot = ProbeAsync();
            _index = null;
            if (slug is not null)
            {
                _changedAt[slug] = _time.GetUtcNow();
            }
        }

        Changed?.Invoke();
    }

    /// <summary>
    /// Whether <paramref name="slug"/> went through a lifecycle change in this circuit
    /// within <see cref="SettlingWindow"/>. A cluster read right after an install or an
    /// enable can briefly miss it while the change settles.
    /// </summary>
    /// <param name="slug">The app slug.</param>
    public bool ChangedRecently(string slug)
    {
        lock (_gate)
        {
            return _changedAt.TryGetValue(slug, out var at) && _time.GetUtcNow() - at <= SettlingWindow;
        }
    }

    private async Task<AppsAccessSnapshot> ProbeAsync()
    {
        var catalogCapabilities = Task.FromResult(new LatticeAppCatalogCapabilities());
        var controlCapabilities = Task.FromResult(new LatticeAppsCapabilities());
        Task<ImmutableArray<WorkspaceAppSummary>?> myApps = Task.FromResult<ImmutableArray<WorkspaceAppSummary>?>(null);

        if (facades.Catalog is { } catalog)
        {
            catalogCapabilities = GuardAsync(() => catalog.GetCapabilitiesAsync(), new LatticeAppCatalogCapabilities(), "catalogue capabilities");
        }

        if (facades.Control is { } control)
        {
            controlCapabilities = GuardAsync(() => control.GetCapabilitiesAsync(), new LatticeAppsCapabilities(), "app control capabilities");
        }

        if (facades.Workspace is { } workspace)
        {
            myApps = GuardAsync<ImmutableArray<WorkspaceAppSummary>?>(
                async () => await workspace.ListMyAppsAsync().ConfigureAwait(false),
                null,
                "your apps");
        }

        await Task.WhenAll(catalogCapabilities, controlCapabilities, myApps).ConfigureAwait(false);

        var snapshot = new AppsAccessSnapshot
        {
            Catalog = await catalogCapabilities.ConfigureAwait(false),
            Control = await controlCapabilities.ConfigureAwait(false),
            WorkspaceServed = (await myApps.ConfigureAwait(false)).HasValue,
            MyApps = (await myApps.ConfigureAwait(false)) ?? [],
        };

        var installed = Task.FromResult(ImmutableArray<AppSummary>.Empty);
        var updates = Task.FromResult(ImmutableArray<AvailableAppSummary>.Empty);
        if (snapshot.Control.CanList && facades.Control is { } lister)
        {
            installed = GuardAsync(async () => (await lister.ListAsync().ConfigureAwait(false)).Apps, [], "installed apps");
        }

        if (snapshot.CanBrowseCatalogue && facades.Catalog is { } browser)
        {
            updates = GuardAsync(
                async () => (await browser.ListAvailableAsync(new AvailableAppQuery
                {
                    Filter = AvailableAppFilter.Updates,
                    PageSize = AvailableAppQuery.MaxPageSize,
                }).ConfigureAwait(false)).Apps,
                [],
                "app updates");
        }

        var complete = snapshot with
        {
            Installed = await installed.ConfigureAwait(false),
            Updates = await updates.ConfigureAwait(false),
        };
        Volatile.Write(ref _last, complete);
        return complete;
    }

    private Task<ImmutableArray<AvailableAppSummary>> LoadIndexAsync(ILatticeAppCatalog catalog) =>
        GuardAsync(
            async () => (await catalog.ListAvailableAsync(new AvailableAppQuery { PageSize = CompletionIndexSize }).ConfigureAwait(false)).Apps,
            [],
            "the catalogue index");

    private async Task<T> GuardAsync<T>(Func<Task<T>> probe, T denied, string what)
    {
        try
        {
            return await probe().ConfigureAwait(false);
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            _logger.LogInformation(error, "The Apps area could not read {What}; it is treated as denied.", what);
            return denied;
        }
    }
}
