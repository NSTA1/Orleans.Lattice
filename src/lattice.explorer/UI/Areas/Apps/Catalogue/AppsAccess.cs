using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The Apps area's per-circuit probe of the caller's rights and apps, memoized so
/// the spine, Home, the palette and every Apps page share one set of calls.
/// </summary>
/// <remarks>
/// The probe runs detached from any one caller's token, so a waiter that gives up
/// (the directory's time box) does not poison the memo for the next one.
/// <see cref="Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppsAccess.Invalidate()"/> drops the memo after a lifecycle change.
/// Every memo is keyed on the caller (<see cref="AppsFacades.Caller"/>: the
/// sign-in, the endpoint and the asserted tenant), so a sign-in, a sign-out, a new
/// connection or a tenant switch re-probes, and an answer read for one caller is
/// never served to another.
/// <para>
/// A role binding, a grant or a group membership changed elsewhere - by another
/// administrator, a seeder or the directory - is not a change this circuit can see
/// (issue #5517). So a snapshot is trusted for
/// <see cref="ExplorerChromeOptions.CapabilityRefreshInterval"/> only: the first
/// read after that (the directory asks on every navigation, the address line on
/// every completion) re-probes in the background and is answered from the snapshot
/// it already holds, so no read waits and nothing probes on a render alone. When the
/// re-probe answers differently, the snapshot is replaced, <see cref="Changed"/> is
/// raised and the area directory is told, so the spine badge, Home, the palette and
/// the open Apps page follow without a reload. A re-probe the cluster did not
/// answer keeps the snapshot it had.
/// </para>
/// </remarks>
/// <param name="facades">The circuit's facades.</param>
/// <param name="logger">Where a failing probe is reported.</param>
/// <param name="time">The clock a lifecycle change and a snapshot's age are measured on.</param>
/// <param name="options">The chrome's bounds, for <see cref="ExplorerChromeOptions.CapabilityRefreshInterval"/>; the defaults when <see langword="null"/>.</param>
/// <param name="services">The circuit's services, to tell the area directory of a refreshed answer; resolved when one arrives, because the directory is built over the areas this probe serves.</param>
internal sealed class AppsAccess(
    AppsFacades facades,
    ILogger<AppsAccess>? logger = null,
    TimeProvider? time = null,
    ExplorerChromeOptions? options = null,
    IServiceProvider? services = null)
{
    /// <summary>How long after a lifecycle change an app's pages treat an unready read as settling rather than final.</summary>
    public static readonly TimeSpan SettlingWindow = TimeSpan.FromSeconds(30);

    private readonly TimeProvider _time = time ?? TimeProvider.System;
    private readonly TimeSpan _refreshInterval = (options ?? new ExplorerChromeOptions()).CapabilityRefreshInterval;

    // Filed under the caller as well as the slug, as every memo here is: a change one
    // sign-in, endpoint or tenant made never makes another caller's read settle (#4414).
    private readonly Dictionary<(ShellCallerKey Caller, string Slug), DateTimeOffset> _changedAt = [];
    /// <summary>How many catalogue entries completions search, at most.</summary>
    public const int CompletionIndexSize = AvailableAppQuery.MaxPageSize;

    private readonly ILogger _logger = logger ?? NullLogger<AppsAccess>.Instance;
    private readonly object _gate = new();
    private Task<AppsAccessSnapshot>? _snapshot;
    private ShellCallerKey _snapshotCaller;
    private DateTimeOffset _snapshotReadAt;
    private Task? _refresh;
    private AppsAccessSnapshot? _last;
    private ShellCallerKey _lastCaller;
    private Task<ImmutableArray<AvailableAppSummary>>? _index;
    private ShellCallerKey _indexCaller;

    /// <summary>
    /// Raised after <see cref="Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppsAccess.Invalidate()"/>, and after a background
    /// re-probe that answered differently, so a page can reload what it shows. It may
    /// be raised off the renderer's thread.
    /// </summary>
    public event Action? Changed;

    /// <summary>
    /// The most recent completed snapshot for the caller now - the previous one while
    /// a re-probe after <see cref="Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue.AppsAccess.Invalidate()"/> is still running - or
    /// <see langword="null"/> before the first probe for that caller completes. A
    /// snapshot read for another caller is never returned.
    /// </summary>
    public AppsAccessSnapshot? Current
    {
        get
        {
            var caller = facades.Caller;
            lock (_gate)
            {
                if (_snapshot is { IsCompletedSuccessfully: true } task && _snapshotCaller == caller)
                {
                    return task.Result;
                }

                return _lastCaller == caller ? _last : null;
            }
        }
    }

    /// <summary>
    /// The background re-read in flight, or a completed task when none is: what a test
    /// awaits instead of waiting for time to pass.
    /// </summary>
    internal Task Refreshing
    {
        get
        {
            lock (_gate)
            {
                return _refresh ?? Task.CompletedTask;
            }
        }
    }

    /// <summary>
    /// The caller's snapshot, probing on first use and again whenever the caller
    /// (sign-in, endpoint or asserted tenant) differs from the one the memo was read for.
    /// A snapshot older than <see cref="ExplorerChromeOptions.CapabilityRefreshInterval"/>
    /// is still returned at once, and re-read in the background (issue #5517).
    /// </summary>
    /// <param name="cancellationToken">Stops this caller waiting; the probe itself continues.</param>
    public Task<AppsAccessSnapshot> GetAsync(CancellationToken cancellationToken = default)
    {
        var caller = facades.Caller;
        Task<AppsAccessSnapshot> task;
        lock (_gate)
        {
            if (_snapshot is null || _snapshotCaller != caller)
            {
                _snapshot = ProbeAsync(caller);
                _snapshotCaller = caller;
                _index = null;
            }
            else if (_refresh is null
                && _snapshot.IsCompletedSuccessfully
                && _time.GetUtcNow() - _snapshotReadAt >= _refreshInterval)
            {
                _refresh = RefreshAsync(caller, _snapshot);
            }

            task = _snapshot;
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

        var caller = facades.Caller;
        Task<ImmutableArray<AvailableAppSummary>> task;
        lock (_gate)
        {
            if (_index is null || _indexCaller != caller)
            {
                _index = LoadIndexAsync(catalog);
                _indexCaller = caller;
            }

            task = _index;
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
        var caller = facades.Caller;
        lock (_gate)
        {
            _snapshot = ProbeAsync(caller);
            _snapshotCaller = caller;
            _index = null;
            if (slug is not null)
            {
                _changedAt[(caller, slug)] = _time.GetUtcNow();
            }
        }

        Changed?.Invoke();
    }

    /// <summary>
    /// Whether <paramref name="slug"/> went through a lifecycle change in this circuit,
    /// made by the caller now, within <see cref="SettlingWindow"/>. A cluster read right
    /// after an install or an enable can briefly miss it while the change settles; a
    /// change made under another sign-in, endpoint or tenant does not count.
    /// </summary>
    /// <param name="slug">The app slug.</param>
    public bool ChangedRecently(string slug)
    {
        var caller = facades.Caller;
        lock (_gate)
        {
            return _changedAt.TryGetValue((caller, slug), out var at) && _time.GetUtcNow() - at <= SettlingWindow;
        }
    }

    private async Task<AppsAccessSnapshot> ProbeAsync(ShellCallerKey caller)
    {
        var complete = await ReadAsync().ConfigureAwait(false);

        lock (_gate)
        {
            if (complete.ProbeUnanswered && _snapshotCaller == caller)
            {
                // The cluster did not answer a capability probe: answered to its own
                // waiters, but never remembered, so the next read asks again.
                _snapshot = null;
            }

            if (facades.Caller == caller)
            {
                // Stamped whether or not the memo is filed under this caller yet: a probe
                // that completes synchronously runs here before GetAsync files it.
                _last = complete;
                _lastCaller = caller;
                _snapshotReadAt = _time.GetUtcNow();
            }
            else if (_snapshotCaller == caller)
            {
                // The caller changed while this probe ran (a sign-in, a new
                // connection or a tenant switch), so some of its calls may have
                // carried the new one: it is answered to its own waiters but never
                // remembered as this caller's answer.
                _snapshot = null;
            }
        }

        return complete;
    }

    /// <summary>
    /// Re-reads the snapshot <paramref name="held"/> answered for <paramref name="caller"/>
    /// in the background, replacing it only while it is still the memo for that caller,
    /// and announcing it only when it answers differently (issue #5517).
    /// </summary>
    private async Task RefreshAsync(ShellCallerKey caller, Task<AppsAccessSnapshot> held)
    {
        try
        {
            // Yield first, so the read that started the refresh is answered from the
            // snapshot it already holds before any facade is called.
            await Task.Yield();
            var fresh = await ReadAsync().ConfigureAwait(false);
            lock (_gate)
            {
                // A sign-in, a tenant switch or a lifecycle change replaced the memo
                // while this ran: that probe's answer stands, not this one.
                if (facades.Caller != caller || _snapshotCaller != caller || !ReferenceEquals(_snapshot, held))
                {
                    return;
                }

                // Asked again only after another interval, whatever it answered.
                _snapshotReadAt = _time.GetUtcNow();
                if (fresh.ProbeUnanswered)
                {
                    // The cluster did not answer: the snapshot already held is kept,
                    // rather than replaced by one that grants and lists nothing.
                    return;
                }

                _last = fresh;
                _lastCaller = caller;
                if (fresh.ShowsSameAs(held.Result))
                {
                    return;
                }

                _snapshot = Task.FromResult(fresh);
                _index = null;
            }

            Changed?.Invoke();

            // The spine badge, Home and the palette are the directory's to re-ask.
            services?.GetService<ExplorerAreaDirectory>()?.Invalidate();
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            _logger.LogInformation(error, "The Apps area could not re-read your apps; the last answer is kept.");
        }
        finally
        {
            lock (_gate)
            {
                _refresh = null;
            }
        }
    }

    private async Task<AppsAccessSnapshot> ReadAsync()
    {
        var catalogCapabilities = Task.FromResult(new LatticeAppCatalogCapabilities());
        var controlCapabilities = Task.FromResult(new LatticeAppsCapabilities());
        Task<ImmutableArray<WorkspaceAppSummary>?> myApps = Task.FromResult<ImmutableArray<WorkspaceAppSummary>?>(null);
        var unanswered = 0;
        void Unanswered() => Interlocked.Exchange(ref unanswered, 1);

        if (facades.Catalog is { } catalog)
        {
            catalogCapabilities = GuardAsync(() => catalog.GetCapabilitiesAsync(), new LatticeAppCatalogCapabilities(), "catalogue capabilities", Unanswered);
        }

        if (facades.Control is { } control)
        {
            controlCapabilities = GuardAsync(() => control.GetCapabilitiesAsync(), new LatticeAppsCapabilities(), "app control capabilities", Unanswered);
        }

        if (facades.Workspace is { } workspace)
        {
            myApps = GuardAsync<ImmutableArray<WorkspaceAppSummary>?>(
                async () => await workspace.ListMyAppsAsync().ConfigureAwait(false),
                null,
                "your apps",
                Unanswered);
        }

        await Task.WhenAll(catalogCapabilities, controlCapabilities, myApps).ConfigureAwait(false);

        var snapshot = new AppsAccessSnapshot
        {
            Catalog = await catalogCapabilities.ConfigureAwait(false),
            Control = await controlCapabilities.ConfigureAwait(false),
            WorkspaceServed = (await myApps.ConfigureAwait(false)).HasValue,
            MyApps = (await myApps.ConfigureAwait(false)) ?? [],
            ProbeUnanswered = Volatile.Read(ref unanswered) == 1,
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

        return complete;
    }

    private Task<ImmutableArray<AvailableAppSummary>> LoadIndexAsync(ILatticeAppCatalog catalog) =>
        GuardAsync(
            async () => (await catalog.ListAvailableAsync(new AvailableAppQuery { PageSize = CompletionIndexSize }).ConfigureAwait(false)).Apps,
            [],
            "the catalogue index");

    private async Task<T> GuardAsync<T>(Func<Task<T>> probe, T denied, string what, Action? unanswered = null)
    {
        try
        {
            return await probe().ConfigureAwait(false);
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            if (unanswered is not null && AreaProbeFaults.IsTransient(error))
            {
                unanswered();
            }

            _logger.LogInformation(error, "The Apps area could not read {What}; it is treated as denied.", what);
            return denied;
        }
    }
}
