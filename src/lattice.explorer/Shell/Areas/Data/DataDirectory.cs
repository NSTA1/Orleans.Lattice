using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Data;

/// <summary>
/// The circuit's view of every tree and view the caller can reach, loaded once
/// through the Core catalogue reader and kept until it is refreshed. It is what
/// the directory page lists, what a tree address resolves against, and what the
/// area's availability, badge, Home status and completions read, so a circuit
/// pays for the catalogue once rather than once per surface.
/// </summary>
/// <remarks>
/// The catalogue reader is resolved lazily, so a head that registers no state
/// connection leaves the area hidden rather than failing the area directory.
/// Loads run on the directory's own lifetime, never on a caller's token, so a
/// caller that gives up (the chrome's availability time box) does not cancel the
/// load another caller is waiting for.
/// </remarks>
internal sealed class DataDirectory : IDisposable
{
    /// <summary>How many catalogue entries one page asks for.</summary>
    public const int PageSize = 500;

    /// <summary>The most catalogue pages one load reads, bounding a runaway catalogue.</summary>
    public const int MaximumPages = 400;

    private readonly IServiceProvider _services;
    private readonly ExplorerTenancy _tenancy;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly Lock _gate = new();
    private Task<IReadOnlyList<DataTreeEntry>>? _load;
    private Task<bool>? _probe;
    private IReadOnlyList<DataTreeEntry>? _entries;
    private Dictionary<string, DataTreeEntry>? _byStateId;

    /// <summary>Creates the directory.</summary>
    /// <param name="services">The circuit's services, from which the catalogue reader is resolved lazily.</param>
    /// <param name="tenancy">Whether, and to which tenant, the Explorer is scoped.</param>
    public DataDirectory(IServiceProvider services, ExplorerTenancy tenancy)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(tenancy);
        _services = services;
        _tenancy = tenancy;
    }

    /// <summary>Raised when a refresh replaced the loaded entries.</summary>
    public event Action? Changed;

    /// <summary>The loaded entries, or <see langword="null"/> before the first load completes.</summary>
    public IReadOnlyList<DataTreeEntry>? Loaded => _entries;

    /// <summary>
    /// Whether the caller can read the catalogue at all: resolves the reader and
    /// reads one entry. <see langword="false"/> means refused; any other failure
    /// throws, so the area can say it is unavailable rather than hide.
    /// </summary>
    /// <param name="cancellationToken">Stops waiting; the probe itself continues.</param>
    public async Task<bool> ProbeAsync(CancellationToken cancellationToken)
    {
        if (_entries is not null)
        {
            return true;
        }

        Task<bool> probe;
        lock (_gate)
        {
            probe = _probe ??= RunProbeAsync();
        }

        try
        {
            return await probe.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            ForgetProbe(probe);
            throw;
        }
    }

    /// <summary>Whether the catalogue reader can be resolved at all in this circuit.</summary>
    public bool HasReader => TryGetReader() is not null;

    /// <summary>Loads every entry, or returns the entries already loaded.</summary>
    /// <param name="cancellationToken">Stops waiting; the load itself continues.</param>
    public Task<IReadOnlyList<DataTreeEntry>> LoadAsync(CancellationToken cancellationToken = default)
    {
        if (_entries is { } loaded)
        {
            return Task.FromResult(loaded);
        }

        Task<IReadOnlyList<DataTreeEntry>> load;
        lock (_gate)
        {
            load = _load ??= RunLoadAsync();
        }

        return load.WaitAsync(cancellationToken);
    }

    /// <summary>Drops the loaded entries, loads them again and raises <see cref="Changed"/>.</summary>
    /// <param name="cancellationToken">Stops waiting; the load itself continues.</param>
    public async Task RefreshAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            _load = null;
            _probe = null;
            _entries = null;
            _byStateId = null;
        }

        Changed?.Invoke();
        try
        {
            await LoadAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Changed?.Invoke();
        }
    }

    /// <summary>
    /// Resolves the tree an address names, or <see langword="null"/> when the caller
    /// cannot reach one there. With tenancy on, only the address's tenant (or the
    /// active tenant) is matched.
    /// </summary>
    /// <param name="address">A Data area address.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    public async Task<DataTreeEntry?> ResolveAsync(ExplorerAddress address, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(address);
        if (address.TreeId is not { } logical)
        {
            return null;
        }

        var entries = await LoadAsync(cancellationToken).ConfigureAwait(false);
        var tenant = _tenancy.IsActive ? address.Tenant ?? _tenancy.ActiveTenant : null;
        foreach (var entry in entries)
        {
            if (string.Equals(entry.LogicalId, logical, StringComparison.Ordinal)
                && string.Equals(entry.Tenant, tenant, StringComparison.Ordinal))
            {
                return entry;
            }
        }

        return null;
    }

    /// <summary>Finds a loaded entry by the id the state API answered with.</summary>
    /// <param name="stateId">The state id.</param>
    public DataTreeEntry? FindByStateId(string? stateId) =>
        stateId is not null && _byStateId is { } index && index.TryGetValue(stateId, out var entry) ? entry : null;

    /// <summary>The loaded views whose source is <paramref name="tree"/>.</summary>
    /// <param name="tree">The source tree.</param>
    public IReadOnlyList<DataTreeEntry> ViewsOf(DataTreeEntry tree)
    {
        ArgumentNullException.ThrowIfNull(tree);
        if (_entries is not { } entries)
        {
            return [];
        }

        var views = new List<DataTreeEntry>();
        foreach (var entry in entries)
        {
            if (entry.Kind == DataTreeKind.View && string.Equals(entry.SourceStateId, tree.StateId, StringComparison.Ordinal))
            {
                views.Add(entry);
            }
        }

        return views;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Cancel();
        _lifetime.Dispose();
    }

    private ICatalogReader? TryGetReader()
    {
        try
        {
            return (ICatalogReader?)_services.GetService(typeof(ICatalogReader));
        }
        catch (InvalidOperationException)
        {
            // The reader is registered but its state connection is not: the head
            // serves no state API, so the area has nothing to read.
            return null;
        }
    }

    private async Task<bool> RunProbeAsync()
    {
        if (TryGetReader() is not { } reader)
        {
            return false;
        }

        try
        {
            await reader.LoadAsync(CatalogKind.Trees, pageToken: null, pageSize: 1, _lifetime.Token).ConfigureAwait(false);
            return true;
        }
        catch (Exception exception) when (DataErrors.IsDenied(exception))
        {
            return false;
        }
    }

    private async Task<IReadOnlyList<DataTreeEntry>> RunLoadAsync()
    {
        try
        {
            var reader = TryGetReader() ?? throw new InvalidOperationException("No state API is configured for this Explorer.");
            var trees = await ReadAllAsync(reader, CatalogKind.Trees).ConfigureAwait(false);
            var views = await ReadViewsAsync(reader).ConfigureAwait(false);
            var entries = Build(trees, views);
            lock (_gate)
            {
                _entries = entries;
                _byStateId = entries.ToDictionary(entry => entry.StateId, StringComparer.Ordinal);
            }

            return entries;
        }
        catch
        {
            lock (_gate)
            {
                _load = null;
            }

            throw;
        }
    }

    private async Task<IReadOnlyList<CatalogItem>> ReadViewsAsync(ICatalogReader reader)
    {
        try
        {
            return await ReadAllAsync(reader, CatalogKind.Views).ConfigureAwait(false);
        }
        catch (Exception exception) when (DataErrors.IsDenied(exception) || DataErrors.IsNotOffered(exception))
        {
            // A caller who may read trees but not the view catalogue still gets
            // the trees; views are simply absent.
            return [];
        }
    }

    private async Task<IReadOnlyList<CatalogItem>> ReadAllAsync(ICatalogReader reader, CatalogKind kind)
    {
        var items = new List<CatalogItem>();
        string? token = null;
        for (var page = 0; page < MaximumPages; page++)
        {
            var result = await reader.LoadAsync(kind, token, PageSize, _lifetime.Token).ConfigureAwait(false);
            items.AddRange(result.Items);
            token = result.NextPageToken;
            if (token is null)
            {
                break;
            }
        }

        return items;
    }

    private DataTreeEntry[] Build(IReadOnlyList<CatalogItem> trees, IReadOnlyList<CatalogItem> views)
    {
        var tenancyActive = _tenancy.IsActive;
        var viewIds = new HashSet<string>(views.Select(view => view.Id), StringComparer.Ordinal);
        var byState = new Dictionary<string, DataTreeEntry>(StringComparer.Ordinal);
        var result = new List<DataTreeEntry>(trees.Count + views.Count);

        foreach (var tree in trees)
        {
            if (tree.IsRestoreShadow
                || viewIds.Contains(tree.Id)
                || byState.ContainsKey(tree.Id)
                || !DataTreeNames.TryDescribe(tree.Id, tenancyActive, out var logical, out var tenant))
            {
                continue;
            }

            var entry = new DataTreeEntry
            {
                LogicalId = logical,
                StateId = tree.Id,
                Kind = DataTreeKind.Tree,
                Tenant = tenant,
                AppSlug = DataTreeNames.AppSlugOf(logical),
                ShardCount = tree.ShardCount,
                Lifecycle = tree.Lifecycle,
            };
            byState.Add(tree.Id, entry);
            result.Add(entry);
        }

        foreach (var view in views)
        {
            var source = view.SourceTreeId is { } sourceId && byState.TryGetValue(sourceId, out var found) ? found : null;
            if (byState.ContainsKey(view.Id) || (tenancyActive && source is null))
            {
                // With tenancy on, a view whose source tree the caller cannot see
                // belongs to another tenant: fail closed and leave it out.
                continue;
            }

            var entry = new DataTreeEntry
            {
                LogicalId = view.Id,
                StateId = view.Id,
                Kind = DataTreeKind.View,
                Tenant = source?.Tenant,
                ViewName = view.DisplayName ?? view.Id,
                SourceLogicalId = source?.LogicalId,
                SourceStateId = view.SourceTreeId,
                IsAggregation = view.IsAggregation,
                IsHistory = view.IsHistory,
                ProjectionVersion = view.ProjectionVersion,
                ShardCount = view.ShardCount,
            };
            byState.Add(view.Id, entry);
            result.Add(entry);
        }

        result.Sort(static (left, right) => string.CompareOrdinal(left.LogicalId, right.LogicalId));
        return [.. result];
    }

    private void ForgetProbe(Task<bool> probe)
    {
        lock (_gate)
        {
            if (ReferenceEquals(_probe, probe))
            {
                _probe = null;
            }
        }
    }
}
