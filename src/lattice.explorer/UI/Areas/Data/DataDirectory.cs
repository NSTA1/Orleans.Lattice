using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

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
/// <para>
/// What is remembered belongs to one caller at one endpoint. The catalogue is
/// filtered by the caller's own read rights and tenant scope, so the memo is keyed
/// on the signed-in identity, the configured endpoint and the active tenant, and
/// any change to one of them forgets it:
/// a catalogue read before sign-in never outlives the sign-in, and one identity's
/// trees are never shown to the next after a sign-out.
/// </para>
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
    private string? _sharingNote;
    private int _generation;
    private (bool Authenticated, string? User, string? Endpoint, string? Tenant) _caller;

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
    /// Why the loaded entries hold no tree, or not every tree, other tenants share
    /// with this one - the caller cannot list the tenant's grants, or a grant names
    /// a scope that shares nothing - or <see langword="null"/> when nothing is
    /// missing.
    /// </summary>
    public string? SharingNote => _entries is null ? null : _sharingNote;

    /// <summary>
    /// Whether another tenant can share a tree with the tenant being listed: only
    /// with tenancy on and a tenant other than the reserved default one, which
    /// takes no part in grants. Where it cannot, the listing offers no sharing
    /// filter and no "Shared by" column.
    /// </summary>
    public bool SharingApplies => SharingAppliesTo(_tenancy.IsActive, _tenancy.ActiveTenant);

    /// <summary>Whether a tree can be shared with <paramref name="tenant"/>.</summary>
    /// <param name="tenancyActive">Whether tenancy is on.</param>
    /// <param name="tenant">The tenant being listed, or <see langword="null"/>.</param>
    internal static bool SharingAppliesTo(bool tenancyActive, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] string? tenant) =>
        tenancyActive
        && !string.IsNullOrEmpty(tenant)
        && !string.Equals(tenant, ExplorerTenantTrees.DefaultTenantId, StringComparison.Ordinal);

    /// <summary>
    /// Whether the caller can read the catalogue at all: resolves the reader and
    /// reads one entry. <see langword="false"/> means refused; any other failure
    /// throws, so the area can say it is unavailable rather than hide.
    /// </summary>
    /// <param name="cancellationToken">Stops waiting; the probe itself continues.</param>
    public async Task<bool> ProbeAsync(CancellationToken cancellationToken)
    {
        ForgetIfTheCallerChanged();
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
        ForgetIfTheCallerChanged();
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
        Forget();
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
    /// Forgets the loaded entries without reading them again, so the next reader
    /// loads afresh, and raises <see cref="Changed"/>. The Tenancy area calls it
    /// when a grant this circuit approved, rejected or revoked changes which trees
    /// are shared with the tenant.
    /// </summary>
    public void Invalidate()
    {
        Forget();
        Changed?.Invoke();
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
            if (entry.Kind != DataTreeKind.Prefix
                && string.Equals(entry.LogicalId, logical, StringComparison.Ordinal)
                && string.Equals(entry.Tenant, tenant, StringComparison.Ordinal))
            {
                return entry;
            }
        }

        // A tree under a shared prefix (or below a shared tree) is not listed,
        // because another tenant's trees cannot be enumerated, but the grant makes
        // it readable: resolve it against the grant that covers it.
        return tenant is null
            ? null
            : DataSharedTrees.Cover(entries.Where(entry => string.Equals(entry.Tenant, tenant, StringComparison.Ordinal)), logical);
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

    /// <summary>
    /// Drops everything remembered when the caller's identity or endpoint is not
    /// the one it was remembered for. Reading the current caller allocates nothing
    /// in the steady state: the tuple is compared by value.
    /// </summary>
    private void ForgetIfTheCallerChanged()
    {
        var caller = CurrentCaller();
        lock (_gate)
        {
            if (caller == _caller)
            {
                return;
            }

            _caller = caller;
            _generation++;
            _load = null;
            _probe = null;
            _entries = null;
            _byStateId = null;
            _sharingNote = null;
        }
    }

    private void Forget()
    {
        lock (_gate)
        {
            _generation++;
            _load = null;
            _probe = null;
            _entries = null;
            _byStateId = null;
            _sharingNote = null;
        }
    }

    private (bool Authenticated, string? User, string? Endpoint, string? Tenant) CurrentCaller()
    {
        var auth = _services.GetService(typeof(IExplorerAuthSession)) as IExplorerAuthSession;
        var session = _services.GetService(typeof(IExplorerSession)) as IExplorerSession;
        return (auth?.IsAuthenticated == true, auth?.Username, session?.Current?.Endpoint, _tenancy.ActiveTenant);
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
        var caller = _caller;
        var generation = _generation;

        // Every call this load makes asserts the tenant the load began in, even if
        // the circuit moves to another tenant meanwhile; such a load is returned
        // to its own waiters but never remembered.
        using var pin = PinAssertedTenant();
        try
        {
            var reader = TryGetReader() ?? throw new InvalidOperationException("No state API is configured for this Explorer.");
            var trees = await ReadAllAsync(reader, CatalogKind.Trees).ConfigureAwait(false);
            var views = await ReadViewsAsync(reader).ConfigureAwait(false);
            var owned = Build(trees, views);
            var (shared, note) = await ReadSharedAsync(caller.Tenant, owned).ConfigureAwait(false);
            DataTreeEntry[] entries = shared.Count == 0 ? owned : [.. owned, .. shared];
            lock (_gate)
            {
                // A load that finished for a caller who has since changed, or
                // after the directory was invalidated, is returned to its own
                // waiters but never remembered.
                if (caller == _caller && generation == _generation)
                {
                    _entries = entries;
                    _sharingNote = note;
                    _byStateId = entries.ToDictionary(entry => entry.StateId, StringComparer.Ordinal);
                }
            }

            return entries;
        }
        catch
        {
            lock (_gate)
            {
                if (generation == _generation)
                {
                    _load = null;
                }
            }

            throw;
        }
    }

    private IDisposable? PinAssertedTenant() =>
        _services.GetService(typeof(ShellAssertedTenant)) is ShellAssertedTenant asserted
            ? asserted.Pin(asserted.AssertedTenant)
            : null;

    /// <summary>
    /// Reads the trees other tenants share with <paramref name="tenant"/> through
    /// grants it approved, with the caller's own authority. Any failure - no grant
    /// facade, a refusal, a fault - fails closed to the owned trees alone and says
    /// so in the note; it never fails the directory.
    /// </summary>
    private async Task<(IReadOnlyList<DataTreeEntry> Shared, string? Note)> ReadSharedAsync(string? tenant, DataTreeEntry[] owned)
    {
        // Grants are tenant to tenant, and the reserved default tenant takes no
        // part in them: without tenancy, or at the default tenant, nothing is shared.
        if (!SharingAppliesTo(_tenancy.IsActive, tenant))
        {
            return ([], null);
        }

        ILatticeTenantGrantAdmin? grants;
        try
        {
            grants = _services.GetShellFacade<ILatticeTenantGrantAdmin>();
        }
        catch (InvalidOperationException)
        {
            grants = null;
        }

        if (grants is null)
        {
            return ([], DataSharedTrees.NotOfferedNote(tenant));
        }

        TenantGrantReport report;
        try
        {
            report = await grants.ListGrantsAsync(tenant, _lifetime.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            return ([], DataSharedTrees.NoteFor(exception, tenant));
        }

        var ownedIds = new HashSet<string>(owned.Length, StringComparer.Ordinal);
        foreach (var entry in owned)
        {
            ownedIds.Add(entry.LogicalId);
        }

        var result = DataSharedTrees.Build(report, tenant, ownedIds);
        return (result.Entries, result.Unreadable == 0 ? null : DataSharedTrees.UnreadableNote(result.Unreadable));
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
