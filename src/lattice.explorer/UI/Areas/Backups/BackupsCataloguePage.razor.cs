using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Backups;

/// <summary>
/// The backup catalogue at <c>/backups</c>: newest first, one page at a time,
/// filtered on the server by kind, name prefix and tree, with the inventory in
/// the lede and, when a tree is chosen, that tree's schedule status.
/// </summary>
public partial class BackupsCataloguePage : IDisposable
{
    /// <summary>How many backups one page shows.</summary>
    public const int PageSize = 25;

    private static readonly IReadOnlyList<LtSelectOption> KindOptions =
    [
        new(string.Empty, "All kinds"),
        new("full", "Full"),
        new("incremental", "Incremental"),
    ];

    private readonly List<string?> _pageTokens = [null];
    private readonly Dictionary<string, BackupHealthReport?> _health = new(StringComparer.Ordinal);
    private readonly HashSet<string> _healthRead = new(StringComparer.Ordinal);
    private readonly ComponentLifetime _load = new();
    private ExplorerAddress? _loadedFor;
    private BackupCatalogPage? _page;
    private int _pageIndex;
    private string? _error;
    private string? _nameInput;
    private string? _treeInput;
    private string? _inventory;
    private bool _healthAvailable;
    private BackupScopeStatus? _scopeStatus;
    private bool _scopeStatusLoading;
    private string? _scopeStatusError;

    [Inject(Key = ShellFacades.Key)]
    internal ILatticeBackupControl Control { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [Inject]
    internal BackupsAccess Access { get; set; } = default!;

    [Inject]
    internal BackupOperations Operations { get; set; } = default!;

    private string KindValue => Address.GetQuery(BackupsAddresses.KindQuery) switch
    {
        "full" => "full",
        "incremental" => "incremental",
        _ => string.Empty,
    };

    private BackupKind? Kind => KindValue switch
    {
        "full" => BackupKind.Full,
        "incremental" => BackupKind.Incremental,
        _ => null,
    };

    private string? NamePrefix => Blank(Address.GetQuery(BackupsAddresses.NameQuery));

    private string? Tree => Blank(Address.GetQuery(BackupsAddresses.TreeQuery));

    private bool IsFiltered => Kind is not null || NamePrefix is not null || Tree is not null;

    private string Lede => _inventory
        ?? "Every backup you may read, newest first. Open one to restore, export or delete it.";

    /// <inheritdoc />
    public void Dispose()
    {
        _load.Leave();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var address = Address;
        if (address.Equals(_loadedFor))
        {
            return;
        }

        _loadedFor = address;
        _nameInput = NamePrefix;
        _treeInput = Tree;
        _pageTokens.Clear();
        _pageTokens.Add(null);
        _pageIndex = 0;
        await LoadAsync(loadExtras: true);
    }

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private static string? Blank(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    private static string KindText(BackupManifest manifest) =>
        manifest.SetName is { Length: > 0 } set
            ? BackupsFormat.Kind(manifest.Kind) + ", set " + set
            : BackupsFormat.Kind(manifest.Kind);

    private static string ScheduleText(bool registered, TimeSpan? interval) =>
        registered
            ? "Registered" + (interval is { } every ? ", every " + BackupsFormat.Interval(every) : string.Empty)
            : "None";

    private BackupHealthReport? HealthOf(BackupManifest manifest) =>
        _health.TryGetValue(manifest.Id, out var report) ? report : null;

    private Task SetKindAsync(string value)
    {
        Navigator.NavigateTo(Address.WithQuery(BackupsAddresses.KindQuery, Blank(value)));
        return Task.CompletedTask;
    }

    private Task SetNameAsync(string value)
    {
        Navigator.NavigateTo(Address.WithQuery(BackupsAddresses.NameQuery, Blank(value)));
        return Task.CompletedTask;
    }

    private Task SetTreeAsync(string value)
    {
        Navigator.NavigateTo(Address.WithQuery(BackupsAddresses.TreeQuery, Blank(value)));
        return Task.CompletedTask;
    }

    private Task ReloadAsync() => LoadAsync(loadExtras: true);

    private Task NextPageAsync()
    {
        if (_page?.NextPageToken is not { } token)
        {
            return Task.CompletedTask;
        }

        _pageIndex++;
        if (_pageTokens.Count > _pageIndex)
        {
            _pageTokens[_pageIndex] = token;
        }
        else
        {
            _pageTokens.Add(token);
        }

        return LoadAsync(loadExtras: false);
    }

    private Task PreviousPageAsync()
    {
        if (_pageIndex == 0)
        {
            return Task.CompletedTask;
        }

        _pageIndex--;
        return LoadAsync(loadExtras: false);
    }

    private async Task LoadAsync(bool loadExtras)
    {
        var cancellationToken = _load.Renew();

        _page = null;
        _error = null;
        _health.Clear();
        _healthRead.Clear();

        if (loadExtras)
        {
            _ = LoadExtrasAsync(cancellationToken);
        }

        BackupCatalogPage page;
        try
        {
            page = await Control.ListBackupsAsync(
                new BackupCatalogRequest
                {
                    PageSize = PageSize,
                    PageToken = _pageTokens[_pageIndex],
                    OrderByCreatedDescending = true,
                    Kind = Kind,
                    NamePrefix = NamePrefix,
                    TreeId = Tree,
                },
                cancellationToken);
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
        {
            _error = BackupsFaults.Describe(exception);
            return;
        }
        catch (OperationCanceledException)
        {
            return;
        }

        _page = page;
        if (_healthAvailable)
        {
            _ = LoadHealthAsync(page, cancellationToken);
        }
    }

    private async Task LoadExtrasAsync(CancellationToken cancellationToken)
    {
        try
        {
            var inventory = Access.GetInventoryAsync(cancellationToken);
            var health = Access.IsHealthMonitoringAvailableAsync(cancellationToken);
            var status = Tree is { } tree ? LoadScopeStatusAsync(tree, cancellationToken) : Task.CompletedTask;

            _healthAvailable = await health;
            if (_healthAvailable && _page is { } page)
            {
                _ = LoadHealthAsync(page, cancellationToken);
            }

            if (await inventory is { } report)
            {
                _inventory = InventorySentence(report);
            }

            await status;
            await InvokeAsync(StateHasChanged);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // A newer load replaced this one.
        }
    }

    private async Task LoadScopeStatusAsync(string tree, CancellationToken cancellationToken)
    {
        _scopeStatusLoading = true;
        _scopeStatusError = null;
        _scopeStatus = null;
        try
        {
            _scopeStatus = await Control.GetScopeStatusAsync(BackupScopeSelector.WholeTree(tree), cancellationToken);
        }
        catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
        {
            _scopeStatusError = BackupsFaults.Describe(exception);
        }
        finally
        {
            _scopeStatusLoading = false;
        }
    }

    private async Task LoadHealthAsync(BackupCatalogPage page, CancellationToken cancellationToken)
    {
        var reads = page.Entries.Select(async manifest =>
        {
            BackupHealthReport? report = null;
            try
            {
                report = await Control.GetBackupHealthAsync(manifest.Id, cancellationToken);
            }
            catch (Exception exception) when (!BackupsFaults.IsCancellation(exception, cancellationToken))
            {
                // A report that cannot be read reads as not checked.
            }

            return (manifest.Id, report);
        }).ToArray();

        try
        {
            foreach (var (id, report) in await Task.WhenAll(reads))
            {
                _health[id] = report;
                _healthRead.Add(id);
            }

            await InvokeAsync(StateHasChanged);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // A newer load replaced this one.
        }
    }

    private static string InventorySentence(BackupInventoryReport report) =>
        report.TotalBackupCount == 0
            ? "No backups yet."
            : BackupsFormat.Count(report.TotalBackupCount) + (report.TotalBackupCount == 1 ? " backup" : " backups")
                + " (" + BackupsFormat.Count(report.FullBackupCount) + " full, "
                + BackupsFormat.Count(report.IncrementalBackupCount) + " incremental), "
                + BackupsFormat.Bytes(report.TotalCatalogBytes)
                + (report.NewestBackupUtc is { } newest ? ", newest " + BackupsFormat.Time(newest) : string.Empty) + ".";
}
