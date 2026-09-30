using System.Collections.Immutable;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The source catalogue (<c>/apps/catalogue?source=&amp;filter=&amp;q=</c>): what each
/// configured source offers, joined with the tenant's installations, paged with
/// continuations. Shown only to an <c>AppInstall</c> holder; anyone else gets
/// not-found, never an error.
/// </summary>
public partial class AppsCataloguePage : IDisposable
{
    /// <summary>Above this many loaded rows the table virtualises, so thousands of apps stay cheap to draw.</summary>
    internal const int VirtualizeThreshold = 100;

    /// <summary>The id of the sentence that says why the search field is disabled, which that field is described by.</summary>
    private const string SearchHintId = "apps-search-hint";

    private static readonly (AvailableAppFilter Filter, string Text)[] Filters =
    [
        (AvailableAppFilter.All, "All"),
        (AvailableAppFilter.Installed, "Installed"),
        (AvailableAppFilter.Available, "Available"),
        (AvailableAppFilter.Updates, "Updates"),
    ];

    private static readonly IReadOnlyList<LtSelectOption> FilterOptions =
        [.. Filters.Select(filter => new LtSelectOption(AppsRoutes.FilterText(filter.Filter), filter.Text))];

    private readonly Dictionary<string, string?> _icons = new(StringComparer.Ordinal);
    private readonly List<AvailableAppSummary> _rows = [];
    private IReadOnlyList<AvailableAppSummary> _items = [];
    private Dictionary<string, int> _offeredBy = new(StringComparer.Ordinal);
    private CancellationTokenSource _load = new();
    private AppsAccessSnapshot? _snapshot;
    private ImmutableArray<AvailableAppSummary> _index = [];
    private ImmutableArray<AppSourceSummary>? _sources;
    private AppsCatalogueView _view = AppsCatalogueView.Default;
    private ExplorerAddress? _loadedFor;
    private string? _continuation;
    private string? _text;
    private string? _error;
    private bool _loading;
    private bool _denied;

    [Inject]
    internal AppsAccess Access { get; set; } = default!;

    [Inject]
    internal AppsFacades Facades { get; set; } = default!;

    [Inject]
    private NavigationManager Navigation { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private bool IsCompact => Breakpoint == LtBreakpoint.Compact;

    private bool CanReview => _snapshot?.CanReview == true;

    private string TenantPhrase => Address.Tenant is { } tenant ? $" for tenant {tenant}" : string.Empty;

    private IReadOnlyList<LtSelectOption> SourceOptions =>
    [
        new LtSelectOption(AppsRoutes.AllSources, "All sources"),
        .. (_sources ?? []).Select(source => new LtSelectOption(source.Key, $"{source.DisplayName} ({source.Kind})")),
    ];

    private AppSourceSummary? SelectedSource =>
        _view.SourceKey is { } key ? (_sources ?? []).FirstOrDefault(source => string.Equals(source.Key, key, StringComparison.Ordinal)) : null;

    private string SourceHint => SelectedSource is { } source
        ? AppsPresentation.SourceDescription(source)
        : string.Join(". ", (_sources ?? []).Select(AppsPresentation.SourceDescription));

    private bool TextEnabled => SelectedSource is { } selected
        ? selected.Capabilities.HasFlag(AppSourceSummaryCapabilities.Search)
        : (_sources ?? []).Any(source => source.Capabilities.HasFlag(AppSourceSummaryCapabilities.Search));

    private string Caption => SelectedSource is { } source ? $"Apps offered by {source.DisplayName}" : "Apps offered by every source";

    private string CountText
    {
        get
        {
            var sources = _sources?.Length ?? 0;
            var apps = _rows.Count;
            return $"{sources} {(sources == 1 ? "source" : "sources")}, {apps}{(_continuation is null ? string.Empty : "+")} {(apps == 1 ? "app" : "apps")}";
        }
    }

    private string EmptyText => _view.Filter switch
    {
        AvailableAppFilter.Installed => "Nothing from this selection is installed here.",
        AvailableAppFilter.Updates => "Every installed app is on its source's newest version.",
        AvailableAppFilter.Available => "Every app from this selection is already installed.",
        _ when _view.Text is not null && TextEnabled => "No app matches the search.",
        _ => "The selected source offers no apps.",
    };

    /// <summary>Cancels outstanding reads.</summary>
    public void Dispose()
    {
        Access.Changed -= OnAccessChanged;
        _load.Cancel();
        _load.Dispose();
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        Access.Changed += OnAccessChanged;
        _snapshot = await Access.GetAsync(_load.Token);
        if (!_snapshot.CanBrowseCatalogue || Facades.Catalog is null)
        {
            // A caller without AppInstall learns nothing about what exists: not found, never an error.
            _denied = true;
            NotFound();
            return;
        }

        try
        {
            _sources = await Facades.Catalog.ListSourcesAsync(_load.Token);
        }
        catch (Exception error) when (error is not OperationCanceledException)
        {
            _sources = [];
            _error = AppsFailureMessages.Describe(error, "list the sources of", "the catalogue");
        }

        _index = await Access.GetCompletionIndexAsync(_load.Token);
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (_denied || _sources is null || Equals(_loadedFor, Address))
        {
            return;
        }

        _loadedFor = Address;
        _view = AppsCatalogueView.FromAddress(Address);
        _text = _view.Text;
        await ReloadAsync();
    }

    private async Task ReloadAsync()
    {
        _rows.Clear();
        _items = [];
        CountOfferingSources();
        _continuation = null;
        await LoadPageAsync();
    }

    private Task LoadMoreAsync() => LoadPageAsync();

    private async Task LoadPageAsync()
    {
        if (Facades.Catalog is not { } catalog)
        {
            return;
        }

        var token = _load.Token;
        _loading = true;
        _error = null;
        StateHasChanged();
        try
        {
            var page = await catalog.ListAvailableAsync(_view.ToQuery(TextEnabled, _continuation), token);
            _rows.AddRange(page.Apps);
            _items = [.. _rows];
            CountOfferingSources();
            _continuation = page.Continuation;
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception error)
        {
            _error = AppsFailureMessages.Describe(error, "list the apps of", "the catalogue");
        }
        finally
        {
            _loading = false;
        }

        StateHasChanged();
        await LoadIconsAsync(catalog, token);
    }

    private async Task LoadIconsAsync(ILatticeAppCatalog catalog, CancellationToken token)
    {
        foreach (var app in _rows.Where(app => app.Presentation?.Icon is not null).ToArray())
        {
            var key = IconKey(app);
            if (_icons.ContainsKey(key))
            {
                continue;
            }

            try
            {
                _icons[key] = AppsPresentation.IconDataUrl(await catalog.GetIconAsync(app.SourceKey, app.Slug, app.NewestVersion, token));
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception)
            {
                _icons[key] = null;
            }

            StateHasChanged();
        }
    }

    private void NotFound()
    {
        try
        {
            Navigation.NotFound();
        }
        catch (InvalidOperationException)
        {
            // A renderer without a not-found handler: the page simply renders nothing.
        }
    }

    private void OnAccessChanged() => _ = InvokeAsync(async () =>
    {
        _snapshot = await Access.GetAsync(_load.Token);
        _index = await Access.GetCompletionIndexAsync(_load.Token);
        CountOfferingSources();
        await ReloadAsync();
    });

    private Task SelectSourceAsync(string value)
    {
        Go(_view with { SourceKey = string.Equals(value, AppsRoutes.AllSources, StringComparison.Ordinal) ? null : value });
        return Task.CompletedTask;
    }

    private Task SelectFilterAsync(AvailableAppFilter filter)
    {
        Go(_view with { Filter = filter });
        return Task.CompletedTask;
    }

    private Task SelectFilterFromTextAsync(string value) => SelectFilterAsync(AppsRoutes.ReadFilter(value));

    private Task SearchAsync(string text)
    {
        Go(_view with { Text = string.IsNullOrWhiteSpace(text) ? null : text.Trim() });
        return Task.CompletedTask;
    }

    private void Go(AppsCatalogueView view) => Navigator.NavigateTo(AppsRoutes.Catalogue(Address.Tenant, view));

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private string? IconOf(AvailableAppSummary app) => _icons.GetValueOrDefault(IconKey(app));

    private static string IconKey(AvailableAppSummary app) => $"{app.SourceKey}/{app.Slug}@{app.NewestVersion}";

    private int OfferedBy(string slug) => _offeredBy.GetValueOrDefault(slug);

    // Counted once per loaded page, not per rendered cell, so a long catalogue stays linear.
    private void CountOfferingSources() =>
        _offeredBy = _rows.Concat(_index)
            .Select(app => (app.Slug, app.SourceKey))
            .Distinct()
            .GroupBy(pair => pair.Slug, StringComparer.Ordinal)
            .ToDictionary(group => group.Key, group => group.Count(), StringComparer.Ordinal);

    private bool CanOpen(AvailableAppSummary app) =>
        app.InstalledState == AppLifecycleState.Enabled
        && _snapshot is { } snapshot
        && snapshot.MyApps.Any(mine => string.Equals(mine.Slug, app.Slug, StringComparison.Ordinal) && mine.HasUi);

    private static (string Text, string? Version) PrimaryAction(AvailableAppSummary app)
    {
        if (app.InstalledState is null or AppLifecycleState.NotInstalled or AppLifecycleState.Uninstalled)
        {
            return ("Review", app.NewestVersion);
        }

        if (app.InstalledState == AppLifecycleState.Failed)
        {
            return ("Re-consent", app.InstalledVersion);
        }

        return AppsPresentation.HasUpdate(app) ? ("Upgrade", app.NewestVersion) : ("Manage", app.InstalledVersion);
    }
}
