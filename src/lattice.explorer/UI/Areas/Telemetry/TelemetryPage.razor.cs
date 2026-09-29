using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Telemetry;

/// <summary>
/// The Telemetry area's page: the list of boards at <c>/telemetry</c> and one
/// board at <c>/telemetry/{board}</c>, both rooted at the active tenant while
/// tenancy is on.
/// </summary>
public partial class TelemetryPage : IDisposable
{
    private readonly HashSet<string> _denied = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _lifetime = new();

    private TelemetryQueryCatalog? _catalog;
    private IReadOnlyList<TelemetryBoardPlan> _plans = [];
    private TelemetryBoardPlan? _board;
    private TelemetryWindow _window = TelemetryWindow.FromAddress(ExplorerAddress.Home);
    private ExplorerAddress? _shown;
    private DateTimeOffset _now;
    private TelemetryTenantScope? _scope;
    private string? _failure;
    private bool _loading = true;
    private bool _stale = true;
    private bool _operator;
    private bool _operatorAsked;
    private int _generation;

    [Inject]
    internal TelemetryCatalogCache Catalog { get; set; } = default!;

    [Inject]
    internal ExplorerTenancy Tenancy { get; set; } = default!;

    [Inject]
    internal ExplorerAreaDirectory Directory { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private bool IsCompact => Breakpoint == LtBreakpoint.Compact;

    private string TitleText => _board is null ? "Telemetry - Orleans.Lattice Explorer" : $"{_board.Board.Title} - Telemetry - Orleans.Lattice Explorer";

    private string? TenantText => Tenancy.IsActive ? Address.Tenant ?? Tenancy.ActiveTenant : null;

    private IReadOnlyList<TelemetryBoardPlan> ListedBoards => [.. _plans.Where(plan => plan.HasCharts)];

    private IReadOnlyList<TelemetryQueryDescriptor> VisibleCharts =>
        _board is null ? [] : [.. _board.Charts.Where(chart => !_denied.Contains(chart.QueryId))];

    private bool ShowScopeChoice => Tenancy.IsActive && (_operator || _window.AllTenants);

    private string? ScopeText
    {
        get
        {
            if (!Tenancy.IsActive)
            {
                return null;
            }

            if (_scope is { } scope)
            {
                return TelemetryScopeCaption.Describe(scope) is { Narrowed: false } caption ? caption.Text : null;
            }

            return _window.AllTenants ? "Every tenant." : TenantText is { } tenant ? $"Tenant {tenant}." : null;
        }
    }

    private string? OmittedText
    {
        get
        {
            var omitted = _plans.SelectMany(plan => plan.Omitted).ToArray();
            return omitted.Length == 0
                ? null
                : $"Not shown: {string.Join(", ", omitted)}. Your grants or the cluster's metric allow-list do not admit {(omitted.Length == 1 ? "it" : "them")}.";
        }
    }

    private IReadOnlyList<string> Notes
    {
        get
        {
            var notes = new List<string>(3);
            if (_window.Notice is { } notice)
            {
                notes.Add(notice);
            }

            if (_board is not null)
            {
                var missing = _board.Omitted
                    .Concat(_board.Charts.Where(chart => _denied.Contains(chart.QueryId)).Select(chart => chart.Title))
                    .ToArray();
                if (missing.Length > 0)
                {
                    notes.Add($"Not shown: {string.Join(", ", missing)}. Your grants or the cluster's metric allow-list do not admit {(missing.Length == 1 ? "it" : "them")}.");
                }
            }

            if (Tenancy.IsActive && _scope is { } scope && TelemetryScopeCaption.Describe(scope) is { Narrowed: true } narrowed)
            {
                notes.Add(narrowed.Text);
            }

            return notes;
        }
    }

    private IReadOnlyList<LtSelectOption> BoardOptions =>
        [.. ListedBoards.Select(plan => new LtSelectOption(plan.Board.Key, plan.Board.Title))];

    private IReadOnlyList<LtSelectOption> RangeOptions
    {
        get
        {
            var options = new List<LtSelectOption> { new(string.Empty, "Default") };
            options.AddRange(TelemetryDurations.Ranges.Select(range => new LtSelectOption(TelemetryDurations.Format(range), TelemetryDurations.Label(range))));
            if (_window.Range is { } current && !TelemetryDurations.Ranges.Contains(current))
            {
                options.Add(new LtSelectOption(TelemetryDurations.Format(current), TelemetryDurations.Label(current)));
            }

            if (_window.IsAbsolute)
            {
                options.Add(new LtSelectOption(PinnedValue, "Pinned window"));
            }

            return options;
        }
    }

    private const string PinnedValue = "pinned";

    private string RangeValue => _window.IsAbsolute ? PinnedValue : _window.Range is { } range ? TelemetryDurations.Format(range) : string.Empty;

    private IReadOnlyList<LtSelectOption> StepOptions
    {
        get
        {
            var options = new List<LtSelectOption> { new(string.Empty, "Automatic") };
            options.AddRange(TelemetryDurations.Steps.Select(step => new LtSelectOption(TelemetryDurations.Format(step), TelemetryDurations.Label(step))));
            if (_window.Step is { } current && !TelemetryDurations.Steps.Contains(current))
            {
                options.Add(new LtSelectOption(TelemetryDurations.Format(current), TelemetryDurations.Label(current)));
            }

            return options;
        }
    }

    private string StepValue => _window.Step is { } step ? TelemetryDurations.Format(step) : string.Empty;

    private string PinHref
    {
        get
        {
            var (start, end) = _window.Resolve(_now)!.Value;
            return Href(Address
                .WithQuery(TelemetryWindow.RangeQuery, null)
                .WithQuery(TelemetryWindow.FromQuery, TelemetryWindow.FormatInstant(start))
                .WithQuery(TelemetryWindow.ToQuery, TelemetryWindow.FormatInstant(end)));
        }
    }

    private string ClearTreeHref => Href(Address.WithQuery(TelemetryWindow.TreeQuery, null));

    /// <inheritdoc />
    public void Dispose()
    {
        Catalog.Changed -= OnCatalogChanged;
        _lifetime.Cancel();
        _lifetime.Dispose();
    }

    /// <inheritdoc />
    protected override void OnInitialized() => Catalog.Changed += OnCatalogChanged;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var address = Address;
        if (!address.Equals(_shown))
        {
            _shown = address;
            _now = Time.GetUtcNow();
            _window = TelemetryWindow.FromAddress(address);
            _denied.Clear();
            _scope = null;
        }

        if (!_operatorAsked && Tenancy.IsActive)
        {
            _operatorAsked = true;
            _operator = Services.GetService<IExplorerTenantSwitcher>() is { } switcher
                && await IsOperatorAsync(switcher);
        }

        if (_stale)
        {
            await LoadAsync();
        }

        Resolve(address);
    }

    private async Task<bool> IsOperatorAsync(IExplorerTenantSwitcher switcher)
    {
        try
        {
            return await switcher.IsOperatorAsync(_lifetime.Token);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return false;
        }
    }

    private async Task LoadAsync()
    {
        _stale = false;
        _loading = true;
        _failure = null;
        try
        {
            _catalog = await Catalog.GetAsync(_lifetime.Token);
            _plans = TelemetryBoards.Plan(_catalog, Tenancy.IsActive);
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
            return;
        }
        catch (Exception)
        {
            _catalog = null;
            _plans = [];
            _failure = "The cluster did not return its metric catalogue. Try again in a moment.";
        }
        finally
        {
            _loading = false;
        }
    }

    private void Resolve(ExplorerAddress address)
    {
        var key = address.Path.Count == 0 ? null : address.Path[0];
        if (key is null)
        {
            _board = null;
            return;
        }

        if (_loading || _failure is not null)
        {
            _board = null;
            return;
        }

        _board = address.Path.Count == 1 ? TelemetryBoards.Find(_plans, key) : null;
        if (_board is null)
        {
            Navigation.NotFound();
        }
    }

    private void OnCatalogChanged() => _ = InvokeAsync(async () =>
    {
        _stale = true;
        _generation++;
        _now = Time.GetUtcNow();
        _denied.Clear();
        _scope = null;
        await LoadAsync();
        Resolve(Address);
        StateHasChanged();
    });

    private Task RefreshAsync()
    {
        // Dropping the shared catalogue raises Changed, which re-reads it here
        // and re-draws every chart; the directory re-probes the area too.
        Catalog.Invalidate();
        Directory.Invalidate();
        return Task.CompletedTask;
    }

    private Task OnDenied(string queryId)
    {
        _denied.Add(queryId);
        return Task.CompletedTask;
    }

    private Task OnScope(TelemetryTenantScope scope)
    {
        if (_scope is null || scope.WasDowngraded)
        {
            _scope = scope;
        }

        return Task.CompletedTask;
    }

    private static string ChartCount(TelemetryBoardPlan plan) =>
        plan.Omitted.Count == 0
            ? $"{plan.Charts.Count} {(plan.Charts.Count == 1 ? "chart" : "charts")}"
            : $"{plan.Charts.Count} of {plan.Total} charts";

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address).ToHref();

    private string BoardHref(TelemetryBoardPlan plan)
    {
        var target = ExplorerAddress.ForArea(TelemetryArea.AreaKey, plan.Board.Key).WithTenant(Address.Tenant);
        foreach (var (key, value) in Address.Query)
        {
            target = target.WithQuery(key, value);
        }

        return Href(target);
    }

    private string RangeHref(string value)
    {
        var target = Address
            .WithQuery(TelemetryWindow.FromQuery, null)
            .WithQuery(TelemetryWindow.ToQuery, null)
            .WithQuery(TelemetryWindow.RangeQuery, value.Length == 0 ? null : value);
        if (value.Length == 0)
        {
            target = target.WithQuery(TelemetryWindow.StepQuery, null);
        }

        return Href(target);
    }

    private string ViewHref(bool table) =>
        Href(Address.WithQuery(TelemetryWindow.ViewQuery, table ? TelemetryWindow.TableView : null));

    private string ScopeHref(bool all) =>
        Href(Address.WithQuery(TelemetryWindow.ScopeQuery, all ? TelemetryWindow.AllTenantsScope : null));

    private string TreeHref(string tree) => Href(Address.WithQuery(TelemetryWindow.TreeQuery, tree));

    private string DataHref(string tree) =>
        Href(ExplorerAddress.ForTree(DataAreaKey, tree).WithTenant(Address.Tenant));

    private void GoToBoard(string key)
    {
        if (TelemetryBoards.Find(_plans, key) is { } plan)
        {
            Navigation.NavigateTo(BoardHref(plan));
        }
    }

    private void GoToRange(string value)
    {
        if (value != PinnedValue)
        {
            Navigation.NavigateTo(RangeHref(value));
        }
    }

    private void GoToStep(string value) =>
        Navigation.NavigateTo(Href(Address.WithQuery(TelemetryWindow.StepQuery, value.Length == 0 ? null : value)));

    /// <summary>The Data area's key, which owns the per-tree metric tiles this area links to.</summary>
    internal const string DataAreaKey = "data";
}
