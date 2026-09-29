using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The rule list (<c>/access/rules</c>, also the area root): every rule, paged,
/// with a search, an ownership filter, and a "New rule" editor. Rules link to
/// their own page, where authored rules are edited and deleted.
/// </summary>
public partial class AccessRulesPage
{
    private const int PageSize = 200;
    private const string AllFilter = "all";
    private const string AuthoredFilter = "authored";
    private const string AppFilter = "app";

    private List<LatticeAuthorizationRule>? _rules;
    private IReadOnlyList<LatticeAuthorizationRule>? _visible;
    private List<LatticeAuthorizationRule>? _visibleSource;
    private string? _visibleFilter;
    private string? _visibleSearch;
    private string? _next;
    private AccessFailure? _failure;
    private AccessModelDescriptor? _model;
    private string _search = string.Empty;
    private string _filter = AllFilter;
    private bool _editorOpen;
    private bool _loadingMore;

    [Inject]
    internal AccessCatalog Catalog { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private static IReadOnlyList<LtSelectOption> FilterOptions { get; } =
    [
        new(AllFilter, "All rules"),
        new(AuthoredFilter, "Authored"),
        new(AppFilter, "App-owned"),
    ];

    private bool IsCompact => Breakpoint == LtBreakpoint.Compact;

    private LtDialogPlacement DialogPlacement => IsCompact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private IReadOnlyList<LatticeAuthorizationRule> Visible
    {
        get
        {
            if (_rules is null)
            {
                return [];
            }

            // Memoised per (loaded rules, filter, search), so the table keeps one
            // items instance across renders and re-reads its rows only when they change.
            if (_visible is not null && ReferenceEquals(_visibleSource, _rules) && _visibleFilter == _filter && _visibleSearch == _search)
            {
                return _visible;
            }

            var term = _search.Trim();
            _visible =
            [
                .. _rules.Where(rule =>
                    _filter switch
                    {
                        AuthoredFilter => !LatticeAppRuleIds.IsAppOwned(rule.RuleId),
                        AppFilter => LatticeAppRuleIds.IsAppOwned(rule.RuleId),
                        _ => true,
                    }
                    && (term.Length == 0
                        || rule.RuleId.Contains(term, StringComparison.OrdinalIgnoreCase)
                        || rule.Subject.Id.Contains(term, StringComparison.OrdinalIgnoreCase)
                        || rule.Scope.TreeId.Contains(term, StringComparison.OrdinalIgnoreCase))),
            ];
            _visibleSource = _rules;
            _visibleFilter = _filter;
            _visibleSearch = _search;
            return _visible;
        }
    }

    private string CountText
    {
        get
        {
            var shown = Visible.Count;
            var loaded = _rules?.Count ?? 0;
            var more = _next is null ? string.Empty : ", more to load";
            return shown == loaded ? $"{loaded} rules{more}" : $"{shown} of {loaded} rules{more}";
        }
    }

    private string EmptyText => _rules is { Count: 0 } ? "No rules are defined on this cluster yet." : "No rule matches.";

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        _editorOpen = string.Equals(Address.GetQuery(AccessRoutes.NewQuery), "true", StringComparison.Ordinal);
        _model = await Catalog.GetAccessModelAsync(CancellationToken.None).ConfigureAwait(true);
        await LoadFirstPageAsync().ConfigureAwait(true);
    }

    private async Task LoadFirstPageAsync()
    {
        _failure = null;
        _rules = null;
        _next = null;
        try
        {
            var page = await Catalog.Admin.ListRulesAsync(new AuthPageRequest { PageSize = PageSize }).ConfigureAwait(true);
            _rules = [.. page.Entries];
            _next = page.NextPageToken;
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            _failure = failure;
        }
    }

    private async Task LoadMoreAsync()
    {
        if (_next is null || _rules is null)
        {
            return;
        }

        _loadingMore = true;
        try
        {
            var page = await Catalog.Admin.ListRulesAsync(new AuthPageRequest { PageSize = PageSize, PageToken = _next }).ConfigureAwait(true);
            _rules = [.. _rules, .. page.Entries];
            _next = page.NextPageToken;
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            Toasts.Show(failure.Message, LtToastTone.Danger);
        }
        finally
        {
            _loadingMore = false;
        }
    }

    private Task ReloadAsync() => LoadFirstPageAsync();

    private void OpenEditor() => _editorOpen = true;

    private Task OnSavedAsync(LatticeAuthorizationRule rule)
    {
        _editorOpen = false;
        if (_rules is not null)
        {
            _rules =
            [
                rule,
                .. _rules.Where(existing => !(existing.RuleId == rule.RuleId && existing.Scope.TreeId == rule.Scope.TreeId)),
            ];
        }

        Toasts.Show($"Rule {rule.RuleId} saved.", LtToastTone.Success);
        return Task.CompletedTask;
    }
}
