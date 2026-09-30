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
    private bool _loaded;
    private string? _loadedScope;
    private (int Count, bool More)? _clusterWide;

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
            var of = Scope is { } tenant ? $" of tenant {tenant}" : string.Empty;
            return shown == loaded ? $"{loaded} rules{of}{more}" : $"{shown} of {loaded} rules{of}{more}";
        }
    }

    private string EmptyText => _rules is { Count: 0 }
        ? Scope is { } tenant ? $"No rules govern tenant {tenant}'s trees yet." : "No rules are defined on this cluster yet."
        : "No rule matches.";

    /// <summary>
    /// The tenant the page's address is rooted at: the listing is that tenant's
    /// rules only. <see langword="null"/> on the cluster-wide page.
    /// </summary>
    private string? Scope => Address.Tenant;

    /// <summary>
    /// The quiet line naming the cluster-wide rules that also apply to the scope
    /// tenant's trees, or <see langword="null"/> when there are none or the page
    /// is cluster-wide.
    /// </summary>
    private string? ClusterWideText => _clusterWide is { Count: > 0 } wide
        ? $"{wide.Count}{(wide.More ? "+" : string.Empty)} cluster-wide {(wide.Count == 1 && !wide.More ? "rule also applies" : "rules also apply")}."
        : null;

    private string ClusterWideHref => Navigator.Canonicalize(AccessRoutes.Rules.WithTenant(null)).ToHref();

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        // The page is reused when only the tenant root changes (/access to
        // /t/{tenant}/access), so the listing follows the address, not the instance.
        if (_loaded && string.Equals(_loadedScope, Scope, StringComparison.Ordinal))
        {
            return;
        }

        _loaded = true;
        _loadedScope = Scope;
        _editorOpen = string.Equals(Address.GetQuery(AccessRoutes.NewQuery), "true", StringComparison.Ordinal);
        _model ??= await Catalog.GetAccessModelAsync(CancellationToken.None).ConfigureAwait(true);
        await LoadFirstPageAsync().ConfigureAwait(true);
    }

    private async Task LoadFirstPageAsync()
    {
        var scope = Scope;
        _failure = null;
        _rules = null;
        _next = null;
        _clusterWide = null;
        try
        {
            var page = await Catalog.ListRulesAsync(scope, new AuthPageRequest { PageSize = PageSize }).ConfigureAwait(true);
            if (!string.Equals(scope, Scope, StringComparison.Ordinal))
            {
                return;
            }

            _rules = [.. page.Entries];
            _next = page.NextPageToken;
            if (scope is not null)
            {
                _clusterWide = await Catalog.CountClusterWideRulesAsync().ConfigureAwait(true);
            }
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

        var scope = Scope;
        _loadingMore = true;
        try
        {
            var page = await Catalog.ListRulesAsync(scope, new AuthPageRequest { PageSize = PageSize, PageToken = _next }).ConfigureAwait(true);
            if (string.Equals(scope, Scope, StringComparison.Ordinal))
            {
                _rules = [.. _rules, .. page.Entries];
                _next = page.NextPageToken;
            }
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
        if (_rules is not null && AccessCatalog.Lists(Scope, rule.Scope.TreeId))
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
