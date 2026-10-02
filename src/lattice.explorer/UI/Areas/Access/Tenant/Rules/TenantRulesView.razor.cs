using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// The rules governing the tenant (<c>/t/{tenant}/access/rules</c>), from the
/// tenant policy's listing: the platform rules on the tenant's trees, read-only
/// and decided first, and the tenant's own rules, each linking to its page. It
/// filters by tree and by subject, shows the tenant's rule cap and its usage, and
/// opens the editor for a new rule.
/// </summary>
public partial class TenantRulesView
{
    private const int PageSize = 200;
    private const string AllTrees = "";
    private const string TenantWideTrees = "*";

    private static readonly List<TenantRuleView> NoRules = [];

    private List<TenantRuleView>? _rules;
    private string? _next;
    private AccessFailure? _failure;
    private AccessModelDescriptor? _model;
    private TenantAccessPosture? _posture;
    private string _treeFilter = AllTrees;
    private string _subjectFilter = string.Empty;
    private bool _editorOpen;
    private bool _loadingMore;
    private string? _loadedTenant;
    private Filtered? _filtered;
    private (List<TenantRuleView> Rules, IReadOnlyList<LtSelectOption> Options)? _treeOptions;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private IReadOnlyList<TenantRuleView> PlatformRules => Filter().Platform;

    private IReadOnlyList<TenantRuleView> TenantRules => Filter().Tenant;

    private string PlatformEmptyText => IsFiltering ? "No platform rule matches." : $"No platform rule governs tenant {Tenant}'s trees.";

    private string TenantEmptyText => IsFiltering ? "No rule of the tenant's matches." : $"Tenant {Tenant} has no rules of its own yet.";

    private bool IsFiltering => _treeFilter.Length > 0 || _subjectFilter.Trim().Length > 0;

    private string CountText
    {
        get
        {
            var loaded = _rules?.Count ?? 0;
            var shown = PlatformRules.Count + TenantRules.Count;
            var more = _next is null ? string.Empty : ", more to load";
            return shown == loaded
                ? $"{loaded} {(loaded == 1 ? "rule" : "rules")} of tenant {Tenant}{more}"
                : $"{shown} of {loaded} rules of tenant {Tenant}{more}";
        }
    }

    /// <summary>The tree filter's choices: every tree, every tree in the tenant, and each tree a loaded rule names.</summary>
    private IReadOnlyList<LtSelectOption> TreeOptions
    {
        get
        {
            var rules = _rules ?? NoRules;
            if (_treeOptions is { } remembered && ReferenceEquals(remembered.Rules, rules))
            {
                return remembered.Options;
            }

            var trees = new SortedSet<string>(StringComparer.Ordinal);
            var tenantWide = false;
            foreach (var rule in rules)
            {
                if (rule.ScopeKind == TenantRuleScopeKind.TenantWide)
                {
                    tenantWide = true;
                }
                else if (rule.TreeName is { } tree)
                {
                    trees.Add(tree);
                }
            }

            var options = new List<LtSelectOption>(trees.Count + 2) { new(AllTrees, "All trees") };
            if (tenantWide)
            {
                options.Add(new LtSelectOption(TenantWideTrees, "Every tree in this tenant"));
            }

            foreach (var tree in trees)
            {
                options.Add(new LtSelectOption(tree, tree));
            }

            _treeOptions = (rules, options);
            return options;
        }
    }

    /// <summary>
    /// The tenant's rule cap and its usage, from the posture probe, or
    /// <see langword="null"/> when the posture reports none.
    /// </summary>
    private string? CapText => _posture?.TenantRules is { IsBounded: true } usage
        ? usage.IsMeasured
            ? AtCap
                ? $"{usage.Usage} of {usage.Limit} tenant rules used. Tenant {Tenant} is at its cap: remove one of its rules before adding another."
                : $"{usage.Usage} of {usage.Limit} tenant rules used."
            : $"Tenant {Tenant} may have up to {usage.Limit} rules of its own."
        : null;

    private bool AtCap => _posture?.TenantRules is { Usage: { } used, Limit: { } limit } && used >= limit;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (string.Equals(_loadedTenant, Tenant, StringComparison.Ordinal))
        {
            return;
        }

        _loadedTenant = Tenant;
        _editorOpen = string.Equals(Navigator.Current?.GetQuery(AccessRoutes.NewQuery), "true", StringComparison.Ordinal);
        await LoadAsync().ConfigureAwait(true);
    }

    private async Task LoadAsync()
    {
        var tenant = Tenant;
        _failure = null;
        _rules = null;
        _next = null;
        try
        {
            var policy = Access.Policy ?? throw new NotSupportedException("This Explorer does not serve tenant rules.");
            var page = await policy.ListRulesAsync(tenant, new TenantAccessPageRequest { PageSize = PageSize }, Lifetime.Token).ConfigureAwait(true);
            if (Lifetime.IsLeft || !string.Equals(tenant, Tenant, StringComparison.Ordinal))
            {
                return;
            }

            _rules = [.. page?.Entries ?? []];
            _next = page?.NextPageToken;
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
            return;
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            _failure = failure;
            return;
        }

        await ReadPostureAsync(tenant).ConfigureAwait(true);
        _model ??= await ReadModelAsync().ConfigureAwait(true);
    }

    private async Task LoadMoreAsync()
    {
        if (_next is null || _rules is null)
        {
            return;
        }

        var tenant = Tenant;
        _loadingMore = true;
        try
        {
            var policy = Access.Policy ?? throw new NotSupportedException("This Explorer does not serve tenant rules.");
            var page = await policy.ListRulesAsync(tenant, new TenantAccessPageRequest { PageSize = PageSize, PageToken = _next }, Lifetime.Token).ConfigureAwait(true);
            if (!Lifetime.IsLeft && string.Equals(tenant, Tenant, StringComparison.Ordinal))
            {
                _rules = [.. _rules, .. page?.Entries ?? []];
                _next = page?.NextPageToken;
            }
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            Toasts.Show(failure.Message, LtToastTone.Danger);
        }
        finally
        {
            _loadingMore = false;
        }
    }

    private async Task ReadPostureAsync(string tenant)
    {
        try
        {
            var state = await Access.GetStateAsync(tenant, Lifetime.Token).ConfigureAwait(true);
            _posture = string.Equals(tenant, Tenant, StringComparison.Ordinal) ? state.Posture : null;
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
        }
    }

    /// <summary>The cluster's access model, which says whether cluster users and groups can be searched; <see langword="null"/> when it cannot be read.</summary>
    private async Task<AccessModelDescriptor?> ReadModelAsync()
    {
        AccessCatalog? catalog;
        try
        {
            catalog = Services.GetService<AccessCatalog>();
        }
        catch (InvalidOperationException)
        {
            // A head without the auth facade: cluster users and groups are typed, not searched.
            return null;
        }

        return catalog is null ? null : await catalog.GetAccessModelAsync(Lifetime.Token).ConfigureAwait(true);
    }

    private void OpenEditor() => _editorOpen = true;

    private async Task OnSaved(TenantRuleView rule)
    {
        _editorOpen = false;
        if (_rules is not null)
        {
            _rules =
            [
                .. _rules.Where(existing => !(existing.Layer == TenantRuleLayer.Tenant && string.Equals(existing.RuleId, rule.RuleId, StringComparison.Ordinal))),
                rule,
            ];
        }

        Toasts.Show($"Rule {rule.RuleId} saved.", LtToastTone.Success);
        await ReadPostureAsync(Tenant).ConfigureAwait(true);
    }

    /// <summary>The loaded rules in each layer that pass the filters, memoised per (rules, tree, subject).</summary>
    private Filtered Filter()
    {
        var rules = _rules ?? NoRules;
        var subject = _subjectFilter.Trim();
        if (_filtered is { } filtered
            && ReferenceEquals(filtered.Source, rules)
            && filtered.Tree == _treeFilter
            && filtered.Subject == subject)
        {
            return filtered;
        }

        var platform = new List<TenantRuleView>();
        var tenant = new List<TenantRuleView>();
        foreach (var rule in rules)
        {
            if (!Passes(rule, _treeFilter, subject))
            {
                continue;
            }

            (rule.Layer == TenantRuleLayer.Platform ? platform : tenant).Add(rule);
        }

        filtered = new Filtered(rules, _treeFilter, subject, platform, tenant);
        _filtered = filtered;
        return filtered;
    }

    /// <summary>
    /// Whether <paramref name="rule"/> passes the filters. A tree filter keeps the
    /// rules that govern that tree, so every-tree rules are kept with each tree.
    /// </summary>
    private static bool Passes(TenantRuleView rule, string tree, string subject)
    {
        var treePasses = tree switch
        {
            AllTrees => true,
            TenantWideTrees => rule.ScopeKind == TenantRuleScopeKind.TenantWide,
            _ => rule.ScopeKind == TenantRuleScopeKind.TenantWide || string.Equals(rule.TreeName, tree, StringComparison.Ordinal),
        };

        return treePasses
            && (subject.Length == 0
                || (rule.SubjectId is { } id && id.Contains(subject, StringComparison.OrdinalIgnoreCase)));
    }

    /// <summary>The loaded rules split by layer under one pair of filters.</summary>
    private sealed record Filtered(
        List<TenantRuleView> Source,
        string Tree,
        string Subject,
        IReadOnlyList<TenantRuleView> Platform,
        IReadOnlyList<TenantRuleView> Tenant);
}
