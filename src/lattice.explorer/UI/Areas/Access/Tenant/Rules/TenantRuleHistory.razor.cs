using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Rules;

/// <summary>
/// A tenant rule's revisions in the cluster's policy store (decision D19: audit
/// uses what exists), read with the Explorer's <see cref="HistoryReader"/> and
/// laid out by <see cref="HistoryTimeline"/>, newest first. The rule is found
/// under the key the policy store files it by: its governed tree, then its full
/// tenant-tier id.
/// </summary>
/// <remarks>
/// The policy store is a reserved tree, which only platform operators may read.
/// When the state API is not served, or the read is refused, the section says so
/// rather than hiding.
/// </remarks>
public partial class TenantRuleHistory : IDisposable
{
    /// <summary>How many revisions one read asks for.</summary>
    internal const int PageSize = 50;

    /// <summary>What the section says when this Explorer has no state API.</summary>
    internal const string NotServedText = "This Explorer does not read history, so the rule's revisions cannot be shown here.";

    /// <summary>What the section says when the policy store's history could not be read.</summary>
    internal const string UnreadableText = "The rule's revisions could not be read. They are kept in the cluster's policy store, which platform operators may read.";

    /// <summary>The separator between a rule's governed tree and its id in the policy store's keys.</summary>
    private const string RuleKeySeparator = "\u001f";

    private readonly ComponentLifetime _lifetime = new();
    private readonly string _headingId = LtIds.Next("lt-tenant-rule-history");
    private readonly List<HistoryRevisionRow> _rows = [];
    private (string Tenant, string RuleId)? _loaded;
    private HistoryTimeline? _timeline;
    private string? _continuation;
    private string? _unavailable;
    private bool _loading;

    /// <summary>The tenant whose rule it is.</summary>
    [Parameter]
    [EditorRequired]
    public string Tenant { get; set; } = string.Empty;

    /// <summary>The tenant rule whose history to show.</summary>
    [Parameter]
    [EditorRequired]
    public TenantRuleView Rule { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    private string State => _unavailable is not null ? "unavailable" : _timeline is null ? "loading" : "loaded";

    /// <summary>
    /// The policy store's key of <paramref name="rule"/>: the physical tree it
    /// governs (the tenant-wide sentinel for every tree), the separator, and its
    /// full <c>tenant:{tenant}:{id}</c> id. <see langword="null"/> when the tenant
    /// or the rule cannot name one.
    /// </summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="rule">The tenant rule.</param>
    /// <returns>The key, or <see langword="null"/>.</returns>
    internal static string? PolicyKey(string tenant, TenantRuleView rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        if (!TenantId.TryParse(tenant, out var id) || id.IsDefault || string.IsNullOrEmpty(rule.RuleId))
        {
            return null;
        }

        string tree;
        if (rule.ScopeKind == TenantRuleScopeKind.TenantWide)
        {
            tree = LatticeScope.TenantWide(id).TreeId;
        }
        else if (!string.IsNullOrEmpty(rule.TreeName))
        {
            tree = LatticeTenantTrees.Compose(id, rule.TreeName);
        }
        else
        {
            return null;
        }

        return string.Concat(tree, RuleKeySeparator, LatticeTenantRuleIds.For(id, rule.RuleId));
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (_loaded is { } loaded && loaded.Tenant == Tenant && loaded.RuleId == Rule.RuleId)
        {
            return;
        }

        _loaded = (Tenant, Rule.RuleId);
        _rows.Clear();
        _continuation = null;
        _timeline = null;
        _unavailable = null;
        await LoadPageAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

    private Task LoadMoreAsync() => LoadPageAsync();

    private async Task LoadPageAsync()
    {
        if (PolicyKey(Tenant, Rule) is not { } key)
        {
            _unavailable = UnreadableText;
            return;
        }

        if (DataServices.Find<ILatticeStateClient>(Services) is not { } client)
        {
            _unavailable = NotServedText;
            return;
        }

        _loading = true;
        try
        {
            var page = await new HistoryReader(client)
                .LoadAsync(LatticeAuthReservedTrees.PolicyTreeId, key, PageSize, _continuation, _lifetime.Token)
                .ConfigureAwait(true);
            if (_lifetime.IsLeft)
            {
                return;
            }

            _rows.AddRange(page.Revisions);
            _continuation = string.IsNullOrEmpty(page.ContinuationToken) ? null : page.ContinuationToken;
            _timeline = HistoryTimeline.Build(
                LatticeAuthReservedTrees.PolicyTreeId,
                key,
                page.Status,
                _rows,
                page.Bound,
                page.EarliestAvailable,
                _continuation,
                newestFirst: true);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _unavailable = UnreadableText;
        }
        finally
        {
            _loading = false;
        }
    }
}
