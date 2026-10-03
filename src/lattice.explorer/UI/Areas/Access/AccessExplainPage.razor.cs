using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// The explain page (<c>/access/explain</c>): drives
/// <see cref="ILatticeAuthAdmin.ExplainAsync"/> and
/// <see cref="ILatticeAuthAdmin.EffectivePermissionsAsync"/> for a subject. The
/// question is kept in the address (<c>?subject=&amp;kind=&amp;operation=&amp;tree=</c>
/// plus <c>key</c> or <c>prefix</c>, and <c>view=permissions</c>), so an answer can
/// be linked to and is asked again when the page opens at that address. At a
/// tenant-rooted address whose access administration is delegated to the caller,
/// the tenant's layer-aware explain instead.
/// </summary>
public partial class AccessExplainPage
{
    private LatticeSubjectSelectorKind _kind = LatticeSubjectSelectorKind.User;
    private string _subject = string.Empty;
    private string _operation = LatticeOperation.Read.ToString().ToLowerInvariant();
    private string _scopeKind = AccessRuleDraft.TreeScope;
    private string _tree = string.Empty;
    private string _keyOrPrefix = string.Empty;
    private string? _subjectError;
    private string? _treeError;
    private string? _keyError;
    private bool _running;
    private AccessFailure? _failure;
    private AuthExplanation? _explanation;
    private AuthEffectivePermissions? _permissions;
    private IReadOnlyList<LatticeAuthorizationRule> _matched = [];
    private int _outOfScope;
    private AccessModelDescriptor? _model;
    private LtComboBox? _treeBox;
    private AccessSubjectPicker? _subjectPicker;
    private bool _asked;
    private readonly AccessTenantGate _gate = new();

    [Inject]
    internal AccessCatalog Catalog { get; set; } = default!;

    [Inject]
    internal TenantAccessCatalog TenantAccess { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    private static IReadOnlyList<LtSelectOption> OperationOptions { get; } =
        [.. AccessRuleFormat.Operations.Select(option => new LtSelectOption(option.Value, option.Label))];

    private static IReadOnlyList<LtSelectOption> ScopeOptions { get; } =
    [
        new(AccessRuleDraft.TreeScope, "Whole tree"),
        new(AccessRuleDraft.PrefixScope, "Key prefix in a tree"),
        new(AccessRuleDraft.KeyScope, "Single key in a tree"),
        new(AccessRuleDraft.ClusterScope, "The cluster (cluster-wide capabilities)"),
    ];

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (await _gate.ResolveAsync(TenantAccess, Address.Tenant).ConfigureAwait(true) || _asked)
        {
            // The tenant's layer-aware explain reads for itself; the question here is asked once.
            return;
        }

        _asked = true;
        _model = await Catalog.GetAccessModelAsync(CancellationToken.None).ConfigureAwait(true);
        ReadQuestion(Address);

        if (_subject.Length == 0)
        {
            return;
        }

        if (string.Equals(Address.GetQuery(AccessRoutes.ViewQuery), AccessRoutes.PermissionsView, StringComparison.Ordinal))
        {
            await RunPermissionsAsync(updateAddress: false).ConfigureAwait(true);
        }
        else if (Address.GetQuery(AccessRoutes.OperationQuery) is not null)
        {
            await RunExplainAsync(updateAddress: false).ConfigureAwait(true);
        }
    }

    private void ReadQuestion(ExplorerAddress address)
    {
        _subject = address.GetQuery(AccessRoutes.SubjectQuery) ?? string.Empty;
        _kind = string.Equals(address.GetQuery(AccessRoutes.KindQuery), "group", StringComparison.Ordinal)
            ? LatticeSubjectSelectorKind.Group
            : LatticeSubjectSelectorKind.User;

        var operation = address.GetQuery(AccessRoutes.OperationQuery);
        if (operation is not null && AccessRuleFormat.Operations.Any(option => option.Value == operation))
        {
            _operation = operation;
        }

        var tree = address.GetQuery(AccessRoutes.TreeQuery);
        var key = address.GetQuery(ExplorerAddress.KeyQuery);
        var prefix = address.GetQuery(ExplorerAddress.PrefixQuery);
        if (!string.IsNullOrEmpty(tree))
        {
            _tree = tree;
            (_scopeKind, _keyOrPrefix) = key is { Length: > 0 } ? (AccessRuleDraft.KeyScope, key)
                : prefix is { Length: > 0 } ? (AccessRuleDraft.PrefixScope, prefix)
                : (AccessRuleDraft.TreeScope, string.Empty);
        }
        else if (operation is not null && AccessRuleFormat.Operations.Any(option => option.Value == operation && option.IsScopeless))
        {
            _scopeKind = AccessRuleDraft.ClusterScope;
        }
    }

    /// <summary>
    /// Which layer, and which rule, decided <paramref name="explanation"/>, as the
    /// cluster reports it: a platform (operator) rule, a tenant's rule, or the
    /// default when a rule-level decision was made and none matched. <see langword="null"/>
    /// when the cluster reports no deciding layer, as one that predates the tenant
    /// tier, or a per-key collection verdict, does.
    /// </summary>
    /// <param name="explanation">The explanation.</param>
    /// <returns>The sentence, or <see langword="null"/>.</returns>
    internal static string? DecidedBy(AuthExplanation explanation)
    {
        ArgumentNullException.ThrowIfNull(explanation);
        return (explanation.DecidingLayer, explanation.DecidingRuleId) switch
        {
            (TenantRuleLayer.Platform, { } rule) => $"Platform rule {rule}",
            (TenantRuleLayer.Tenant, { } rule) => $"Tenant rule {rule}, because no platform rule matched",
            (TenantRuleLayer.Platform, null) => "The platform layer",
            (TenantRuleLayer.Tenant, null) => "The tenant layer, because no platform rule matched",
            _ => null,
        };
    }

    private Task ExplainAsync() => RunExplainAsync(updateAddress: true);

    private Task EffectivePermissionsAsync() => RunPermissionsAsync(updateAddress: true);

    private async Task RunExplainAsync(bool updateAddress)
    {
        if (!TryReadScope(out var scope) | !ValidateSubject() || (updateAddress && !await ConfirmPickersAsync(checkTree: _scopeKind != AccessRuleDraft.ClusterScope).ConfigureAwait(true)))
        {
            return;
        }

        var operation = AccessRuleFormat.Operations.First(option => option.Value == _operation).Flag;
        await RunAsync(async () =>
        {
            var explanation = await Catalog.Admin.ExplainAsync(_subject.Trim(), operation, scope!, _kind).ConfigureAwait(true);
            _explanation = explanation;
            _matched = AccessRuleFormat.InPrecedenceOrder(explanation.MatchedRules);
        }).ConfigureAwait(true);

        if (updateAddress)
        {
            UpdateAddress(permissions: false);
        }
    }

    private async Task RunPermissionsAsync(bool updateAddress)
    {
        if (!ValidateSubject() || (updateAddress && !await ConfirmPickersAsync(checkTree: false).ConfigureAwait(true)))
        {
            return;
        }

        await RunAsync(async () =>
        {
            var permissions = await Catalog.Admin.EffectivePermissionsAsync(_subject.Trim(), _kind).ConfigureAwait(true);
            _permissions = permissions;

            // At a tenant-rooted address only the tenant's own rules are listed; the
            // cluster-wide ones that also apply are counted, and every other
            // tenant's are left out.
            var scope = Address.Tenant;
            _matched = AccessRuleFormat.InPrecedenceOrder([.. permissions.Rules.Where(rule => AccessCatalog.Lists(scope, rule.Scope.TreeId))]);
            _outOfScope = scope is null ? 0 : permissions.Rules.Count(rule => AccessRuleFormat.IsClusterWide(rule.Scope));
        }).ConfigureAwait(true);

        if (updateAddress)
        {
            UpdateAddress(permissions: true);
        }
    }

    private async Task<bool> ConfirmPickersAsync(bool checkTree)
    {
        var tree = !checkTree || _treeBox is null || await _treeBox.ConfirmAsync().ConfigureAwait(true);
        var subject = _subjectPicker is null || await _subjectPicker.ConfirmAsync().ConfigureAwait(true);
        return tree && subject;
    }

    private async Task RunAsync(Func<Task> ask)
    {
        _running = true;
        _failure = null;
        _explanation = null;
        _permissions = null;
        _matched = [];
        _outOfScope = 0;
        try
        {
            await ask().ConfigureAwait(true);
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            _failure = failure;
        }
        finally
        {
            _running = false;
        }
    }

    private bool ValidateSubject()
    {
        _subjectError = _subject.Trim().Length == 0 ? "Choose the user or group to explain." : null;
        return _subjectError is null;
    }

    private bool TryReadScope(out LatticeScope? scope)
    {
        scope = null;
        _treeError = null;
        _keyError = null;
        if (_scopeKind == AccessRuleDraft.ClusterScope)
        {
            scope = LatticeScope.ClusterWide();
            return true;
        }

        var tree = _tree.Trim();
        if (tree.Length == 0)
        {
            _treeError = "Enter the tree to ask about.";
            return false;
        }

        if (_scopeKind is AccessRuleDraft.KeyScope or AccessRuleDraft.PrefixScope && _keyOrPrefix.Length == 0)
        {
            _keyError = _scopeKind == AccessRuleDraft.KeyScope ? "Enter the key." : "Enter the prefix.";
            return false;
        }

        scope = _scopeKind switch
        {
            AccessRuleDraft.KeyScope => LatticeScope.Key(tree, _keyOrPrefix),
            AccessRuleDraft.PrefixScope => LatticeScope.Prefix(tree, _keyOrPrefix),
            _ => LatticeScope.Tree(tree),
        };
        return true;
    }

    private void UpdateAddress(bool permissions)
    {
        var address = AccessRoutes.Explain
            .WithQuery(AccessRoutes.SubjectQuery, _subject.Trim())
            .WithQuery(AccessRoutes.KindQuery, AccessRuleFormat.SubjectKindLabel(_kind));
        if (permissions)
        {
            address = address.WithQuery(AccessRoutes.ViewQuery, AccessRoutes.PermissionsView);
        }
        else
        {
            address = address.WithQuery(AccessRoutes.OperationQuery, _operation);
            if (_scopeKind != AccessRuleDraft.ClusterScope)
            {
                address = address.WithQuery(AccessRoutes.TreeQuery, _tree.Trim());
                if (_scopeKind == AccessRuleDraft.KeyScope)
                {
                    address = address.WithQuery(ExplorerAddress.KeyQuery, _keyOrPrefix);
                }
                else if (_scopeKind == AccessRuleDraft.PrefixScope)
                {
                    address = address.WithQuery(ExplorerAddress.PrefixQuery, _keyOrPrefix);
                }
            }
        }

        Navigator.NavigateTo(Navigator.Canonicalize(address.WithTenant(Address.Tenant)), replace: true);
    }

    private string OutOfScopeText => _outOfScope == 1 ? "1 cluster-wide rule also applies." : $"{_outOfScope} cluster-wide rules also apply.";

    private string ClusterWidePermissionsHref => Navigator.Canonicalize(Address.WithTenant(null)).ToHref();

    private string GroupHref(string groupId) => Navigator.Canonicalize(AccessRoutes.Group(groupId)).ToHref();
}
