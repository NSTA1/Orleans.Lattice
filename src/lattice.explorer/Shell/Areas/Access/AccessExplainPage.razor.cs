using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Access;

/// <summary>
/// The explain page (<c>/access/explain</c>): drives
/// <see cref="ILatticeAuthAdmin.ExplainAsync"/> and
/// <see cref="ILatticeAuthAdmin.EffectivePermissionsAsync"/> for a subject. The
/// question is kept in the address (<c>?subject=&amp;kind=&amp;operation=&amp;tree=</c>
/// plus <c>key</c> or <c>prefix</c>, and <c>view=permissions</c>), so an answer can
/// be linked to and is asked again when the page opens at that address.
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
    private AccessModelDescriptor? _model;

    [Inject]
    internal AccessCatalog Catalog { get; set; } = default!;

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
    protected override async Task OnInitializedAsync()
    {
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

    private Task ExplainAsync() => RunExplainAsync(updateAddress: true);

    private Task EffectivePermissionsAsync() => RunPermissionsAsync(updateAddress: true);

    private async Task RunExplainAsync(bool updateAddress)
    {
        if (!TryReadScope(out var scope) | !ValidateSubject())
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
        if (!ValidateSubject())
        {
            return;
        }

        await RunAsync(async () =>
        {
            var permissions = await Catalog.Admin.EffectivePermissionsAsync(_subject.Trim(), _kind).ConfigureAwait(true);
            _permissions = permissions;
            _matched = AccessRuleFormat.InPrecedenceOrder(permissions.Rules);
        }).ConfigureAwait(true);

        if (updateAddress)
        {
            UpdateAddress(permissions: true);
        }
    }

    private async Task RunAsync(Func<Task> ask)
    {
        _running = true;
        _failure = null;
        _explanation = null;
        _permissions = null;
        _matched = [];
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

        Navigator.NavigateTo(Navigator.Canonicalize(address), replace: true);
    }

    private string GroupHref(string groupId) => Navigator.Canonicalize(AccessRoutes.Group(groupId)).ToHref();
}
