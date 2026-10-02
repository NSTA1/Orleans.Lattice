using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// One of the tenant's own groups (<c>/t/{tenant}/access/groups/{name}</c>): its
/// display name; its direct members, each with its kind and source, added through
/// the tenant-aware subject picker and removed with a confirmation, a nesting
/// refusal (D3) shown with its typed reason; "Add me" for a tenant administrator
/// who is not a direct member (issue #4150); the rules and app role bindings that
/// use it; its history (D19); and delete with its cascade previewed. A name the
/// tenant has no group of is not found.
/// </summary>
public partial class TenantGroupDetailView
{
    private TenantGroupDescriptor? _group;
    private IReadOnlyList<TenantGroupMember>? _members;
    private IReadOnlyList<TenantRuleView>? _references;
    private TenantAccessPosture? _posture;
    private AccessModelDescriptor? _model;
    private AccessFailure? _failure;
    private (string Tenant, string Name)? _loaded;
    private string _displayName = string.Empty;
    private string? _displayNameError;
    private TenantSubjectKind _memberKind = TenantSubjectKind.User;
    private string _memberId = string.Empty;
    private string? _memberError;
    private TenantGroupMember? _removing;
    private bool _deleteOpen;
    private bool _busy;
    private AccessSubjectPicker? _picker;

    /// <summary>The group's tenant-local name.</summary>
    [Parameter]
    [EditorRequired]
    public string Name { get; set; } = string.Empty;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    [Inject]
    internal AccessCatalog Catalog { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    private string FullId => AccessSubjectPicker.TenantGroupId(Tenant, Name);

    private bool IsTenantAdmin => _posture?.CallerIsTenantAdmin == true;

    private bool IsOperatorOnly => _posture is { CallerIsPlatformOperator: true, CallerIsTenantAdmin: false };

    private string CallerValue => IsTenantAdmin ? "tenant-admin" : IsOperatorOnly ? "operator" : "unknown";

    private bool EdgesAtCap => _posture is { } posture && TenantGroupFormat.AtCap(posture.MembershipEdges);

    /// <summary>
    /// The caller's own subject id, when the sign-in names it: only a Basic sign-in
    /// names the subject the cluster authenticates; a token sign-in shows a display name.
    /// </summary>
    private string? CallerSubject
    {
        get
        {
            var caller = ShellCaller.Of(Services).Current;
            return caller.Authenticated
                && string.Equals(caller.Scheme, ExplorerAuthSchemes.Basic, StringComparison.Ordinal)
                && !string.IsNullOrWhiteSpace(caller.User)
                    ? caller.User
                    : null;
        }
    }

    private bool CanAddMe => IsTenantAdmin && _members is { } members && CallerSubject is { } subject && !IsDirectUser(members, subject);

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (_loaded is { } loaded && loaded.Tenant == Tenant && loaded.Name == Name)
        {
            return;
        }

        _loaded = (Tenant, Name);
        _deleteOpen = false;
        _removing = null;
        _model ??= await Catalog.GetAccessModelAsync(Lifetime.Token).ConfigureAwait(true);
        await LoadAsync().ConfigureAwait(true);
    }

    /// <summary>The source a rule that names the group comes from.</summary>
    /// <param name="rule">The rule.</param>
    /// <returns>The label.</returns>
    internal static string SourceLabel(TenantRuleView rule) => rule.Origin switch
    {
        TenantRuleOrigin.Tenant => "Tenant rule",
        TenantRuleOrigin.AppRole => "App role binding",
        TenantRuleOrigin.PlatformWide => "Platform, all trees",
        _ => "Platform",
    };

    /// <summary>What a rule that names the group governs, in the tenant's own tree names.</summary>
    /// <param name="rule">The rule.</param>
    /// <returns>The label.</returns>
    internal static string ScopeLabel(TenantRuleView rule) => rule.ScopeKind switch
    {
        TenantRuleScopeKind.TenantWide => "All of the tenant's trees",
        _ when rule.TreeName is null => "Not shown",
        TenantRuleScopeKind.Key => $"Key {rule.KeyOrPrefix} in {rule.TreeName}",
        TenantRuleScopeKind.Prefix => $"Prefix {rule.KeyOrPrefix} in {rule.TreeName}",
        _ => $"Tree {rule.TreeName}",
    };

    private static bool IsDirectUser(IReadOnlyList<TenantGroupMember> members, string subject)
    {
        for (var i = 0; i < members.Count; i++)
        {
            if (members[i].Kind == TenantSubjectKind.User && string.Equals(members[i].MemberId, subject, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static string MemberKey(TenantGroupMember member) => TenantGroupFormat.KindValue(member.Kind) + ":" + member.MemberId;

    private string GroupHref(string name) => Href(AccessRoutes.TenantGroup(Tenant, name));

    private string? RuleHref(TenantRuleView rule) =>
        rule.Origin == TenantRuleOrigin.Tenant ? Href(AccessRoutes.TenantRule(Tenant, rule.RuleId)) : null;

    private async Task LoadAsync()
    {
        var (tenant, name) = (Tenant, Name);
        _failure = null;
        _group = null;
        _members = null;
        _references = null;
        try
        {
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant groups.");
            var group = await directory.GetGroupAsync(tenant, name, Lifetime.Token).ConfigureAwait(true);
            if (Lifetime.IsLeft || tenant != Tenant || name != Name)
            {
                return;
            }

            if (group is null)
            {
                Navigation.NotFound();
                return;
            }

            var state = await Access.GetStateAsync(tenant, Lifetime.Token).ConfigureAwait(true);
            var members = await directory.ListGroupMembersAsync(tenant, name, Lifetime.Token).ConfigureAwait(true);
            var references = await TenantGroupUsage.ReadReferencesAsync(Access, tenant, name, Lifetime.Token).ConfigureAwait(true);
            if (Lifetime.IsLeft || tenant != Tenant || name != Name)
            {
                return;
            }

            _group = group;
            _displayName = group.DisplayName ?? string.Empty;
            _posture = state.Posture;
            _members = members ?? [];
            _references = references;
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
        }
        catch (ArgumentException exception) when (!Lifetime.IsLeft && exception is not TenantAccessConfinementException)
        {
            // A name that is no valid local group name names no group.
            Navigation.NotFound();
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            _failure = failure;
        }
    }

    private async Task SaveDisplayNameAsync()
    {
        if (_group is null || _busy)
        {
            return;
        }

        var tenant = Tenant;
        _busy = true;
        _displayNameError = null;
        try
        {
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant groups.");
            var name = _displayName.Trim();
            var updated = await directory.UpsertGroupAsync(tenant, _group with { DisplayName = name.Length == 0 ? null : name }, Lifetime.Token).ConfigureAwait(true);
            _group = updated;
            Access.Invalidate();
            Toasts.Show($"Group {updated.Name} renamed.", LtToastTone.Success);
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            _displayNameError = failure.Message;
        }
        finally
        {
            _busy = false;
        }
    }

    private async Task AddMemberAsync()
    {
        if (_group is null || _members is null || _busy || EdgesAtCap)
        {
            return;
        }

        _memberError = null;
        var memberId = _memberId.Trim();
        if (memberId.Length == 0)
        {
            _memberError = "Choose the member.";
            return;
        }

        if (_memberKind == TenantSubjectKind.TenantGroup && string.Equals(memberId, Name, StringComparison.Ordinal))
        {
            _memberError = "A group cannot be a member of itself.";
            return;
        }

        if (_picker is { } picker && !await picker.ConfirmAsync().ConfigureAwait(true))
        {
            return;
        }

        if (await AddAsync(memberId, _memberKind, error => _memberError = error).ConfigureAwait(true))
        {
            _memberId = string.Empty;
        }
    }

    private async Task AddMeAsync()
    {
        if (CallerSubject is not { } subject || _busy || EdgesAtCap)
        {
            return;
        }

        await AddAsync(subject, TenantSubjectKind.User, error => Toasts.Show(error, LtToastTone.Danger)).ConfigureAwait(true);
    }

    /// <summary>Adds a direct member, reporting a refusal through <paramref name="refused"/>.</summary>
    /// <returns><see langword="true"/> when the member was added.</returns>
    private async Task<bool> AddAsync(string memberId, TenantSubjectKind kind, Action<string> refused)
    {
        var (tenant, name) = (Tenant, Name);
        _busy = true;
        try
        {
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant groups.");
            await directory.AddGroupMemberAsync(tenant, name, memberId, kind, Lifetime.Token).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
            return false;
        }
        catch (LatticeQuotaExceededException)
        {
            Access.Invalidate();
            refused($"Tenant {tenant} is at its cap of membership entries, so {memberId} was not added.");
            return false;
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            refused(TenantGroupFormat.RefusalMessage(exception, failure));
            return false;
        }
        finally
        {
            _busy = false;
        }

        Access.Invalidate();
        if (Lifetime.IsLeft || tenant != Tenant || name != Name || _members is null)
        {
            return true;
        }

        var member = new TenantGroupMember { MemberId = memberId, Kind = kind };
        if (!_members.Contains(member))
        {
            // A new list, not an in-place change: the table re-reads its rows only when its items change.
            _members = [.. _members.Append(member).OrderBy(entry => entry.MemberId, StringComparer.Ordinal)];
        }

        Toasts.Show($"{memberId} added to {name}.", LtToastTone.Success);
        return true;
    }

    private async Task RemoveMemberAsync()
    {
        var member = _removing;
        _removing = null;
        if (_group is null || _members is null || member is null)
        {
            return;
        }

        var (tenant, name) = (Tenant, Name);
        _busy = true;
        try
        {
            var directory = Access.Directory ?? throw new NotSupportedException("This Explorer does not serve tenant groups.");
            await directory.RemoveGroupMemberAsync(tenant, name, member.MemberId, member.Kind, Lifetime.Token).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
            return;
        }
        catch (Exception exception) when (TenantAccessFailure.From(exception, tenant) is { } failure)
        {
            Toasts.Show(failure.Message, LtToastTone.Danger);
            return;
        }
        finally
        {
            _busy = false;
        }

        Access.Invalidate();
        _members = [.. _members.Where(entry => entry != member)];
        Toasts.Show($"{member.MemberId} removed from {name}.", LtToastTone.Success);
    }

    private void OnDeleted(TenantGroupRemovalResult result)
    {
        _deleteOpen = false;
        Navigator.NavigateTo(Navigator.Canonicalize(AccessRoutes.TenantGroups(result.TenantId)));
    }
}
