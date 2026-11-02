using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// Picks a subject - a user or a group - by id, as a type-ahead combobox over the
/// cluster's identity directory and stored groups. The sources are searched as the
/// id is typed (debounced by input, never on a timer), with a directory only a
/// listed principal is accepted, and a principal's display name is always rendered
/// as text. Without a directory a user id is used as typed.
/// </summary>
/// <remarks>
/// <para>
/// Cluster-wide, a user comes from the identity directory alone, and a group from
/// two labelled sources in one list (<see cref="ClusterGroupSuggestionSource"/>):
/// the directory's groups and the groups stored in the Access area itself, so a
/// group created in Access can be chosen once a directory is configured. Where
/// <see cref="AllowTenantGroups"/> is set, a platform operator - and only one - may
/// also choose a <c>Tenant group</c> by tenant and name: it resolves to its full id
/// <c>t/{tenant}/{name}</c> and is verified against that tenant's stored groups
/// through the tenant directory facade before a submit acts. A tenant group id is
/// never typed into existence: one typed into the user or group source is refused.
/// </para>
/// <para>
/// On a tenant page (<see cref="Tenant"/> set) the picker offers three labelled
/// sources, chosen by <see cref="SubjectKind"/>: the tenant's own groups
/// (<c>Tenant</c>, by tenant-local name, from the tenant directory), and the
/// cluster's groups and users (<c>Cluster</c>, through the identity-directory
/// search). Another tenant's group is never offered: the tenant directory lists
/// only the named tenant's groups, the cluster sources leave out every reserved
/// tenant group id, and a typed one is refused. A chosen group's provenance is
/// shown with its full id.
/// </para>
/// </remarks>
public partial class AccessSubjectPicker
{
    /// <summary>The kind value of a user.</summary>
    internal const string UserValue = "user";

    /// <summary>The kind value of a cluster group.</summary>
    internal const string GroupValue = "group";

    /// <summary>The kind value of a cluster group on a tenant page.</summary>
    internal const string ClusterGroupValue = "cluster-group";

    /// <summary>The kind value of a tenant's own group: on a tenant page that tenant's, cluster-wide any tenant's, chosen by an operator.</summary>
    internal const string TenantGroupValue = "tenant-group";

    /// <summary>The provenance label of a tenant group chosen cluster-wide.</summary>
    internal const string TenantGroupLabel = "Tenant group";

    /// <summary>The sentence a reserved tenant group id typed into a cluster source is refused with.</summary>
    internal const string ForeignTenantGroupMessage = "A tenant group is chosen as a Tenant group, by its name. Another tenant's group can never be chosen.";

    /// <summary>The sentence a reserved tenant group id typed into a cluster-wide user or group source is refused with.</summary>
    internal const string TypedTenantGroupMessage = "A tenant group id cannot be typed here. A platform operator chooses one as a Tenant group, by its tenant and name.";

    /// <summary>The sentence a tenant group is refused with for a caller who is not a platform operator.</summary>
    internal const string OperatorOnlyMessage = "Only a platform operator can choose a tenant group.";

    /// <summary>The sentence a tenant group is refused with where this picker does not offer one.</summary>
    internal const string TenantGroupNotOfferedMessage = "A tenant group cannot be chosen here.";

    /// <summary>The sentence a tenant group with only one of its tenant and name is refused with.</summary>
    internal const string ChooseTenantAndGroupMessage = "Choose the tenant, then one of its groups.";

    /// <summary>The sentence a tenant group is refused with while delegated tenant access administration is off.</summary>
    internal const string TenantGroupsOffMessage = "Delegated tenant access administration is off, so no tenant group can be chosen.";

    /// <summary>The id field's hint when the cluster has no identity directory to search.</summary>
    internal const string NoDirectoryHint = "No identity directory is configured, so the id is used as typed and is not validated.";

    /// <summary>The id field's hint for a user when the caller may not search the cluster's identity directory.</summary>
    internal const string DeniedUserHint = "You can enter a user id. Searching the cluster directory needs operator access.";

    /// <summary>The id field's hint for a group when the caller may not search the cluster's identity directory.</summary>
    internal const string DeniedGroupHint = "You can enter a group id. Searching the cluster directory needs operator access.";

    private LtComboBox? _box;
    private string? _sourceError;
    private TenantGroupSuggestionSource? _tenantGroups;
    private ILtSuggestionSource? _clusterGroups;
    private ILtSuggestionSource? _clusterUsers;
    private string? _emittedId;
    private bool _clusterTenantGroup;
    private string _groupTenant = string.Empty;
    private string _groupName = string.Empty;
    private bool _isOperator;
    private (string Value, string Label)? _chosen;

    /// <summary>The field's label, such as <c>Subject</c> or <c>Member</c>.</summary>
    [Parameter]
    public string Label { get; set; } = "Subject";

    /// <summary>The subject kind.</summary>
    [Parameter]
    public LatticeSubjectSelectorKind Kind { get; set; } = LatticeSubjectSelectorKind.User;

    /// <summary>Raised when the kind changes; the id is cleared with it.</summary>
    [Parameter]
    public EventCallback<LatticeSubjectSelectorKind> KindChanged { get; set; }

    /// <summary>Whether the kind can be chosen; when not, it is fixed to <see cref="Kind"/> (or, on a tenant page, <see cref="SubjectKind"/>).</summary>
    [Parameter]
    public bool AllowKindChange { get; set; } = true;

    /// <summary>
    /// The subject id. For one of the tenant's own groups on a tenant page it is the
    /// group's tenant-local name; for a tenant group chosen cluster-wide it is the
    /// full id <c>t/{tenant}/{name}</c>, with <see cref="Kind"/> a group.
    /// </summary>
    [Parameter]
    public string? Id { get; set; }

    /// <summary>Raised when the id changes.</summary>
    [Parameter]
    public EventCallback<string> IdChanged { get; set; }

    /// <summary>The id field's error, such as a directory validation failure.</summary>
    [Parameter]
    public string? Error { get; set; }

    /// <summary>Whether the cluster has an identity directory to search.</summary>
    [Parameter]
    public bool DirectoryAvailable { get; set; }

    /// <summary>
    /// Whether the cluster refused this caller the identity directory, so whether
    /// one is configured is unknown. Read only when <see cref="DirectoryAvailable"/>
    /// is <see langword="false"/>: the id is still used as typed, but the field
    /// says searching needs operator access rather than that no directory exists.
    /// </summary>
    [Parameter]
    public bool DirectorySearchDenied { get; set; }

    /// <summary>The directory's one-line explanation of what a valid id looks like.</summary>
    [Parameter]
    public string? DirectoryExplanation { get; set; }

    /// <summary>Whether the picker is disabled.</summary>
    [Parameter]
    public bool Disabled { get; set; }

    /// <summary>Raised after a directory match is chosen, with the principal, so a form can use its display name.</summary>
    [Parameter]
    public EventCallback<DirectoryPrincipalDescriptor> OnPrincipalSelected { get; set; }

    /// <summary>
    /// The tenant whose page the picker is on, or <see langword="null"/> for a
    /// cluster-wide picker. When set, the picker offers the tenant's own groups
    /// beside the cluster's groups and users, chosen by <see cref="SubjectKind"/>.
    /// </summary>
    [Parameter]
    public string? Tenant { get; set; }

    /// <summary>On a tenant page, which source the subject is chosen from: a user, a cluster group, or one of the tenant's own groups.</summary>
    [Parameter]
    public TenantSubjectKind SubjectKind { get; set; } = TenantSubjectKind.User;

    /// <summary>Raised on a tenant page when <see cref="SubjectKind"/> changes; the id is cleared with it.</summary>
    [Parameter]
    public EventCallback<TenantSubjectKind> SubjectKindChanged { get; set; }

    /// <summary>
    /// Cluster-wide only: whether a platform operator may choose any tenant's group
    /// as a <c>Tenant group</c>, by tenant and name. Off (the default) for a field
    /// where a tenant group can never stand, such as a cluster group's members. The
    /// choice is offered only once the caller is proved an operator, and a submit
    /// asks again.
    /// </summary>
    [Parameter]
    public bool AllowTenantGroups { get; set; }

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [Inject]
    internal TenantAccessCatalog TenantAccess { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    private static IReadOnlyList<LtSelectOption> KindOptions { get; } =
    [
        new(UserValue, "User"),
        new(GroupValue, "Group"),
    ];

    private static IReadOnlyList<LtSelectOption> OperatorKindOptions { get; } =
    [
        new(UserValue, "User"),
        new(GroupValue, "Group"),
        new(TenantGroupValue, TenantGroupLabel),
    ];

    private static IReadOnlyList<LtSelectOption> TenantKindOptions { get; } =
    [
        new(TenantGroupValue, "Tenant group"),
        new(ClusterGroupValue, "Cluster group"),
        new(UserValue, "Cluster user"),
    ];

    /// <summary>Whether the picker is on a tenant page, offering the tenant's own groups.</summary>
    private bool IsTenantMode => !string.IsNullOrEmpty(Tenant);

    private bool IsTenantGroup => IsTenantMode && SubjectKind == TenantSubjectKind.TenantGroup;

    /// <summary>Whether a cluster-wide picker is choosing a tenant's group by tenant and name.</summary>
    private bool IsClusterTenantGroup => !IsTenantMode && _clusterTenantGroup;

    /// <summary>
    /// Whether the Tenant group kind is offered: to a proved operator where it is
    /// allowed, and kept for a value that already is one so it can be seen (a
    /// submit by anyone else is refused).
    /// </summary>
    private bool OffersTenantGroups => AllowTenantGroups && (_isOperator || _clusterTenantGroup);

    private IReadOnlyList<LtSelectOption> Options => IsTenantMode ? TenantKindOptions : OffersTenantGroups ? OperatorKindOptions : KindOptions;

    private ILtSuggestionSource? Source
    {
        get
        {
            if (!IsTenantMode)
            {
                if (IsClusterTenantGroup)
                {
                    return GroupsOf(_groupTenant.Trim());
                }

                return Kind == LatticeSubjectSelectorKind.Group
                    ? Suggestions.ClusterGroups
                    : DirectoryAvailable ? Suggestions.Users : null;
            }

            if (IsTenantGroup)
            {
                return GroupsOf(Tenant!);
            }

            if (!DirectoryAvailable)
            {
                return null;
            }

            return SubjectKind == TenantSubjectKind.ClusterGroup
                ? _clusterGroups ??= new ClusterSubjectSuggestionSource(Suggestions.Groups)
                : _clusterUsers ??= new ClusterSubjectSuggestionSource(Suggestions.Users);
        }
    }

    private LtComboBoxMode Mode => IsTenantGroup || IsClusterTenantGroup || DirectoryAvailable ? LtComboBoxMode.PickExisting : LtComboBoxMode.Suggest;

    private string KindValue => IsTenantMode
        ? SubjectKind switch
        {
            TenantSubjectKind.TenantGroup => TenantGroupValue,
            TenantSubjectKind.ClusterGroup => ClusterGroupValue,
            _ => UserValue,
        }
        : _clusterTenantGroup ? TenantGroupValue : AccessRuleFormat.SubjectKindLabel(Kind);

    private string KindWord => IsTenantGroup || IsClusterTenantGroup ? "tenant group" : SelectorKind == LatticeSubjectSelectorKind.Group ? "group" : "user";

    private LatticeSubjectSelectorKind SelectorKind => IsTenantMode
        ? SubjectKind == TenantSubjectKind.User ? LatticeSubjectSelectorKind.User : LatticeSubjectSelectorKind.Group
        : Kind;

    /// <summary>The combobox's value: the tenant-local name of a tenant group chosen cluster-wide, otherwise the id.</summary>
    private string? BoxValue => IsClusterTenantGroup ? _groupName : Id;

    private string IdHint => IsTenantGroup
        ? $"Type part of a name to choose one of tenant {Tenant}'s own groups."
        : IsClusterTenantGroup
            ? _groupTenant.Trim().Length == 0
                ? ChooseTenantAndGroupMessage
                : $"Type part of a name to choose one of tenant {_groupTenant.Trim()}'s groups. A rule naming it may govern only tenant {_groupTenant.Trim()}'s own trees."
            : DirectoryAvailable
                ? (string.IsNullOrWhiteSpace(DirectoryExplanation) ? "Type part of a name or id to search the directory." : DirectoryExplanation!)
                : DirectorySearchDenied
                    ? SelectorKind == LatticeSubjectSelectorKind.Group ? DeniedGroupHint : DeniedUserHint
                    : NoDirectoryHint;

    /// <summary>
    /// The chosen subject's provenance and full id, with its <c>data-lt-provenance</c>
    /// value, or <see langword="null"/> when no group is chosen or its source is unknown.
    /// </summary>
    private (string Source, string FullId, string Kind)? Provenance
    {
        get
        {
            if (IsClusterTenantGroup)
            {
                var tenant = _groupTenant.Trim();
                var name = _groupName.Trim();
                return tenant.Length == 0 || name.Length == 0 ? null : (TenantGroupLabel, TenantGroupId(tenant, name), "tenant");
            }

            if (string.IsNullOrWhiteSpace(Id))
            {
                return null;
            }

            var id = Id.Trim();
            if (!IsTenantMode)
            {
                return Kind == LatticeSubjectSelectorKind.Group && _chosen is { } chosen && string.Equals(chosen.Value, id, StringComparison.Ordinal)
                    ? (chosen.Label, id, ProvenanceKind(chosen.Label))
                    : null;
            }

            if (SubjectKind == TenantSubjectKind.User)
            {
                return null;
            }

            return SubjectKind == TenantSubjectKind.TenantGroup
                ? (TenantGroupSuggestionSource.SourceLabel, TenantGroupId(Tenant!, id), "tenant")
                : (ClusterSubjectSuggestionSource.SourceLabel, id, "cluster");
        }
    }

    /// <summary>The full id of the tenant group <paramref name="name"/> of <paramref name="tenant"/>: <c>t/{tenant}/{name}</c>.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="name">The group's tenant-local name.</param>
    /// <returns>The full id.</returns>
    internal static string TenantGroupId(string tenant, string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenant);
        ArgumentNullException.ThrowIfNull(name);
        return string.Concat(ClusterSubjectSuggestionSource.TenantGroupPrefix, tenant, "/", name);
    }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (IsTenantMode)
        {
            return;
        }

        var id = Id ?? string.Empty;
        if (_emittedId is not null && string.Equals(id, _emittedId, StringComparison.Ordinal))
        {
            return;
        }

        // An id set from outside - an edited rule's subject, or a form reset - decides
        // whether this is a tenant group, and which.
        _emittedId = id;
        if (AllowTenantGroups && Kind == LatticeSubjectSelectorKind.Group && ClusterSubjectSuggestionSource.IsTenantGroupId(id))
        {
            _clusterTenantGroup = true;
            var rest = id[ClusterSubjectSuggestionSource.TenantGroupPrefix.Length..];
            var slash = rest.IndexOf('/', StringComparison.Ordinal);
            _groupTenant = slash < 0 ? rest : rest[..slash];
            _groupName = slash < 0 ? string.Empty : rest[(slash + 1)..];
        }
        else if (_clusterTenantGroup && Kind == LatticeSubjectSelectorKind.Group && id.Length == 0)
        {
            _groupTenant = string.Empty;
            _groupName = string.Empty;
        }
        else
        {
            _clusterTenantGroup = false;
        }
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        if (AllowTenantGroups && !IsTenantMode)
        {
            _isOperator = await IsPlatformOperatorAsync().ConfigureAwait(true);
        }
    }

    /// <summary>Checks the typed id against its source, as a submit must before acting.</summary>
    /// <returns>
    /// <see langword="false"/> when the source lists no such subject; on a tenant
    /// page, when a cluster source is given a reserved tenant group id; and
    /// cluster-wide, when a tenant group id is typed rather than chosen, or a chosen
    /// tenant group is not one the caller, as a platform operator, can verify in
    /// that tenant's stored groups.
    /// </returns>
    public async Task<bool> ConfirmAsync()
    {
        if (IsClusterTenantGroup)
        {
            var refusal = await CheckTenantGroupAsync(_groupTenant.Trim(), _groupName.Trim()).ConfigureAwait(true);
            _sourceError = refusal;
            StateHasChanged();
            return refusal is null;
        }

        if (ClusterSubjectSuggestionSource.IsTenantGroupId(Id?.Trim()) && !IsTenantGroup)
        {
            _sourceError = IsTenantMode ? ForeignTenantGroupMessage : AllowTenantGroups ? TypedTenantGroupMessage : TenantGroupNotOfferedMessage;
            StateHasChanged();
            return false;
        }

        _sourceError = null;
        return _box is null || await _box.ConfirmAsync();
    }

    /// <summary>
    /// The refusal for the tenant group <paramref name="name"/> of
    /// <paramref name="tenant"/>, or <see langword="null"/> when it may be chosen:
    /// it must be allowed here, the caller a platform operator, and the tenant
    /// directory must report the group. Anything that cannot be established refuses.
    /// </summary>
    private async Task<string?> CheckTenantGroupAsync(string tenant, string name)
    {
        if (tenant.Length == 0 && name.Length == 0)
        {
            // Whether a subject is required is the page's rule.
            return null;
        }

        if (!AllowTenantGroups)
        {
            return TenantGroupNotOfferedMessage;
        }

        if (!await IsPlatformOperatorAsync().ConfigureAwait(true))
        {
            return OperatorOnlyMessage;
        }

        if (tenant.Length == 0 || name.Length == 0)
        {
            return ChooseTenantAndGroupMessage;
        }

        if (!TenantAccessCatalog.Administers(tenant) || TenantAccess.Directory is not { } directory)
        {
            return NoSuchTenantGroupMessage(tenant, name);
        }

        try
        {
            var group = await directory.GetGroupAsync(tenant, name).ConfigureAwait(true);
            return group is null ? NoSuchTenantGroupMessage(tenant, name) : null;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return OperatorOnlyMessage;
        }
        catch (TenantAccessAdministrationDisabledException)
        {
            return TenantGroupsOffMessage;
        }
        catch (ArgumentException)
        {
            return NoSuchTenantGroupMessage(tenant, name);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return $"Tenant group {TenantGroupId(tenant, name)} could not be verified, so it cannot be chosen now.";
        }
    }

    /// <summary>The sentence a tenant group the tenant directory does not report is refused with.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="name">The group's tenant-local name.</param>
    internal static string NoSuchTenantGroupMessage(string tenant, string name) => $"Tenant {tenant} has no group named {name}.";

    private async Task<bool> IsPlatformOperatorAsync()
    {
        if (Services.GetService<IExplorerTenantOperatorGate>() is not { } gate)
        {
            return false;
        }

        try
        {
            return await gate.IsPlatformOperatorAsync().ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return false;
        }
    }

    private TenantGroupSuggestionSource? GroupsOf(string tenant)
    {
        if (!TenantAccessCatalog.Administers(tenant))
        {
            return null;
        }

        if (_tenantGroups is null || !string.Equals(_tenantGroups.Tenant, tenant, StringComparison.Ordinal))
        {
            _tenantGroups = new TenantGroupSuggestionSource(TenantAccess, tenant);
        }

        return _tenantGroups;
    }

    private async Task OnKindChangedAsync(string value)
    {
        if (IsTenantMode)
        {
            var subjectKind = value switch
            {
                TenantGroupValue => TenantSubjectKind.TenantGroup,
                ClusterGroupValue => TenantSubjectKind.ClusterGroup,
                _ => TenantSubjectKind.User,
            };
            if (subjectKind == SubjectKind)
            {
                return;
            }

            var before = SelectorKind;
            SubjectKind = subjectKind;
            await SubjectKindChanged.InvokeAsync(subjectKind);
            if (SelectorKind != before)
            {
                Kind = SelectorKind;
                await KindChanged.InvokeAsync(Kind);
            }
        }
        else
        {
            var tenantGroup = value == TenantGroupValue && AllowTenantGroups;
            var kind = value == GroupValue || tenantGroup ? LatticeSubjectSelectorKind.Group : LatticeSubjectSelectorKind.User;
            if (kind == Kind && tenantGroup == _clusterTenantGroup)
            {
                return;
            }

            _clusterTenantGroup = tenantGroup;
            _groupTenant = string.Empty;
            _groupName = string.Empty;
            _chosen = null;
            if (kind != Kind)
            {
                Kind = kind;
                await KindChanged.InvokeAsync(kind);
            }
        }

        _sourceError = null;
        await EmitAsync(string.Empty);
    }

    private async Task OnIdChangedAsync(string value)
    {
        _sourceError = null;
        await EmitAsync(value);
    }

    private async Task OnGroupTenantChangedAsync(string value)
    {
        _sourceError = null;
        _groupTenant = value;
        _groupName = string.Empty;
        await EmitAsync(ComposeTenantGroupId());
    }

    private async Task OnGroupNameChangedAsync(string value)
    {
        _sourceError = null;
        _groupName = value;
        await EmitAsync(ComposeTenantGroupId());
    }

    private string ComposeTenantGroupId()
    {
        var tenant = _groupTenant.Trim();
        var name = _groupName.Trim();
        return tenant.Length == 0 || name.Length == 0 ? string.Empty : TenantGroupId(tenant, name);
    }

    private async Task EmitAsync(string id)
    {
        Id = id;
        _emittedId = id;
        await IdChanged.InvokeAsync(id);
    }

    /// <summary>
    /// The display name a chosen suggestion carries: its detail without the
    /// provenance label a source puts in front of it, or its value when it has none.
    /// </summary>
    /// <param name="suggestion">The chosen suggestion.</param>
    /// <returns>The display name.</returns>
    internal static string DisplayNameOf(LtSuggestion suggestion)
    {
        ArgumentNullException.ThrowIfNull(suggestion);
        var detail = suggestion.Detail;
        if (string.IsNullOrEmpty(detail))
        {
            return suggestion.Value;
        }

        return LabelOf(detail) is { } source
            ? detail.Length == source.Length ? suggestion.Value : detail[(source.Length + 3)..]
            : detail;
    }

    /// <summary>
    /// The provenance label at the start of <paramref name="detail"/> - <c>Tenant</c>,
    /// <c>Cluster</c>, <c>Directory</c>, <c>Stored</c> or both of the last two - or
    /// <see langword="null"/> when it carries none.
    /// </summary>
    /// <param name="detail">A suggestion's detail.</param>
    /// <returns>The label, or <see langword="null"/>.</returns>
    internal static string? LabelOf(string? detail)
    {
        if (string.IsNullOrEmpty(detail))
        {
            return null;
        }

        foreach (var source in (ReadOnlySpan<string>)[TenantGroupSuggestionSource.SourceLabel, ClusterSubjectSuggestionSource.SourceLabel, ClusterGroupSuggestionSource.BothLabel, ClusterGroupSuggestionSource.DirectoryLabel, ClusterGroupSuggestionSource.StoredLabel])
        {
            if (string.Equals(detail, source, StringComparison.Ordinal)
                || (detail.Length > source.Length + 3 && detail.StartsWith(source, StringComparison.Ordinal) && detail.AsSpan(source.Length).StartsWith(" - ", StringComparison.Ordinal)))
            {
                return source;
            }
        }

        return null;
    }

    private static string ProvenanceKind(string label) => label switch
    {
        ClusterGroupSuggestionSource.StoredLabel => "stored",
        ClusterGroupSuggestionSource.BothLabel => "directory-and-stored",
        _ => "directory",
    };

    private Task OnChooseAsync(LtSuggestion suggestion)
    {
        var labelled = IsTenantMode || IsClusterTenantGroup || Kind == LatticeSubjectSelectorKind.Group;
        if (!IsTenantMode && !IsClusterTenantGroup && Kind == LatticeSubjectSelectorKind.Group)
        {
            _chosen = LabelOf(suggestion.Detail) is { } label ? (suggestion.Value, label) : null;
        }

        return OnPrincipalSelected.InvokeAsync(new DirectoryPrincipalDescriptor
        {
            Id = IsClusterTenantGroup && _groupTenant.Trim().Length > 0 ? TenantGroupId(_groupTenant.Trim(), suggestion.Value) : suggestion.Value,
            DisplayName = labelled ? DisplayNameOf(suggestion) : suggestion.Detail ?? suggestion.Value,
            Kind = SelectorKind == LatticeSubjectSelectorKind.Group ? DirectoryPrincipalKind.Group : DirectoryPrincipalKind.User,
        });
    }
}
