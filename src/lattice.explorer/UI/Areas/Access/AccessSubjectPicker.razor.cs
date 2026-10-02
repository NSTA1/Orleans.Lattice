using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access.Tenant;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Suggestions;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.UI.Areas.Access;

/// <summary>
/// Picks a subject - a user or a group - by id, as a type-ahead combobox over the
/// cluster's identity directory when one is configured. The directory is searched
/// as the id is typed (debounced by input, never on a timer), only a listed
/// principal is accepted, and a principal's display name is always rendered as
/// text. Without a directory the id is used as typed.
/// </summary>
/// <remarks>
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

    /// <summary>The kind value of one of the tenant's own groups.</summary>
    internal const string TenantGroupValue = "tenant-group";

    /// <summary>The sentence a reserved tenant group id typed into a cluster source is refused with.</summary>
    internal const string ForeignTenantGroupMessage = "A tenant group is chosen as a Tenant group, by its name. Another tenant's group can never be chosen.";

    private LtComboBox? _box;
    private string? _sourceError;
    private TenantGroupSuggestionSource? _tenantGroups;
    private ILtSuggestionSource? _clusterGroups;
    private ILtSuggestionSource? _clusterUsers;

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

    /// <summary>The subject id. For one of the tenant's own groups it is the group's tenant-local name.</summary>
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

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [Inject]
    internal TenantAccessCatalog TenantAccess { get; set; } = default!;

    private static IReadOnlyList<LtSelectOption> KindOptions { get; } =
    [
        new(UserValue, "User"),
        new(GroupValue, "Group"),
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

    private IReadOnlyList<LtSelectOption> Options => IsTenantMode ? TenantKindOptions : KindOptions;

    private ILtSuggestionSource? Source
    {
        get
        {
            if (!IsTenantMode)
            {
                return DirectoryAvailable ? Suggestions.Subjects(Kind) : null;
            }

            if (IsTenantGroup)
            {
                if (_tenantGroups is null || !string.Equals(_tenantGroups.Tenant, Tenant, StringComparison.Ordinal))
                {
                    _tenantGroups = new TenantGroupSuggestionSource(TenantAccess, Tenant!);
                }

                return _tenantGroups;
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

    private LtComboBoxMode Mode => IsTenantGroup || DirectoryAvailable ? LtComboBoxMode.PickExisting : LtComboBoxMode.Suggest;

    private string KindValue => IsTenantMode
        ? SubjectKind switch
        {
            TenantSubjectKind.TenantGroup => TenantGroupValue,
            TenantSubjectKind.ClusterGroup => ClusterGroupValue,
            _ => UserValue,
        }
        : AccessRuleFormat.SubjectKindLabel(Kind);

    private string KindWord => IsTenantGroup ? "tenant group" : SelectorKind == LatticeSubjectSelectorKind.Group ? "group" : "user";

    private LatticeSubjectSelectorKind SelectorKind => IsTenantMode
        ? SubjectKind == TenantSubjectKind.User ? LatticeSubjectSelectorKind.User : LatticeSubjectSelectorKind.Group
        : Kind;

    private string IdHint => IsTenantGroup
        ? $"Type part of a name to choose one of tenant {Tenant}'s own groups."
        : DirectoryAvailable
            ? (string.IsNullOrWhiteSpace(DirectoryExplanation) ? "Type part of a name or id to search the directory." : DirectoryExplanation!)
            : "No identity directory is configured, so the id is used as typed and is not validated.";

    /// <summary>
    /// The chosen group's provenance and full id, or <see langword="null"/> when
    /// no group is chosen on a tenant page.
    /// </summary>
    private (string Source, string FullId)? Provenance
    {
        get
        {
            if (!IsTenantMode || SubjectKind == TenantSubjectKind.User || string.IsNullOrWhiteSpace(Id))
            {
                return null;
            }

            var id = Id.Trim();
            return SubjectKind == TenantSubjectKind.TenantGroup
                ? (TenantGroupSuggestionSource.SourceLabel, TenantGroupId(Tenant!, id))
                : (ClusterSubjectSuggestionSource.SourceLabel, id);
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

    /// <summary>Checks the typed id against its source, as a submit must before acting.</summary>
    /// <returns>
    /// <see langword="false"/> when the source lists no such subject, or, on a
    /// tenant page, when a cluster source is given a reserved tenant group id.
    /// </returns>
    public async Task<bool> ConfirmAsync()
    {
        if (IsTenantMode && !IsTenantGroup && ClusterSubjectSuggestionSource.IsTenantGroupId(Id?.Trim()))
        {
            _sourceError = ForeignTenantGroupMessage;
            StateHasChanged();
            return false;
        }

        _sourceError = null;
        return _box is null || await _box.ConfirmAsync();
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
            var kind = value == GroupValue ? LatticeSubjectSelectorKind.Group : LatticeSubjectSelectorKind.User;
            if (kind == Kind)
            {
                return;
            }

            Kind = kind;
            await KindChanged.InvokeAsync(kind);
        }

        _sourceError = null;
        Id = string.Empty;
        await IdChanged.InvokeAsync(string.Empty);
    }

    private async Task OnIdChangedAsync(string value)
    {
        _sourceError = null;
        Id = value;
        await IdChanged.InvokeAsync(value);
    }

    /// <summary>
    /// The display name a chosen suggestion carries: its detail without the
    /// provenance label a tenant page puts in front of it, or its value when it
    /// has none.
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

        foreach (var source in (ReadOnlySpan<string>)[TenantGroupSuggestionSource.SourceLabel, ClusterSubjectSuggestionSource.SourceLabel])
        {
            if (string.Equals(detail, source, StringComparison.Ordinal))
            {
                return suggestion.Value;
            }

            if (detail.Length > source.Length + 3 && detail.StartsWith(source, StringComparison.Ordinal) && detail.AsSpan(source.Length).StartsWith(" - ", StringComparison.Ordinal))
            {
                return detail[(source.Length + 3)..];
            }
        }

        return detail;
    }

    private Task OnChooseAsync(LtSuggestion suggestion) =>
        OnPrincipalSelected.InvokeAsync(new DirectoryPrincipalDescriptor
        {
            Id = suggestion.Value,
            DisplayName = IsTenantMode ? DisplayNameOf(suggestion) : suggestion.Detail ?? suggestion.Value,
            Kind = SelectorKind == LatticeSubjectSelectorKind.Group ? DirectoryPrincipalKind.Group : DirectoryPrincipalKind.User,
        });
}
