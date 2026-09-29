using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
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
public partial class AccessSubjectPicker
{
    private LtComboBox? _box;

    /// <summary>The field's label, such as <c>Subject</c> or <c>Member</c>.</summary>
    [Parameter]
    public string Label { get; set; } = "Subject";

    /// <summary>The subject kind.</summary>
    [Parameter]
    public LatticeSubjectSelectorKind Kind { get; set; } = LatticeSubjectSelectorKind.User;

    /// <summary>Raised when the kind changes; the id is cleared with it.</summary>
    [Parameter]
    public EventCallback<LatticeSubjectSelectorKind> KindChanged { get; set; }

    /// <summary>Whether the kind can be chosen; when not, it is fixed to <see cref="Kind"/>.</summary>
    [Parameter]
    public bool AllowKindChange { get; set; } = true;

    /// <summary>The subject id.</summary>
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

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    private static IReadOnlyList<LtSelectOption> KindOptions { get; } =
    [
        new("user", "User"),
        new("group", "Group"),
    ];

    private ILtSuggestionSource? Source => DirectoryAvailable ? Suggestions.Subjects(Kind) : null;

    private string KindValue => AccessRuleFormat.SubjectKindLabel(Kind);

    private string KindWord => Kind == LatticeSubjectSelectorKind.Group ? "group" : "user";

    private string IdHint => DirectoryAvailable
        ? (string.IsNullOrWhiteSpace(DirectoryExplanation) ? "Type part of a name or id to search the directory." : DirectoryExplanation!)
        : "No identity directory is configured, so the id is used as typed and is not validated.";

    /// <summary>Checks the typed id against the directory, as a submit must before acting.</summary>
    /// <returns><see langword="false"/> when a directory is configured and lists no such principal.</returns>
    public Task<bool> ConfirmAsync() => _box?.ConfirmAsync() ?? Task.FromResult(true);

    private async Task OnKindChangedAsync(string value)
    {
        var kind = value == "group" ? LatticeSubjectSelectorKind.Group : LatticeSubjectSelectorKind.User;
        if (kind == Kind)
        {
            return;
        }

        Kind = kind;
        await KindChanged.InvokeAsync(kind);
        Id = string.Empty;
        await IdChanged.InvokeAsync(string.Empty);
    }

    private async Task OnIdChangedAsync(string value)
    {
        Id = value;
        await IdChanged.InvokeAsync(value);
    }

    private Task OnChooseAsync(LtSuggestion suggestion) =>
        OnPrincipalSelected.InvokeAsync(new DirectoryPrincipalDescriptor
        {
            Id = suggestion.Value,
            DisplayName = suggestion.Detail ?? suggestion.Value,
            Kind = Kind == LatticeSubjectSelectorKind.Group ? DirectoryPrincipalKind.Group : DirectoryPrincipalKind.User,
        });
}