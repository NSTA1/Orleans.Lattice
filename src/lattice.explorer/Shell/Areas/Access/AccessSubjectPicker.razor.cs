using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Shell.Areas.Access;

/// <summary>
/// Picks a subject - a user or a group - by id, searching the cluster's identity
/// directory when one is configured. Searches run only when asked for, never on
/// a timer, and a principal's display name is always rendered as text.
/// </summary>
public partial class AccessSubjectPicker
{
    private const int PageSize = 20;

    private IReadOnlyList<DirectoryPrincipalDescriptor>? _results;
    private string? _continuation;
    private string? _searchError;
    private bool _searching;

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
    internal AccessCatalog Catalog { get; set; } = default!;

    private static IReadOnlyList<LtSelectOption> KindOptions { get; } =
    [
        new("user", "User"),
        new("group", "Group"),
    ];

    private string KindValue => AccessRuleFormat.SubjectKindLabel(Kind);

    private string KindWord => Kind == LatticeSubjectSelectorKind.Group ? "group" : "user";

    private string IdHint => DirectoryAvailable
        ? (string.IsNullOrWhiteSpace(DirectoryExplanation) ? "Type part of a name or id, then search the directory." : DirectoryExplanation!)
        : "No identity directory is configured, so the id is used as typed and is not validated.";

    private async Task OnKindChangedAsync(string value)
    {
        var kind = value == "group" ? LatticeSubjectSelectorKind.Group : LatticeSubjectSelectorKind.User;
        if (kind == Kind)
        {
            return;
        }

        Kind = kind;
        ClearResults();
        await KindChanged.InvokeAsync(kind);
        Id = string.Empty;
        await IdChanged.InvokeAsync(string.Empty);
    }

    private async Task OnIdChangedAsync(string value)
    {
        Id = value;
        await IdChanged.InvokeAsync(value);
    }

    private Task SearchAsync()
    {
        ClearResults();
        return RunSearchAsync(null);
    }

    private Task LoadMoreAsync() => RunSearchAsync(_continuation);

    private async Task RunSearchAsync(string? continuation)
    {
        _searching = true;
        _searchError = null;
        try
        {
            var result = await Catalog.Admin.SearchDirectoryAsync(
                new DirectorySearchRequest
                {
                    Term = Id?.Trim() ?? string.Empty,
                    Kind = Kind == LatticeSubjectSelectorKind.Group ? DirectoryPrincipalKind.Group : DirectoryPrincipalKind.User,
                    PageSize = PageSize,
                    ContinuationToken = continuation,
                }).ConfigureAwait(true);

            if (!result.Available)
            {
                _searchError = "The identity directory is unavailable, so the id is used as typed and is not validated.";
                return;
            }

            _results = continuation is null ? result.Principals : [.. _results ?? [], .. result.Principals];
            _continuation = result.ContinuationToken;
        }
        catch (Exception exception) when (AccessFailure.From(exception) is { } failure)
        {
            _searchError = failure.Message;
        }
        finally
        {
            _searching = false;
        }
    }

    private async Task SelectAsync(DirectoryPrincipalDescriptor principal)
    {
        Id = principal.Id;
        ClearResults();
        await IdChanged.InvokeAsync(principal.Id);
        await OnPrincipalSelected.InvokeAsync(principal);
    }

    private void ClearResults()
    {
        _results = null;
        _continuation = null;
        _searchError = null;
    }
}
