using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The Access-area subject picker's search / paging / selection state machine,
/// extracted from the Razor view so it is unit-testable without a component-render
/// harness. Drives a debounced, server-backed typeahead over
/// <see cref="IMembershipAdminService.SearchDirectoryAsync"/>: a rapid keystroke
/// burst coalesces (via <see cref="ISubjectSearchDebounce"/>) into a single
/// bounded directory query; a continuation token pages more results in; toggling
/// the <see cref="Kind"/> clears the current selection and re-searches; and a
/// directory that reports itself unavailable (or a denied / failed read) flips
/// the model into a free-text, unvalidated fallback where the typed term is taken
/// as the selected id verbatim rather than enumerating the tenant. The view binds
/// to this model and forwards <see cref="SubjectSelected"/> to its parent.
/// </summary>
public sealed class SubjectPickerModel
{
    /// <summary>The default bounded result-page size the picker requests per search.</summary>
    public const int DefaultPageSize = 10;

    private readonly IMembershipAdminService _membership;
    private readonly ISubjectSearchDebounce _debounce;
    private readonly List<DirectoryPrincipalDescriptor> _results = new();
    private readonly Func<Task> _searchNow;

    private int _searchGeneration;

    /// <summary>Creates a model over the membership service and a debounce seam.</summary>
    /// <param name="membership">The membership admin service the search runs over. Must not be <see langword="null"/>.</param>
    /// <param name="debounce">The debounce seam that coalesces rapid input. Must not be <see langword="null"/>.</param>
    public SubjectPickerModel(IMembershipAdminService membership, ISubjectSearchDebounce debounce)
    {
        ArgumentNullException.ThrowIfNull(membership);
        ArgumentNullException.ThrowIfNull(debounce);
        _membership = membership;
        _debounce = debounce;
        // Cached so scheduling a search does not allocate a fresh delegate per keystroke.
        _searchNow = SearchNowAsync;
    }

    /// <summary>Raised whenever the model's observable state changes, so the view can re-render.</summary>
    public event Action? Changed;

    /// <summary>
    /// Raised with the chosen id and its directory display name whenever the
    /// selected subject changes. The id is empty when the selection is cleared,
    /// and the display name is empty whenever no meaningful directory name is
    /// known for the id (a cleared selection or the free-text fallback path).
    /// </summary>
    public event Action<string, string>? SubjectSelected;

    /// <summary>The kind of principal the search is restricted to.</summary>
    public DirectoryPrincipalKind Kind { get; private set; } = DirectoryPrincipalKind.User;

    /// <summary>The current search term (in the fallback path, the free-text subject id).</summary>
    public string SearchTerm { get; private set; } = string.Empty;

    /// <summary>The currently selected subject id, or empty when nothing is selected.</summary>
    public string SelectedId { get; private set; } = string.Empty;

    /// <summary>
    /// The directory display name of the currently selected subject, or empty
    /// when nothing is selected, when the selection came from the free-text
    /// fallback, or when the matched principal carries no meaningful display name
    /// (one that is blank or equal to its id).
    /// </summary>
    public string SelectedDisplayName { get; private set; } = string.Empty;

    /// <summary>The bounded page of matched principals from the most recent search.</summary>
    public IReadOnlyList<DirectoryPrincipalDescriptor> Results => _results;

    /// <summary>The continuation cursor for the next page, or <see langword="null"/> at the last page.</summary>
    public string? NextPageToken { get; private set; }

    /// <summary><see langword="true"/> when a further page can be loaded via <see cref="LoadMoreAsync"/>.</summary>
    public bool HasMore => NextPageToken is not null;

    /// <summary>
    /// <see langword="true"/> when a searchable identity directory is available;
    /// <see langword="false"/> once a read reports no directory (or is denied /
    /// fails), in which case the view degrades to free-text entry.
    /// </summary>
    public bool DirectoryAvailable { get; private set; } = true;

    /// <summary><see langword="true"/> while a directory read is in flight.</summary>
    public bool IsSearching { get; private set; }

    /// <summary>The bounded page size requested per search. Defaults to <see cref="DefaultPageSize"/>.</summary>
    public int PageSize { get; init; } = DefaultPageSize;

    /// <summary>
    /// Seeds the model with an existing <paramref name="selectedId"/> and
    /// <paramref name="kind"/> (for example when editing a rule whose subject is
    /// already chosen) and runs the initial browse search, which also establishes
    /// whether the directory is available.
    /// </summary>
    /// <param name="selectedId">The already-selected subject id, or <see langword="null"/> for none.</param>
    /// <param name="kind">The principal kind to search within.</param>
    public Task InitializeAsync(string? selectedId = null, DirectoryPrincipalKind kind = DirectoryPrincipalKind.User)
    {
        Kind = kind;
        SelectedId = selectedId ?? string.Empty;
        return SearchNowAsync();
    }

    /// <summary>
    /// Applies a change to the <see cref="Kind"/> filter: a no-op when unchanged,
    /// otherwise it clears the current selection and re-runs the search under the
    /// new kind.
    /// </summary>
    /// <param name="kind">The new principal kind to search within.</param>
    public Task SetKindAsync(DirectoryPrincipalKind kind)
    {
        if (kind == Kind)
        {
            return Task.CompletedTask;
        }

        Kind = kind;
        ClearSelection();
        return SearchNowAsync();
    }

    /// <summary>
    /// Records a new search <paramref name="term"/>. On the directory-backed path
    /// it schedules a debounced search; on the free-text fallback path it takes the
    /// term verbatim as the selected id (never issuing a directory query).
    /// </summary>
    /// <param name="term">The new search term. Must not be <see langword="null"/>.</param>
    public Task SetSearchTermAsync(string term)
    {
        ArgumentNullException.ThrowIfNull(term);
        SearchTerm = term;

        if (!DirectoryAvailable)
        {
            SetSelectedInternal(term);
            Changed?.Invoke();
            return Task.CompletedTask;
        }

        Changed?.Invoke();
        _debounce.Schedule(_searchNow);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Runs the directory search for the current term and kind immediately,
    /// bypassing the debounce (the action the debounce ultimately invokes, and the
    /// deterministic entry point for tests). Replaces the current result page and
    /// records the continuation token.
    /// </summary>
    public async Task SearchNowAsync()
    {
        var generation = ++_searchGeneration;
        IsSearching = true;
        Changed?.Invoke();

        var view = await _membership
            .SearchDirectoryAsync(SearchTerm, Kind, PageSize, pageToken: null)
            .ConfigureAwait(false);

        if (generation != _searchGeneration)
        {
            // A newer search superseded this one; let the newer completion own state.
            return;
        }

        DirectoryAvailable = view.Available;
        _results.Clear();
        if (view.Available)
        {
            _results.AddRange(view.Principals);
            NextPageToken = view.NextPageToken;
        }
        else
        {
            NextPageToken = null;
        }

        IsSearching = false;
        Changed?.Invoke();
    }

    /// <summary>
    /// Appends the next page of results using the current
    /// <see cref="NextPageToken"/>. A no-op when there is no further page or a
    /// search is already running. Stops paging when the returned token is
    /// <see langword="null"/>.
    /// </summary>
    public async Task LoadMoreAsync()
    {
        if (NextPageToken is null || IsSearching || !DirectoryAvailable)
        {
            return;
        }

        var generation = _searchGeneration;
        var token = NextPageToken;
        IsSearching = true;
        Changed?.Invoke();

        var view = await _membership
            .SearchDirectoryAsync(SearchTerm, Kind, PageSize, token)
            .ConfigureAwait(false);

        if (generation != _searchGeneration)
        {
            // A reset search ran while this page was in flight; drop the stale page.
            return;
        }

        if (view.Available)
        {
            _results.AddRange(view.Principals);
            NextPageToken = view.NextPageToken;
        }

        IsSearching = false;
        Changed?.Invoke();
    }

    /// <summary>Selects the principal with the given <paramref name="id"/>, raising <see cref="SubjectSelected"/>.</summary>
    /// <param name="id">The chosen subject id. Must not be <see langword="null"/>.</param>
    public Task SelectAsync(string id)
    {
        ArgumentNullException.ThrowIfNull(id);
        SetSelectedInternal(id);
        Changed?.Invoke();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Quietly aligns the model to a parent-pushed <paramref name="kind"/> and
    /// <paramref name="selectedId"/> (for example when a rule being edited sets both
    /// at once) without clearing, re-searching, or raising
    /// <see cref="SubjectSelected"/>, so an external state sync never fights the
    /// two-way binding nor wipes the caller's own value.
    /// </summary>
    /// <param name="kind">The principal kind to align to.</param>
    /// <param name="selectedId">The selected id to align to. Must not be <see langword="null"/>.</param>
    public void SyncExternalState(DirectoryPrincipalKind kind, string selectedId)
    {
        ArgumentNullException.ThrowIfNull(selectedId);
        Kind = kind;
        SelectedId = selectedId;
    }

    private void ClearSelection() => SetSelectedInternal(string.Empty);

    private void SetSelectedInternal(string id)
    {
        if (string.Equals(id, SelectedId, StringComparison.Ordinal))
        {
            return;
        }

        SelectedId = id;
        SelectedDisplayName = ResolveMeaningfulDisplayName(id);
        SubjectSelected?.Invoke(id, SelectedDisplayName);
    }

    /// <summary>
    /// Looks the id up in the current result page and returns its display name
    /// only when that name is meaningful (non-blank and not equal to the id);
    /// returns empty for a cleared selection, a free-text id absent from the page,
    /// or a principal whose display name merely echoes its id.
    /// </summary>
    private string ResolveMeaningfulDisplayName(string id)
    {
        if (string.IsNullOrEmpty(id))
        {
            return string.Empty;
        }

        foreach (var principal in _results)
        {
            if (!string.Equals(principal.Id, id, StringComparison.Ordinal))
            {
                continue;
            }

            return string.IsNullOrWhiteSpace(principal.DisplayName)
                || string.Equals(principal.DisplayName, principal.Id, StringComparison.Ordinal)
                    ? string.Empty
                    : principal.DisplayName;
        }

        return string.Empty;
    }
}
