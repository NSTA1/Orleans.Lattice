using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.History;
using Orleans.Lattice.Explorer.UI.Areas.Data;

namespace Orleans.Lattice.Explorer.UI.Areas.Access.Tenant.Groups;

/// <summary>
/// A tenant group's history (D19): the revisions of its record in the cluster's
/// membership store, read through the Explorer's existing <see cref="IHistoryReader"/>
/// and shown newest first. No new audit system: every change to a tenant group is a
/// write to the dogfooded membership tree, whose per-key history already records it.
/// </summary>
public partial class TenantGroupHistory
{
    /// <summary>
    /// The membership store's group tree, whose per-key history records every change
    /// to a group. Used only to read the history; a physical tree id is never shown.
    /// </summary>
    internal const string GroupsTree = "sys-membership-groups";

    /// <summary>How many revisions one read asks for.</summary>
    internal const int PageSize = 20;

    /// <summary>The note shown when this Explorer has no history reader.</summary>
    internal const string NotServedText = "This Explorer cannot read history, so the group's changes are not shown here.";

    /// <summary>The note shown when the caller may not read the membership store's history.</summary>
    internal const string DeniedText = "The group's history is kept in the cluster's membership store, which only a platform operator may read.";

    private readonly List<HistoryRevisionRow> _chronological = [];
    private IReadOnlyList<HistoryRevisionRow>? _rows;
    private string? _continuation;
    private string? _unavailable;
    private bool _loading;
    private (string Tenant, string Name)? _loaded;

    /// <summary>The group's tenant-local name.</summary>
    [Parameter]
    [EditorRequired]
    public string Name { get; set; } = string.Empty;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    private string HistoryState => _unavailable is not null ? "unavailable" : _rows is null ? "loading" : "ready";

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (_loaded is { } loaded && loaded.Tenant == Tenant && loaded.Name == Name)
        {
            return;
        }

        _loaded = (Tenant, Name);
        _chronological.Clear();
        _rows = null;
        _continuation = null;
        _unavailable = null;
        await LoadPageAsync().ConfigureAwait(true);
    }

    /// <summary>The time a revision was written, as the Data area shows it.</summary>
    /// <param name="row">The revision.</param>
    /// <returns>The time, or <c>-</c> when it names none.</returns>
    internal static string When(HistoryRevisionRow row) => DataFormat.Time(row.Hlc);

    /// <summary>What a revision did to the group's record.</summary>
    /// <param name="row">The revision.</param>
    /// <returns>The text.</returns>
    internal static string ChangeText(HistoryRevisionRow row) => row.Kind switch
    {
        HistoryRowKind.Delete or HistoryRowKind.RangeTombstone => "Deleted",
        _ => "Created or changed",
    };

    private Task LoadMoreAsync() => LoadPageAsync();

    private async Task LoadPageAsync()
    {
        if (Resolve() is not { } reader)
        {
            _unavailable = NotServedText;
            return;
        }

        var (tenant, name) = (Tenant, Name);
        _loading = true;
        try
        {
            var page = await reader.LoadAsync(GroupsTree, AccessSubjectPicker.TenantGroupId(tenant, name), PageSize, _continuation, Lifetime.Token).ConfigureAwait(true);
            if (Lifetime.IsLeft || tenant != Tenant || name != Name)
            {
                return;
            }

            _chronological.AddRange(page.Revisions);
            _continuation = string.IsNullOrEmpty(page.ContinuationToken) ? null : page.ContinuationToken;
            var newestFirst = new HistoryRevisionRow[_chronological.Count];
            for (var i = 0; i < newestFirst.Length; i++)
            {
                newestFirst[i] = _chronological[_chronological.Count - 1 - i];
            }

            _rows = newestFirst;
        }
        catch (OperationCanceledException) when (Lifetime.IsLeft)
        {
        }
        catch (LatticeAuthorizationDeniedException)
        {
            _unavailable = DeniedText;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _unavailable = "The group's history could not be read. " + (TenantAccessFailure.From(exception, tenant)?.Message ?? string.Empty);
        }
        finally
        {
            _loading = false;
        }
    }

    private IHistoryReader? Resolve()
    {
        try
        {
            return Services.GetService(typeof(IHistoryReader)) as IHistoryReader;
        }
        catch (InvalidOperationException)
        {
            // A reader whose own state connection this head does not register cannot serve.
            return null;
        }
    }
}
