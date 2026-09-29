using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Data;

/// <summary>
/// The Data area's directory page at <c>/data</c>: every tree and view the caller
/// can reach, filterable and virtualised, each named by its logical id.
/// </summary>
public partial class DataDirectoryPage : IDisposable
{
    private static readonly (DataTreeKind? Kind, string Text)[] KindOptions =
    [
        (null, "All"),
        (DataTreeKind.Tree, "Trees"),
        (DataTreeKind.View, "Views"),
    ];

    private readonly CancellationTokenSource _lifetime = new();
    private IReadOnlyList<DataTreeEntry>? _entries;
    private IReadOnlyList<DataTreeEntry> _visible = [];
    private string? _filter;
    private DataTreeKind? _kind;
    private string? _error;
    private bool _subscribed;

    [Inject]
    internal DataDirectory Directory { get; set; } = default!;

    [Inject]
    internal ExplorerTenancy Tenancy { get; set; } = default!;

    private string Lede => Tenancy.IsActive && Tenancy.ActiveTenant is { } tenant
        ? $"Every tree and view you can reach in tenant {tenant}."
        : "Every tree and view you can reach.";

    private string CountText => _entries is null
        ? string.Empty
        : _visible.Count == _entries.Count
            ? (_entries.Count == 1 ? "1 tree or view" : $"{DataFormat.Count(_entries.Count)} trees and views")
            : $"{DataFormat.Count(_visible.Count)} of {DataFormat.Count(_entries.Count)} match";

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        _filter = Address.GetQuery(DataTabs.FilterQuery);
        Directory.Changed += OnDirectoryChanged;
        _subscribed = true;
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync() => await LoadAsync();

    /// <inheritdoc />
    public void Dispose()
    {
        if (_subscribed)
        {
            Directory.Changed -= OnDirectoryChanged;
        }

        _lifetime.Cancel();
        _lifetime.Dispose();
        GC.SuppressFinalize(this);
    }

    internal static LtStateRole? StatusRole(DataTreeEntry entry) => entry.Kind == DataTreeKind.View
        ? LtStateRole.Enabled
        : entry.Lifecycle switch
        {
            "Active" => LtStateRole.Healthy,
            "SoftDeleted" => LtStateRole.Disabled,
            "Purging" => LtStateRole.Failed,
            _ => LtStateRole.Unknown,
        };

    internal static string StatusText(DataTreeEntry entry) => entry.Kind == DataTreeKind.View
        ? "View"
        : entry.Lifecycle switch
        {
            "Active" => "Active",
            "SoftDeleted" => "Soft-deleted",
            "Purging" => "Purging",
            _ => "Unknown",
        };

    private static string CompactSummary(DataTreeEntry entry)
    {
        var parts = new List<string>(3) { entry.KindText };
        if (entry.ShardCount is { } shards)
        {
            parts.Add(DataArea.Plural(shards, "shard"));
        }

        if (entry.AppSlug is { } slug)
        {
            parts.Add("app " + slug);
        }

        return string.Join(" - ", parts);
    }

    private string AppHref(string slug) =>
        Navigator.Canonicalize(ExplorerAddress.ForArea(DataArea.AppsAreaKey, slug)).ToHref();

    private DataTreeEntry? SourceOf(DataTreeEntry view) => Directory.FindByStateId(view.SourceStateId);

    private async Task LoadAsync()
    {
        _error = null;
        try
        {
            _entries = await Directory.LoadAsync(_lifetime.Token);
            Apply();
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _entries = null;
            _error = DataErrors.Describe(exception, "read the tree catalogue");
        }
    }

    private async Task RefreshAsync()
    {
        _entries = null;
        _error = null;
        try
        {
            await Directory.RefreshAsync(_lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
            return;
        }
        catch (Exception)
        {
            // LoadAsync below reports the failure.
        }

        await LoadAsync();
    }

    private void OnDirectoryChanged()
    {
        if (Directory.Loaded is { } loaded)
        {
            _entries = loaded;
            _error = null;
            Apply();
        }

        _ = InvokeAsync(StateHasChanged);
    }

    private void OnFilterChanged(string value)
    {
        _filter = value;
        Apply();
    }

    private void SetKind(DataTreeKind? kind)
    {
        _kind = kind;
        Apply();
    }

    private void Apply()
    {
        if (_entries is not { } entries)
        {
            _visible = [];
            return;
        }

        var filter = _filter?.Trim();
        if (string.IsNullOrEmpty(filter) && _kind is null)
        {
            _visible = entries;
            return;
        }

        var visible = new List<DataTreeEntry>();
        foreach (var entry in entries)
        {
            if (_kind is { } kind && entry.Kind != kind)
            {
                continue;
            }

            if (!string.IsNullOrEmpty(filter)
                && !entry.LogicalId.Contains(filter, StringComparison.OrdinalIgnoreCase)
                && !entry.DisplayName.Contains(filter, StringComparison.OrdinalIgnoreCase)
                && !(entry.AppSlug?.Contains(filter, StringComparison.OrdinalIgnoreCase) ?? false))
            {
                continue;
            }

            visible.Add(entry);
        }

        _visible = visible;
    }
}
