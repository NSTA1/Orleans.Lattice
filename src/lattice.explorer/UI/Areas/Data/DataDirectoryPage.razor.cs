using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Data area's directory page at <c>/data</c>: every tree and view the caller
/// can reach, filterable and virtualised, each named by its logical id.
/// </summary>
public partial class DataDirectoryPage : IDisposable
{
    /// <summary>The kind filter's value for every row.</summary>
    internal const string AllKinds = "all";

    /// <summary>The kind filter's value for the tenant's own trees.</summary>
    internal const string TreeKinds = "trees";

    /// <summary>The kind filter's value for views.</summary>
    internal const string ViewKinds = "views";

    /// <summary>The kind filter's value for the trees and prefixes other tenants share with this one.</summary>
    internal const string SharedKinds = "shared";

    private static readonly LtSelectOption[] OwnedKindOptions =
    [
        new(AllKinds, "All"),
        new(TreeKinds, "Trees"),
        new(ViewKinds, "Views"),
    ];

    private static readonly LtSelectOption[] TenantKindOptions =
    [
        .. OwnedKindOptions,
        new(SharedKinds, "Shared with this tenant"),
    ];

    private readonly CancellationTokenSource _lifetime = new();
    private IReadOnlyList<DataTreeEntry>? _entries;
    private IReadOnlyList<DataTreeEntry> _visible = [];
    private string? _filter;
    private string _kind = AllKinds;
    private string? _error;
    private bool _subscribed;
    private bool _anyApp;
    private bool _anySource;

    [Inject]
    internal DataDirectory Directory { get; set; } = default!;

    [Inject]
    internal ExplorerTenancy Tenancy { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    // Only a tenant other than the reserved default one can have trees shared with
    // it, so the filter offers "shared" only where sharing applies.
    private IReadOnlyList<LtSelectOption> KindOptions => Directory.SharingApplies ? TenantKindOptions : OwnedKindOptions;

    // The responsive contract: a segmented control of more than three options becomes a select at the compact band.
    private bool UseKindSelect => Breakpoint == LtBreakpoint.Compact && KindOptions.Count > 3;

    // Which optional columns the table draws; the table is keyed on it.
    private (bool Shared, bool App, bool Source) ColumnSet => (Directory.SharingApplies, _anyApp, _anySource);

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

    internal static LtStateRole? StatusRole(DataTreeEntry entry) => entry.IsShared
        ? null
        : entry.Kind == DataTreeKind.View
        ? LtStateRole.Enabled
        : entry.Lifecycle switch
        {
            "Active" => LtStateRole.Healthy,
            "SoftDeleted" => LtStateRole.Disabled,
            "Purging" => LtStateRole.Failed,
            _ => LtStateRole.Unknown,
        };

    internal static string StatusText(DataTreeEntry entry) => entry.AccessText is { } access
        ? access
        : entry.Kind == DataTreeKind.View
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

        if (entry.SharedText is { } shared)
        {
            parts.Add(shared.ToLowerInvariant());
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

    private void SetKind(string kind)
    {
        _kind = kind;
        Apply();
    }

    private bool MatchesKind(DataTreeEntry entry) => _kind switch
    {
        TreeKinds => entry.Kind == DataTreeKind.Tree && !entry.IsShared,
        ViewKinds => entry.Kind == DataTreeKind.View,
        SharedKinds => entry.IsShared,
        _ => true,
    };

    private void Apply()
    {
        if (_entries is not { } entries)
        {
            _visible = [];
            _anyApp = false;
            _anySource = false;
            return;
        }

        // The columns follow the whole listing, not the filtered rows, so typing a
        // filter never makes them come and go.
        _anyApp = false;
        _anySource = false;
        foreach (var entry in entries)
        {
            _anyApp |= entry.AppSlug is not null;
            _anySource |= entry.SourceLogicalId is not null;
        }

        if (_kind == SharedKinds && !Directory.SharingApplies)
        {
            _kind = AllKinds;
        }

        var filter = _filter?.Trim();
        if (string.IsNullOrEmpty(filter) && _kind == AllKinds)
        {
            _visible = entries;
            return;
        }

        var visible = new List<DataTreeEntry>();
        foreach (var entry in entries)
        {
            if (!MatchesKind(entry))
            {
                continue;
            }

            if (!string.IsNullOrEmpty(filter)
                && !entry.LogicalId.Contains(filter, StringComparison.OrdinalIgnoreCase)
                && !entry.DisplayName.Contains(filter, StringComparison.OrdinalIgnoreCase)
                && !(entry.AppSlug?.Contains(filter, StringComparison.OrdinalIgnoreCase) ?? false)
                && !(entry.SharedBy?.Contains(filter, StringComparison.OrdinalIgnoreCase) ?? false))
            {
                continue;
            }

            visible.Add(entry);
        }

        _visible = visible;
    }
}
