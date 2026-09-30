using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Metrics;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Data area's tree workspace at <c>/data/{tree-path}</c>: the tree the
/// address names, resolved against the caller's directory (so an address the
/// caller cannot reach reads as not found), with its keys, history, metrics,
/// dead letters, tag indexes and views as tabs.
/// </summary>
public partial class DataTreePage : IDisposable
{
    private readonly ComponentLifetime _lifetime = new();
    private DataTreeEntry? _tree;
    private DataTreeEntry? _source;
    private DataWorkspace? _workspace;
    private ExplorerAddress? _resolvedFor;
    private long? _liveKeys;
    private bool _missing;
    private string? _error;

    [Inject]
    internal DataDirectory Directory { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var address = Address;
        var target = ExplorerAddress.Create(address.Tenant, address.Area, address.Path);
        if (!target.Equals(_resolvedFor))
        {
            await ResolveAsync(target);
        }

        _workspace = _tree is null ? null : new DataWorkspace(_tree, address, Navigator, Directory);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

    private async Task ResolveAsync(ExplorerAddress target)
    {
        _resolvedFor = target;
        _tree = null;
        _source = null;
        _liveKeys = null;
        _missing = false;
        _error = null;
        try
        {
            _tree = await Directory.ResolveAsync(target, _lifetime.Token);
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
            return;
        }
        catch (Exception exception)
        {
            _error = DataErrors.Describe(exception, "read the tree catalogue");
            _resolvedFor = null;
            return;
        }

        if (_tree is null)
        {
            _missing = true;
            return;
        }

        _source = Directory.FindByStateId(_tree.SourceStateId);
        _ = LoadKeyCountAsync(_tree);
    }

    private async Task LoadKeyCountAsync(DataTreeEntry tree)
    {
        if (DataServices.Find<IMetricsReader>(Services) is not { } reader)
        {
            return;
        }

        try
        {
            var metrics = await reader.GetAsync(tree.StateId, _lifetime.Token);
            if (metrics is { DetailPaused: false } && ReferenceEquals(tree, _tree))
            {
                _liveKeys = metrics.LiveKeys;
                await InvokeAsync(StateHasChanged);
            }
        }
        catch (Exception)
        {
            // The key count in the summary line is a courtesy; the Metrics tab
            // reports a failure properly.
        }
    }

    private async Task RetryAsync()
    {
        _resolvedFor = null;
        await OnParametersSetAsync();
    }

    private void OnTabChanged(string tab)
    {
        if (_workspace is not null && tab != _workspace.Tab)
        {
            _workspace.NavigateTo(_workspace.ForTab(tab));
        }
    }
}
