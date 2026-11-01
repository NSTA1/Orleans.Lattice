using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Metrics;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Data area's tree workspace at <c>/data/{tree-path}</c>: the tree the
/// address names, resolved against the caller's directory (so an address the
/// caller cannot reach reads as not found), with its keys, history, metrics,
/// dead letters, tag indexes and views as tabs. It owns the visible control of
/// the <c>data.export</c> command, drawn only for a caller the export gate admits,
/// and the tree's Delete tree... and Recover tree... controls for a caller who may
/// manage its lifecycle (issue #5477).
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
    private bool _canExport;
    private bool _exportOpen;
    private DataImportRights _importRights;
    private bool _importOpen;
    private bool _seedFirst;

    [Inject]
    internal DataDirectory Directory { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    [Inject]
    internal DataAdminGate Gate { get; set; } = default!;

    [Inject]
    internal DataViewCommandScope ViewCommand { get; set; } = default!;

    [Inject]
    internal DataExportGate ExportGate { get; set; } = default!;

    [Inject]
    internal DataExportCommandScope ExportCommand { get; set; } = default!;

    [Inject]
    internal DataCommandSignals Signals { get; set; } = default!;

    [Inject]
    internal DataImportGate ImportGate { get; set; } = default!;

    [Inject]
    internal DataImportCommandScope ImportCommand { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        base.OnInitialized();
        Signals.Requested += OnCommand;
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var address = Address;
        var target = ExplorerAddress.Create(address.Tenant, address.Area, address.Path);
        if (!target.Equals(_resolvedFor))
        {
            await ResolveAsync(target);
        }

        if (!_lifetime.IsLeft && target.Equals(_resolvedFor))
        {
            _workspace = _tree is null ? null : new DataWorkspace(_tree, Address, Navigator, Directory);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        ViewCommand.Withdraw(_tree);
        ExportCommand.Withdraw(_tree);
        ImportCommand.Withdraw(_tree);
        Signals.Requested -= OnCommand;
        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

    private async Task ResolveAsync(ExplorerAddress target)
    {
        var token = _lifetime.Renew();
        _resolvedFor = target;
        ViewCommand.Withdraw(_tree);
        ExportCommand.Withdraw(_tree);
        ImportCommand.Withdraw(_tree);
        _tree = null;
        _workspace = null;
        _source = null;
        _liveKeys = null;
        _missing = false;
        _error = null;
        _canExport = false;
        _exportOpen = false;
        _importRights = DataImportRights.None;
        _importOpen = false;
        ResetLifecycle();
        try
        {
            var tree = await Directory.ResolveAsync(target, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _tree = tree;
        }
        catch (Exception) when (token.IsCancellationRequested)
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
        _ = OfferViewCommandAsync(_tree, token);
        _ = LoadExportGateAsync(_tree);
        _ = LoadImportGateAsync(_tree);
        _ = LoadLifecycleGateAsync(_tree);
    }

    /// <summary>
    /// Offers the palette's New view command over <paramref name="tree"/> once the
    /// caller is proven to administer it; a view, a shared tree or a refusal offers nothing.
    /// </summary>
    private async Task OfferViewCommandAsync(DataTreeEntry tree, CancellationToken token)
    {
        if (tree.Kind != DataTreeKind.Tree || tree.IsShared || Gate.Admin is null)
        {
            return;
        }

        try
        {
            if (await Gate.CanAdministerAsync(tree.StateId, token) && !token.IsCancellationRequested && ReferenceEquals(tree, _tree))
            {
                ViewCommand.Offer(tree);
            }
        }
        catch (Exception)
        {
            // The command is a convenience; the Views tab draws its own control.
        }
    }

    private async Task LoadExportGateAsync(DataTreeEntry tree)
    {
        bool allowed;
        try
        {
            allowed = await ExportGate.CanExportAsync(tree, _lifetime.Token);
        }
        catch (Exception)
        {
            return;
        }

        if (!allowed || !ReferenceEquals(tree, _tree))
        {
            return;
        }

        _canExport = true;
        ExportCommand.Offer(tree);
        if (Signals.TryTake(DataArea.ExportCommandId))
        {
            _exportOpen = true;
        }

        await InvokeAsync(StateHasChanged);
    }

    private void OnCommand(string commandId)
    {
        if (string.Equals(commandId, DataArea.ImportCommandId, StringComparison.Ordinal))
        {
            _ = InvokeAsync(() =>
            {
                // A caller the gate admits to no mode has no Import control, and the
                // command opens nothing for them either.
                if (_importRights.Any)
                {
                    _seedFirst = false;
                    _importOpen = true;
                    StateHasChanged();
                }
            });
        }
        if (string.Equals(commandId, DataArea.ExportCommandId, StringComparison.Ordinal))
        {
            _ = InvokeAsync(() =>
            {
                // A caller the gate does not admit has no Export control, and the command
                // opens nothing for them either.
                if (_canExport)
                {
                    _exportOpen = true;
                    StateHasChanged();
                }
            });
        }
    }

    private void OpenExport() => _exportOpen = true;

    private void OpenImport()
    {
        _seedFirst = false;
        _importOpen = true;
    }

    /// <summary>
    /// Draws the Import control, and offers its palette command, once the gate admits the
    /// caller to some import mode; opens the dialog at once when the palette or a
    /// <c>?import=seed</c> address (a tree just created from a file) asked for it.
    /// </summary>
    private async Task LoadImportGateAsync(DataTreeEntry tree)
    {
        DataImportRights rights;
        try
        {
            rights = await ImportGate.ForTreeAsync(tree, _lifetime.Token);
        }
        catch (Exception)
        {
            return;
        }

        if (!rights.Any || !ReferenceEquals(tree, _tree))
        {
            return;
        }

        _importRights = rights;
        ImportCommand.Offer(tree);
        var seed = string.Equals(Address.GetQuery(DataTabs.ImportQuery), DataTabs.ImportSeedValue, StringComparison.Ordinal);
        if (Signals.TryTake(DataArea.ImportCommandId) || seed)
        {
            _seedFirst = seed;
            _importOpen = true;
        }

        await InvokeAsync(StateHasChanged);
    }

    private async Task OnImportedAsync()
    {
        _liveKeys = null;
        if (_tree is { } tree)
        {
            await LoadKeyCountAsync(tree);
        }
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
