using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// <c>/replication/trees</c>: the enrolled trees, with merge mode, enrolment source and
/// app ownership, and confirmed enable and disable through the enrolment facade.
/// </summary>
public partial class ReplicationTreesPage
{
    private LtComboBox? _enableTreeBox;
    private static readonly IReadOnlyList<LtSelectOption> MergeModeOptions =
    [
        .. Enum.GetValues<LatticeMergeMode>().Select(mode => new LtSelectOption(mode.ToString(), ReplicationFormat.MergeMode(mode))),
    ];

    private IReadOnlyList<ReplicationTreeRow>? _rows;
    private IReadOnlyList<ReplicationTreeRow> _filtered = [];
    private IReadOnlyList<string> _regions = [];
    private IReadOnlyList<string> _apps = [];
    private ReplicationFault? _fault;
    private ReplicationFault? _statusFault;
    private ReplicationFilter _filter = ReplicationFilter.None;
    private bool _loaded;
    private bool _canEnable;
    private bool _busy;
    private readonly ComponentLifetime _cancellation = new();
    private readonly ComponentLifetime _reads = new();

    private bool _enableOpen;
    private bool _enableTreeFixed;
    private bool _enableModeFixed;
    private string _enableTree = string.Empty;
    private string _enableMode = nameof(LatticeMergeMode.LwwRegister);
    private string _enableBootstrap = string.Empty;

    private bool _disableOpen;
    private ReplicationTreeRow? _disableRow;

    [Inject]
    internal ReplicationDataSource Data { get; set; } = default!;

    [Inject]
    internal LtToastService Toasts { get; set; } = default!;

    [Inject]
    internal ExplorerSuggestions Suggestions { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    internal IReadOnlyList<ReplicationTreeRow> FilteredRows => _filtered;

    /// <summary>
    /// Whether this caller may enable and disable here: an enrolment facade is
    /// registered and its (permission-scoped) report was readable.
    /// </summary>
    internal bool CanControl => Data.HasControl && _rows is not null;

    /// <summary>
    /// Whether to offer enabling replication for a tree the caller names (issue #5478):
    /// the facade's probe says the caller holds the Replication capability, or the
    /// report lists a tree, which it does only for a tree the caller may manage. The
    /// report alone proves nothing, because every caller may read it. Presentation
    /// only; the facade authorizes every enable on attempt.
    /// </summary>
    internal bool CanEnableNew => CanControl && (_canEnable || _rows!.Count > 0);
    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private string FaultTitle => _fault?.Kind switch
    {
        ReplicationFaultKind.Denied => "Replication enrolment is not open to you",
        ReplicationFaultKind.NotServed => "Replication enrolment is not served here",
        ReplicationFaultKind.Bootstrapping => "Tree is bootstrapping",
        _ => "Replication enrolment could not be read",
    };

    private string? EnableTreeError
    {
        get
        {
            var tree = _enableTree.Trim();
            if (tree.Length == 0)
            {
                return null;
            }

            return ReplicationTreeOwnership.TryGetAppSlug(tree, out var slug)
                ? $"This tree belongs to the app {slug}; its enrolment follows the app install."
                : null;
        }
    }

    private bool CanSubmitEnable =>
        !_busy && _enableTree.Trim().Length > 0 && EnableTreeError is null && Enum.TryParse<LatticeMergeMode>(_enableMode, out _);

    /// <summary>
    /// How the tree picker judges its value: only a tree this cluster holds, until a bootstrap
    /// source is named - then any id, because the bootstrap is how a tree this region does not
    /// hold yet arrives from the source cluster.
    /// </summary>
    internal LtComboBoxMode EnableTreeMode =>
        _enableTreeFixed || string.IsNullOrWhiteSpace(_enableBootstrap) ? LtComboBoxMode.PickExisting : LtComboBoxMode.Suggest;

    /// <summary>
    /// What the dialog says while an enable is in flight. A bootstrap waits for the source cluster
    /// to answer, which can take a minute or more (#5560), so the page says so rather than leaving
    /// a disabled button as the only sign of work.
    /// </summary>
    internal string EnablingStatus => string.IsNullOrWhiteSpace(_enableBootstrap)
        ? $"Enabling replication for {_enableTree.Trim()}. Waiting for the cluster to record the enrolment."
        : $"Enabling replication for {_enableTree.Trim()} and asking {_enableBootstrap.Trim()} for a snapshot. "
          + $"This waits for {_enableBootstrap.Trim()} to answer, which can take a minute or more. "
          + "You can close this dialog; stay on this page and a message reports the result.";

    private string EnableTreeHint => EnableTreeMode == LtComboBoxMode.Suggest
        ? "The logical tree id, such as orders. It need not exist here yet: the bootstrap brings it from the source cluster."
        : "The logical tree id, such as orders.";

    /// <inheritdoc />
    public void Dispose()
    {
        _reads.Leave();
        _cancellation.Leave();
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        _filter = ReplicationFilter.From(Address);
        if (!_loaded)
        {
            _loaded = true;
            await LoadAsync(refresh: false);
        }
        else
        {
            Apply();
        }
    }

    private Task ReloadAsync() => LoadAsync(refresh: true);

    private async Task LoadAsync(bool refresh)
    {
        var token = _reads.Renew();
        try
        {
            var config = await Data.GetConfigAsync(refresh, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            var canEnable = config.Value is not null && await Data.CanEnableAsync(refresh, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            var status = await Data.GetEstateAsync(refresh, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _fault = config.Fault;
            _canEnable = canEnable;
            _statusFault = status.Fault;
            var links = status.Value?.Links ?? [];
            _rows = config.Value is { } report ? ReplicationTreeRow.Build(report, links) : null;
            _regions = status.Value?.PeerRegions ?? [];
            Apply();
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            // The read was replaced, or the page went away.
        }
    }

    private void Apply()
    {
        _filtered = _rows is null ? [] : [.. _rows.Where(row => row.Matches(_filter))];
        _apps = _rows is null
            ? []
            : [.. _rows.Select(row => row.AppSlug).OfType<string>().Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)];
    }

    private string? TreeHref(string treeId) =>
        ReplicationAddresses.ForTree(treeId) is { } address ? Navigator.Canonicalize(address).ToHref() : null;

    private string AppHref(string slug) => Navigator.Canonicalize(ReplicationTreeOwnership.AppReplicationAddress(slug)).ToHref();

    private static string CompactSummary(ReplicationTreeRow row) =>
        ReplicationFormat.MergeMode(row.Entry.Mode) + " - " + ReplicationFormat.Source(row.Entry.Source)
        + (row.AppSlug is { } slug ? " - app " + slug : string.Empty);

    private void OpenEnableForNew()
    {
        _enableTree = string.Empty;
        _enableTreeFixed = false;
        _enableMode = nameof(LatticeMergeMode.LwwRegister);
        _enableModeFixed = false;
        _enableBootstrap = string.Empty;
        _enableOpen = true;
    }

    private void OpenEnableFor(ReplicationTreeRow row)
    {
        _enableTree = row.TreeId;
        _enableTreeFixed = true;
        _enableModeFixed = row.Entry.Mode is not null;
        _enableMode = (row.Entry.Mode ?? LatticeMergeMode.LwwRegister).ToString();
        _enableBootstrap = string.Empty;
        _enableOpen = true;
    }

    private void OnEnableOpenChanged(bool open) => _enableOpen = open;

    private void OnEnableTreeChanged(string value) => _enableTree = value ?? string.Empty;

    private void OnEnableModeChanged(string value) => _enableMode = value;

    private void OnEnableBootstrapChanged(string value) => _enableBootstrap = value ?? string.Empty;

    private Task CloseEnableAsync()
    {
        _enableOpen = false;
        return Task.CompletedTask;
    }

    private async Task EnableAsync()
    {
        if (_cancellation.IsLeft || !_enableOpen || !CanSubmitEnable || !Enum.TryParse<LatticeMergeMode>(_enableMode, out var mode))
        {
            return;
        }

        var draft = (Tree: _enableTree, Mode: _enableMode, Bootstrap: _enableBootstrap);
        var token = _cancellation.Token;
        var tree = _enableTree.Trim();
        _busy = true;
        try
        {
            if (!_enableTreeFixed && _enableTreeBox is not null && !await _enableTreeBox.ConfirmAsync())
            {
                return;
            }

            if (token.IsCancellationRequested || !_enableOpen || draft != (_enableTree, _enableMode, _enableBootstrap))
            {
                return;
            }

            var result = await Data.EnableAsync(tree, mode, draft.Bootstrap, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            _enableOpen = false;
            if (result.AlreadyEnabled)
            {
                Toasts.Show($"Replication was already enabled for {result.TreeId} ({ReplicationFormat.MergeMode(result.Mode)}).", LtToastTone.Info);
            }
            else
            {
                Toasts.Show(
                    $"Replication enabled for {result.TreeId} ({ReplicationFormat.MergeMode(result.Mode)})."
                    + (result.BootstrapRequested ? " A snapshot bootstrap was requested." : string.Empty),
                    LtToastTone.Success);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            if (!token.IsCancellationRequested)
            {
                Toasts.Show(ChangeFailure(ex, "enable", tree), LtToastTone.Danger);
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _busy = false;
            }
        }

        if (!token.IsCancellationRequested)
        {
            await LoadAsync(refresh: true);
        }
    }

    private void OpenDisable(ReplicationTreeRow row)
    {
        _disableRow = row;
        _disableOpen = true;
    }

    private void OnDisableOpenChanged(bool open)
    {
        _disableOpen = open;
        if (!open && !_busy)
        {
            _disableRow = null;
        }
    }

    private async Task DisableAsync()
    {
        if (_cancellation.IsLeft || _busy || _disableRow is not { } row)
        {
            return;
        }

        var token = _cancellation.Token;
        _busy = true;
        try
        {
            var result = await Data.DisableAsync(row.TreeId, token);
            if (token.IsCancellationRequested)
            {
                return;
            }

            Toasts.Show(
                result.AlreadyDisabled
                    ? $"Replication was already disabled for {result.TreeId}."
                    : $"Replication disabled for {result.TreeId}.",
                result.AlreadyDisabled ? LtToastTone.Info : LtToastTone.Success);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            if (!token.IsCancellationRequested)
            {
                Toasts.Show(ChangeFailure(ex, "disable", row.TreeId), LtToastTone.Danger);
            }
        }
        finally
        {
            if (!token.IsCancellationRequested)
            {
                _busy = false;
                _disableOpen = false;
                _disableRow = null;
            }
        }

        if (!token.IsCancellationRequested)
        {
            await LoadAsync(refresh: true);
        }
    }

    private static string ChangeFailure(Exception exception, string verb, string treeId) => exception switch
    {
        UnauthorizedAccessException => $"You are not allowed to {verb} replication for {treeId}.",
        NotSupportedException => "This cluster does not serve replication enrolment.",
        ArgumentException or InvalidOperationException =>
            $"The cluster refused to {verb} replication for {treeId}. A tree's merge mode is fixed when it is first enabled.",
        _ => $"The cluster could not {verb} replication for {treeId}. Try again in a moment.",
    };
}
