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
    private bool _busy;
    private readonly ComponentLifetime _cancellation = new();

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

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private string FaultTitle => _fault?.Kind switch
    {
        ReplicationFaultKind.Denied => "Replication enrolment is not open to you",
        ReplicationFaultKind.NotServed => "Replication enrolment is not served here",
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

    /// <inheritdoc />
    public void Dispose()
    {
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
        try
        {
            var config = await Data.GetConfigAsync(refresh, _cancellation.Token);
            var status = await Data.GetEstateAsync(refresh, _cancellation.Token);
            _fault = config.Fault;
            _statusFault = status.Fault;
            var links = status.Value?.Links ?? [];
            _rows = config.Value is { } report ? ReplicationTreeRow.Build(report, links) : null;
            _regions = status.Value?.PeerRegions ?? [];
            Apply();
        }
        catch (OperationCanceledException)
        {
            // The page went away.
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
        if (!CanSubmitEnable || !Enum.TryParse<LatticeMergeMode>(_enableMode, out var mode))
        {
            return;
        }

        if (!_enableTreeFixed && _enableTreeBox is not null && !await _enableTreeBox.ConfirmAsync())
        {
            return;
        }

        var tree = _enableTree.Trim();
        _busy = true;
        try
        {
            var result = await Data.EnableAsync(tree, mode, _enableBootstrap, _cancellation.Token);
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
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            Toasts.Show(ChangeFailure(ex, "enable", tree), LtToastTone.Danger);
        }
        finally
        {
            _busy = false;
        }

        await LoadAsync(refresh: true);
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
        if (_disableRow is not { } row)
        {
            return;
        }

        _busy = true;
        try
        {
            var result = await Data.DisableAsync(row.TreeId, _cancellation.Token);
            Toasts.Show(
                result.AlreadyDisabled
                    ? $"Replication was already disabled for {result.TreeId}."
                    : $"Replication disabled for {result.TreeId}.",
                result.AlreadyDisabled ? LtToastTone.Info : LtToastTone.Success);
        }
        catch (OperationCanceledException)
        {
            return;
        }
        catch (Exception ex)
        {
            Toasts.Show(ChangeFailure(ex, "disable", row.TreeId), LtToastTone.Danger);
        }
        finally
        {
            _busy = false;
            _disableOpen = false;
            _disableRow = null;
        }

        await LoadAsync(refresh: true);
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
