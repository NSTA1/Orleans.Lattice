using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>
/// <c>/schema</c>: the trees under a schema policy, version config or app
/// declaration, with versioning and compliance summaries, and the "Scan
/// compliance..." picker the palette command opens.
/// </summary>
public partial class SchemaDirectoryPage
{
    private readonly CancellationTokenSource _lifetime = new();
    private SchemaDirectoryRead? _read;
    private IReadOnlyList<SchemaTreeRow> _rows = [];
    private IReadOnlyList<LtSelectOption> _scanTargets = [];
    private string? _error;
    private bool _loading;
    private bool _pickerOpen;
    private string? _pickedTree;

    [Inject]
    internal SchemaDirectory Directory { get; set; } = default!;

    [Inject]
    internal SchemaComplianceLedger Ledger { get; set; } = default!;

    [Inject]
    internal SchemaCommandSignals Signals { get; set; } = default!;

    /// <summary>The width band the layout measured; <see langword="null"/> outside the layout reads as expanded.</summary>
    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    internal LtBreakpoint? Breakpoint { get; set; }

    internal bool ShowAll => string.Equals(Address.GetQuery(SchemaAddresses.ShowQuery), SchemaAddresses.ShowAll, StringComparison.Ordinal);

    internal string Filter => Address.GetQuery(SchemaAddresses.FilterQuery) ?? string.Empty;

    internal IReadOnlyList<LtSelectOption> ScanTargets => _scanTargets;

    private LtDialogPlacement DialogPlacement => Breakpoint == LtBreakpoint.Compact ? LtDialogPlacement.End : LtDialogPlacement.Center;

    private string StatusLine
    {
        get
        {
            if (_read is null)
            {
                return string.Empty;
            }

            var governed = _read.Governed.Count();
            return $"{SchemaFormat.Count(governed, "tree")} of {SchemaFormat.Count(_read.TreeCount, "tree")} under schema. Read at {SchemaFormat.Time(_read.ReadAt)}.";
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Signals.Requested -= OnCommandRequested;
        Ledger.Recorded -= OnComplianceRecorded;
        _lifetime.Cancel();
        _lifetime.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void OnInitialized()
    {
        Signals.Requested += OnCommandRequested;
        Ledger.Recorded += OnComplianceRecorded;
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (_read is null && _error is null)
        {
            await LoadAsync(refresh: false);
        }
        else
        {
            Project();
        }

        if (Signals.TryTake(SchemaArea.ScanCommandId))
        {
            OpenScanPicker();
        }
    }

    internal static string PolicyText(SchemaTreeRow row) => row.PolicyState switch
    {
        SchemaReadState.Denied => "Not permitted",
        SchemaReadState.Unavailable => "Not available",
        SchemaReadState.Failed => "Could not read",
        _ => row.Policy is { } policy ? SchemaFormat.Policy(policy) : "None",
    };

    internal static string VersionText(SchemaTreeRow row) => row.VersionState switch
    {
        SchemaReadState.Denied => "Not permitted",
        SchemaReadState.Unavailable => "Not available",
        SchemaReadState.Failed => "Could not read",
        _ => row.Version is { } version ? SchemaFormat.Version(version) : "Unversioned",
    };

    private string ComplianceText(SchemaTreeRow row) => Ledger.Find(row.TreeId) switch
    {
        null => "Not scanned",
        { Report.HasPolicy: false } => "No policy",
        { IsCompliant: true } result => $"Compliant, {SchemaFormat.Count(result.Report.ScannedCount, "value")}",
        { } result => $"{SchemaFormat.Count(result.Report.NonCompliantCount, "value")} non-compliant",
    };

    private LtStateRole? ComplianceState(SchemaTreeRow row) => Ledger.Find(row.TreeId) switch
    {
        null or { Report.HasPolicy: false } => null,
        { IsCompliant: true } => LtStateRole.Healthy,
        _ => LtStateRole.Drift,
    };

    private static string CompactSummary(SchemaTreeRow row) => $"Policy: {PolicyText(row)}. Versioning: {VersionText(row)}.";

    private string Href(ExplorerAddress address) => Navigator.Canonicalize(address.WithTenant(Address.Tenant)).ToHref();

    private ExplorerAddress ShowAddress(bool all)
    {
        var address = all ? SchemaAddresses.AllTrees : SchemaAddresses.Directory;
        return Filter.Length == 0 ? address : address.WithQuery(SchemaAddresses.FilterQuery, Filter);
    }

    private async Task LoadAsync(bool refresh)
    {
        _loading = true;
        _error = null;
        try
        {
            _read = await Directory.GetAsync(refresh, _lifetime.Token);
            Project();
        }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            _read = null;
            _error = exception switch
            {
                InvalidOperationException => exception.Message,
                _ => SchemaFailure.Describe(exception, "list the trees"),
            };
        }
        finally
        {
            _loading = false;
        }
    }

    private void Project()
    {
        if (_read is null)
        {
            _rows = [];
            _scanTargets = [];
            return;
        }

        var filter = Filter;
        _rows = _read.Rows
            .Where(row => (ShowAll || row.IsGoverned)
                && (filter.Length == 0 || row.TreeId.Contains(filter, StringComparison.OrdinalIgnoreCase)))
            .ToArray();
        _scanTargets = _read.Rows
            .Where(row => row.Policy is not null)
            .Select(row => new LtSelectOption(row.TreeId, row.TreeId))
            .ToArray();
        if (_pickerOpen && string.IsNullOrEmpty(_pickedTree) && _scanTargets.Count > 0)
        {
            _pickedTree = _scanTargets[0].Value;
        }
    }

    private Task RefreshAsync() => LoadAsync(refresh: true);

    private void OnFilterChanged(string value)
    {
        var address = (ShowAll ? SchemaAddresses.AllTrees : SchemaAddresses.Directory)
            .WithQuery(SchemaAddresses.FilterQuery, string.IsNullOrEmpty(value) ? null : value)
            .WithTenant(Address.Tenant);
        Navigator.NavigateTo(address, replace: true);
    }

    private void OpenScanPicker()
    {
        _pickedTree = _scanTargets.Count > 0 ? _scanTargets[0].Value : null;
        _pickerOpen = true;
    }

    private void ClosePicker() => _pickerOpen = false;

    private void OnPickerOpenChanged(bool open) => _pickerOpen = open;

    private void OnPicked(string tree) => _pickedTree = tree;

    private void StartPickedScan()
    {
        if (string.IsNullOrEmpty(_pickedTree))
        {
            return;
        }

        _pickerOpen = false;
        Navigator.NavigateTo(SchemaAddresses.StartScan(_pickedTree).WithTenant(Address.Tenant));
    }

    private void OnCommandRequested(string commandId)
    {
        if (string.Equals(commandId, SchemaArea.ScanCommandId, StringComparison.Ordinal))
        {
            _ = InvokeAsync(() =>
            {
                OpenScanPicker();
                StateHasChanged();
            });
        }
    }

    private void OnComplianceRecorded(string treeId) => _ = InvokeAsync(StateHasChanged);
}
