using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>
/// The Compliance tab: runs the read-only compliance scan and shows its result,
/// recorded in the session's <see cref="SchemaComplianceLedger"/>.
/// </summary>
public partial class SchemaCompliancePanel : IDisposable
{
    private readonly CancellationTokenSource _lifetime = new();
    private CancellationTokenSource? _scanning;
    private SchemaComplianceResult? _result;
    private string? _loadedTree;
    private string? _error;

    [CascadingParameter]
    internal SchemaWorkspace? Workspace { get; set; }

    [Inject]
    internal SchemaFacades Facades { get; set; } = default!;

    [Inject]
    internal SchemaComplianceLedger Ledger { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        _scanning?.Cancel();
        _scanning?.Dispose();
        _lifetime.Cancel();
        _lifetime.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is not { } workspace)
        {
            return;
        }

        if (!string.Equals(workspace.TreeId, _loadedTree, StringComparison.Ordinal))
        {
            _loadedTree = workspace.TreeId;
            _error = null;
            _result = Ledger.Find(workspace.TreeId);
        }

        if (string.Equals(workspace.Address.GetQuery(SchemaAddresses.ScanQuery), SchemaAddresses.ScanStart, StringComparison.Ordinal))
        {
            // The request travels in the address once: drop it, so a reload or
            // a return through history does not scan again.
            workspace.NavigateTo(workspace.ForTab(SchemaTabs.Compliance), replace: true);
            if (_scanning is null && workspace.Grants.ScanCompliance)
            {
                await ScanAsync();
            }
        }
    }

    private async Task ScanAsync()
    {
        if (Workspace is not { } workspace || _scanning is not null)
        {
            return;
        }

        var scan = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
        _scanning = scan;
        _error = null;
        StateHasChanged();
        try
        {
            var report = await Facades.RequireSchema().ScanComplianceAsync(workspace.TreeId, scan.Token);
            Ledger.Record(workspace.TreeId, report, Time.GetUtcNow());
            _result = Ledger.Find(workspace.TreeId);
        }
        catch (OperationCanceledException) when (scan.IsCancellationRequested)
        {
            if (!_lifetime.IsCancellationRequested)
            {
                _error = "The scan was stopped before it finished.";
            }
        }
        catch (Exception exception)
        {
            _error = SchemaFailure.Describe(exception, "scan compliance");
        }
        finally
        {
            if (ReferenceEquals(_scanning, scan))
            {
                _scanning = null;
            }

            scan.Dispose();
        }
    }

    private void CancelScan() => _scanning?.Cancel();
}
