using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Operations;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The Compliance tab: starts the read-only compliance scan as a tracked cluster
/// operation (#4126), follows its progress in entries scanned until it finishes,
/// and records the report in the session's <see cref="SchemaComplianceLedger"/>.
/// The scan runs on the cluster, so leaving the tab, closing it or reloading does
/// not stop it: on return the tab picks up the tree's latest scan where it is.
/// </summary>
public partial class SchemaCompliancePanel : IDisposable
{
    /// <summary>The sentence shown once a scan was stopped before it finished.</summary>
    internal const string StoppedText = "The scan was stopped before it finished.";

    private readonly ComponentLifetime _lifetime = new();
    private OperationFollower? _follower;
    private string? _operationId;
    private bool _starting;
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

    /// <summary>The scan's status while it has not finished, or <see langword="null"/>.</summary>
    private LatticeOperationStatus? Running =>
        _follower?.Status is { IsTerminal: false } status ? status : null;

    /// <summary>Whether a scan is being started or is running.</summary>
    private bool IsScanning => _starting || Running is not null;

    /// <inheritdoc />
    public void Dispose()
    {
        StopFollowing();
        _lifetime.Leave();
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
            StopFollowing();
            _result = Ledger.Find(workspace.TreeId);
            if (workspace.Grants.ScanCompliance)
            {
                await ResumeLatestAsync(workspace.TreeId);
            }
        }

        if (string.Equals(workspace.Address.GetQuery(SchemaAddresses.ScanQuery), SchemaAddresses.ScanStart, StringComparison.Ordinal))
        {
            // The request travels in the address once: drop it, so a reload or
            // a return through history does not scan again.
            workspace.NavigateTo(workspace.ForTab(SchemaTabs.Compliance), replace: true);
            if (!IsScanning && workspace.Grants.ScanCompliance)
            {
                await ScanAsync();
            }
        }
    }

    /// <summary>
    /// Picks up the tree's newest scan from the cluster: follows it while it runs,
    /// and shows its report when this session has none, so a scan started in
    /// another tab, or before a reload, is not lost.
    /// </summary>
    private async Task ResumeLatestAsync(string treeId)
    {
        if (Facades.Compliance is not { } operations)
        {
            return;
        }

        LatticeOperationStatus? latest = null;
        try
        {
            var page = await operations.ListOperationsAsync(new LatticeOperationListRequest(), _lifetime.Token);
            latest = page.Operations.FirstOrDefault(status => Scans(status, treeId));
        }
        catch (Exception) when (!_lifetime.IsLeft)
        {
            // The listing only restores context; the tab works without it.
        }

        if (latest is null || !string.Equals(_loadedTree, treeId, StringComparison.Ordinal))
        {
            return;
        }

        if (!latest.IsTerminal)
        {
            await FollowAsync(operations, latest.OperationId);
        }
        else if (_result is null && latest.State == LatticeOperationState.Succeeded)
        {
            Settle(latest);
        }
    }

    private async Task ScanAsync()
    {
        if (Workspace is not { } workspace || IsScanning)
        {
            return;
        }

        _starting = true;
        _error = null;
        StateHasChanged();
        try
        {
            var operations = Facades.RequireCompliance();
            var handle = await operations.StartComplianceScanAsync(workspace.TreeId, cancellationToken: _lifetime.Token);
            await FollowAsync(operations, handle.OperationId);
        }
        catch (Exception exception) when (!_lifetime.IsLeft)
        {
            _error = SchemaFailure.Describe(exception, "scan compliance");
        }
        finally
        {
            _starting = false;
        }
    }

    private async Task FollowAsync(ILatticeSchemaComplianceOperations operations, string operationId)
    {
        StopFollowing();
        var follower = new OperationFollower(Time);
        _follower = follower;
        _operationId = operationId;
        follower.Changed += OnFollowedChanged;
        await follower.StartAsync(ct => operations.GetOperationStatusAsync(operationId, ct), _lifetime.Token);
        if (ReferenceEquals(_follower, follower) && follower.Status is { IsTerminal: true } status)
        {
            Settle(status);
        }
    }

    private async Task CancelScanAsync()
    {
        if (_operationId is not { } id || Facades.Compliance is not { } operations || _follower is not { } follower)
        {
            return;
        }

        try
        {
            await operations.CancelOperationAsync(id, _lifetime.Token);
            await follower.RefreshAsync(_lifetime.Token);
        }
        catch (Exception exception) when (!_lifetime.IsLeft)
        {
            _error = SchemaFailure.Describe(exception, "stop the scan");
        }
    }

    private void OnFollowedChanged()
    {
        _ = InvokeAsync(() =>
        {
            if (_follower?.Status is { IsTerminal: true } status)
            {
                Settle(status);
            }
            else if (_follower?.LastError is { } error)
            {
                _error = SchemaFailure.Describe(error, "read the scan's progress");
            }

            StateHasChanged();
        });
    }

    /// <summary>Records a finished scan: its report when it succeeded, why it ended otherwise.</summary>
    private void Settle(LatticeOperationStatus status)
    {
        if (_loadedTree is not { } treeId)
        {
            return;
        }

        switch (status.State)
        {
            case LatticeOperationState.Succeeded when SchemaComplianceScanResults.TryReadReport(status.Result, out var report):
                Ledger.Record(treeId, report, status.FinishedAtUtc ?? Time.GetUtcNow());
                _result = Ledger.Find(treeId);
                _error = null;
                break;
            case LatticeOperationState.Cancelled:
                _error = StoppedText;
                break;
            default:
                _error = "The scan failed. " + (string.IsNullOrWhiteSpace(status.FailureReason) ? "No reason was given." : status.FailureReason);
                break;
        }

        StopFollowing();
    }

    private void StopFollowing()
    {
        if (_follower is { } follower)
        {
            follower.Changed -= OnFollowedChanged;
            follower.Dispose();
        }

        _follower = null;
        _operationId = null;
    }

    /// <summary>Whether <paramref name="status"/> is a scan of <paramref name="treeId"/>: its tree, or that name inside the caller's tenant.</summary>
    internal static bool Scans(LatticeOperationStatus status, string treeId)
    {
        if (status.Scope.TreeIds is not [var scanned])
        {
            return false;
        }

        return string.Equals(scanned, treeId, StringComparison.Ordinal)
            || (scanned.StartsWith("t/", StringComparison.Ordinal) && scanned.EndsWith("/" + treeId, StringComparison.Ordinal)
                && scanned.Length == scanned.IndexOf('/', 2) + 1 + treeId.Length);
    }
}