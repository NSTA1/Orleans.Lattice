using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Operations;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The resumable status page of a tree's long-running schema operation: the
/// operation this session started, if any, joined with the cluster's own
/// remediation report, then followed through the operations facade when an
/// operation id is known.
/// </summary>
public partial class SchemaOperationStatus : IDisposable
{
    /// <summary>How often a report-only remediation status is re-read.</summary>
    public static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(2);

    private static readonly string[] RemediationPhases =
    [
        SchemaOperationPhases.DryRun,
        SchemaOperationPhases.Build,
        SchemaOperationPhases.Cutover,
    ];

    private static readonly string[] AdvanceAndMigratePhases =
    [
        SchemaOperationPhases.Advance,
        SchemaOperationPhases.DryRun,
        SchemaOperationPhases.Build,
        SchemaOperationPhases.Cutover,
    ];

    private readonly ComponentLifetime _lifetime = new();
    private SchemaOperation? _local;
    private LatticeSchemaRemediationReport? _report;
    private OperationFollower? _follower;
    private string? _followedOperationId;
    private string? _loadedTree;
    private string? _statusError;
    private string? _cancelError;
    private ITimer? _timer;
    private bool _read;
    private bool _reading;
    private bool _cancelling;
    private bool _terminalRefreshDone;

    [CascadingParameter]
    internal SchemaWorkspace? Workspace { get; set; }

    [Inject]
    internal SchemaFacades Facades { get; set; } = default!;

    [Inject]
    internal SchemaOperations Operations { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    /// <summary>Whether a timer or operation follower is re-reading the status.</summary>
    internal bool IsPolling => _timer is not null || _follower is { IsFollowing: true };

    /// <summary>
    /// Where the operation is: the cluster operation when visible, this session's
    /// staged start otherwise, then the legacy remediation report.
    /// </summary>
    internal SchemaOperationStage? Stage
    {
        get
        {
            if (_follower?.Status is { } status)
            {
                return StageOf(status);
            }

            if (_local is { } local && (_follower is null || _follower.NotFound))
            {
                return local.Stage;
            }

            if (_report is not { } report)
            {
                return null;
            }

            return report switch
            {
                { InProgress: true } => SchemaOperationStage.Running,
                { Phase: LatticeSchemaRemediationPhase.Completed } => SchemaOperationStage.Completed,
                { Phase: LatticeSchemaRemediationPhase.Aborted } => SchemaOperationStage.Aborted,
                { Phase: LatticeSchemaRemediationPhase.Cancelled } => SchemaOperationStage.Cancelled,
                _ => null,
            };
        }
    }

    private LatticeSchemaRemediationReport? Report => _report ?? (_local?.Report is { } terminal && !_local.IsActive ? terminal : null);

    private LatticeOperationStatus? Status => _follower?.Status ?? _local?.Status;

    private IEnumerable<StageItem> Steps => Status is { } status && status.PhaseCount is > 0
        ? OperationSteps(status)
        : ReportSteps();

    private string FinishedTitle => Stage switch
    {
        SchemaOperationStage.Aborted => "Stopped, nothing cut over",
        SchemaOperationStage.Cancelled => "Cancelled, nothing cut over",
        SchemaOperationStage.Failed => "Refused",
        _ => "Finished",
    };

    private string Headline => Stage switch
    {
        SchemaOperationStage.Starting => "Asking the cluster to start.",
        SchemaOperationStage.Running => _local is null ? "An operation is running on this tree." : "Running in the cluster.",
        SchemaOperationStage.Completed => "The last operation finished, and the tree serves its result.",
        SchemaOperationStage.Aborted => "The last operation stopped at a value that still fails.",
        SchemaOperationStage.Cancelled => "The last operation was cancelled before cutover.",
        SchemaOperationStage.Failed => "The cluster did not run the operation.",
        _ => string.Empty,
    };

    private bool CanCancel => Status is { IsTerminal: false, CancelRequested: false } status && Workspace is { } workspace && status.Kind switch
    {
        SchemaOperationKinds.Remediation => workspace.Grants.Remediate,
        SchemaOperationKinds.Migration or SchemaOperationKinds.AdvanceAndMigrate => workspace.Grants.ManageVersion,
        _ => false,
    };

    /// <inheritdoc />
    public void Dispose()
    {
        Operations.Changed -= OnOperationChanged;
        DetachFollower();
        _timer?.Dispose();
        _timer = null;
        _lifetime.Leave();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void OnInitialized() => Operations.Changed += OnOperationChanged;

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Workspace is { } workspace && !string.Equals(workspace.TreeId, _loadedTree, StringComparison.Ordinal))
        {
            _loadedTree = workspace.TreeId;
            _local = Operations.Find(workspace.TreeId);
            _report = null;
            _read = false;
            _terminalRefreshDone = false;
            DetachFollower();
            await ReadAsync();
            await FollowKnownOperationAsync();
            UpdatePolling();
        }
    }

    private string CurrentOperationId => _local?.OperationId ?? _report?.OperationId ?? string.Empty;

    private int StepPosition(StageItem step)
    {
        if (Status is { PhaseIndex: { } index })
        {
            if (Status.IsTerminal && step.Index <= index)
            {
                return -1;
            }

            return step.Index.CompareTo(index);
        }

        var current = Stage switch
        {
            SchemaOperationStage.Aborted or SchemaOperationStage.Failed or SchemaOperationStage.Cancelled => SchemaOperationStage.Completed,
            { } stage => stage,
            null => SchemaOperationStage.Starting,
        };

        if (current == SchemaOperationStage.Completed)
        {
            return step.Stage == SchemaOperationStage.Completed ? 0 : -1;
        }

        return step.Stage.CompareTo(current);
    }

    private async Task ReadAsync()
    {
        if (Workspace is not { } workspace || !workspace.Grants.ViewRemediation || _reading)
        {
            _read = true;
            return;
        }

        _reading = true;
        try
        {
            _report = await Facades.RequireSchema().GetRemediationStatusAsync(workspace.TreeId, _lifetime.Token);
            _statusError = null;
        }
        catch (OperationCanceledException) when (_lifetime.IsLeft)
        {
        }
        catch (Exception exception)
        {
            _statusError = SchemaFailure.Describe(exception, "read the operation status");
        }
        finally
        {
            _reading = false;
            _read = true;
        }
    }

    private async Task FollowKnownOperationAsync()
    {
        var operationId = CurrentOperationId;
        if (string.IsNullOrEmpty(operationId) || string.Equals(operationId, _followedOperationId, StringComparison.Ordinal))
        {
            return;
        }

        DetachFollower();
        _followedOperationId = operationId;
        var follower = new OperationFollower(Time);
        _follower = follower;
        follower.Changed += OnFollowedChanged;
        await follower.StartAsync(ct => Facades.RequireSchemaOperations().GetOperationStatusAsync(operationId, ct), _lifetime.Token);
        if (follower.NotFound)
        {
            await ReadAsync();
        }
    }

    private async Task RefreshAsync()
    {
        await ReadAsync();
        await FollowKnownOperationAsync();
        if (_follower is { } follower && !follower.NotFound)
        {
            await follower.RefreshAsync(_lifetime.Token);
        }

        UpdatePolling();
    }

    private async Task CancelAsync()
    {
        if (Status is not { } status)
        {
            return;
        }

        _cancelling = true;
        _cancelError = null;
        try
        {
            var cancelled = await Facades.RequireSchemaOperations().CancelOperationAsync(status.OperationId, _lifetime.Token);
            if (cancelled is null)
            {
                _cancelError = "The operation is no longer there to cancel.";
            }

            if (_follower is { } follower)
            {
                await follower.RefreshAsync(_lifetime.Token);
            }
        }
        catch (Exception exception) when (!_lifetime.IsLeft)
        {
            _cancelError = SchemaFailure.Describe(exception, "cancel the operation");
        }
        finally
        {
            _cancelling = false;
        }
    }

    private void Dismiss()
    {
        if (Workspace is { } workspace)
        {
            Operations.Dismiss(workspace.TreeId);
        }
    }

    private void UpdatePolling()
    {
        var fallbackRunning = (_follower is null || _follower.NotFound) && _report is { InProgress: true };
        if (fallbackRunning && _timer is null)
        {
            _timer = Time.CreateTimer(_ => _ = InvokeAsync(TickAsync), null, PollInterval, PollInterval);
        }
        else if (!fallbackRunning && _timer is not null)
        {
            _timer.Dispose();
            _timer = null;
        }
    }

    private async Task TickAsync()
    {
        if (_lifetime.IsLeft)
        {
            return;
        }

        await ReadAsync();
        await FollowKnownOperationAsync();
        UpdatePolling();
        StateHasChanged();
    }

    private void OnOperationChanged(string treeId)
    {
        if (Workspace is not { } workspace || !string.Equals(treeId, workspace.TreeId, StringComparison.Ordinal))
        {
            return;
        }

        _ = InvokeAsync(async () =>
        {
            if (_lifetime.IsLeft)
            {
                return;
            }

            _local = Operations.Find(treeId);
            await FollowKnownOperationAsync();
            if (_local is { IsActive: false } && !_terminalRefreshDone)
            {
                await ReadAsync();
                await workspace.RefreshAsync();
                _terminalRefreshDone = true;
            }

            UpdatePolling();
            StateHasChanged();
        });
    }

    private void OnFollowedChanged() => _ = InvokeAsync(async () =>
    {
        if (_lifetime.IsLeft)
        {
            return;
        }

        if (_follower is { Status.IsTerminal: true } && !_terminalRefreshDone)
        {
            await ReadAsync();
            if (Workspace is { } workspace)
            {
                await workspace.RefreshAsync();
            }

            _terminalRefreshDone = true;
        }
        else if (_follower is { NotFound: true })
        {
            await ReadAsync();
        }

        UpdatePolling();
        StateHasChanged();
    });

    private void DetachFollower()
    {
        if (_follower is { } follower)
        {
            follower.Changed -= OnFollowedChanged;
            follower.Dispose();
            _follower = null;
        }

        _followedOperationId = null;
        _cancelError = null;
    }

    private IEnumerable<StageItem> ReportSteps() =>
    [
        new(0, SchemaOperationStage.Starting, "Confirmed"),
        new(1, SchemaOperationStage.Running, "Running in the cluster"),
        new(2, SchemaOperationStage.Completed, FinishedTitle),
    ];

    private static IEnumerable<StageItem> OperationSteps(LatticeOperationStatus status)
    {
        var phases = Phases(status);
        for (var i = 0; i < (status.PhaseCount ?? phases.Length); i++)
        {
            var title = i < phases.Length ? SchemaFormat.OperationPhase(phases[i]) : "Phase " + (i + 1).ToString(System.Globalization.CultureInfo.InvariantCulture);
            yield return new StageItem(i, SchemaOperationStage.Running, title);
        }
    }

    private static string[] Phases(LatticeOperationStatus status) => status.Kind switch
    {
        SchemaOperationKinds.AdvanceAndMigrate => AdvanceAndMigratePhases,
        _ => RemediationPhases,
    };

    private static SchemaOperationStage StageOf(LatticeOperationStatus status)
    {
        if (!status.IsTerminal)
        {
            return SchemaOperationStage.Running;
        }

        return status.State switch
        {
            LatticeOperationState.Succeeded => SchemaOperationStage.Completed,
            LatticeOperationState.Cancelled => SchemaOperationStage.Cancelled,
            LatticeOperationState.Failed when status.Result.TryGetValue(SchemaOperationResultKeys.Outcome, out var outcome)
                && string.Equals(outcome, SchemaOperationResultKeys.Aborted, StringComparison.Ordinal) => SchemaOperationStage.Aborted,
            _ => SchemaOperationStage.Failed,
        };
    }

    private sealed record StageItem(int Index, SchemaOperationStage Stage, string Title);
}
