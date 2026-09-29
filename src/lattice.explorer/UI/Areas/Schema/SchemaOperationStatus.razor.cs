using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The resumable status page of a tree's long-running schema operation: the
/// operation this session started, if any, joined with the cluster's own
/// remediation status, re-read every <see cref="PollInterval"/> while it runs.
/// </summary>
public partial class SchemaOperationStatus : IDisposable
{
    /// <summary>How often a running operation's status is re-read.</summary>
    public static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(2);

    private static readonly (SchemaOperationStage Step, string Title)[] StepTitles =
    [
        (SchemaOperationStage.Starting, "Confirmed"),
        (SchemaOperationStage.Running, "Running in the cluster"),
        (SchemaOperationStage.Completed, "Finished"),
    ];

    private readonly CancellationTokenSource _lifetime = new();
    private SchemaOperation? _local;
    private LatticeSchemaRemediationReport? _report;
    private string? _loadedTree;
    private string? _statusError;
    private ITimer? _timer;
    private bool _read;
    private bool _reading;

    [CascadingParameter]
    internal SchemaWorkspace? Workspace { get; set; }

    [Inject]
    internal SchemaFacades Facades { get; set; } = default!;

    [Inject]
    internal SchemaOperations Operations { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    /// <summary>Whether a timer is re-reading the status.</summary>
    internal bool IsPolling => _timer is not null;

    /// <summary>
    /// Where the operation is: this session's own operation when it started one,
    /// otherwise the cluster's report; <see langword="null"/> when nothing has run.
    /// </summary>
    internal SchemaOperationStage? Stage
    {
        get
        {
            if (_local is { } local)
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
                _ => null,
            };
        }
    }

    private LatticeSchemaRemediationReport? Report => _local?.Report is { } terminal && !_local.IsActive ? terminal : _report;

    private IEnumerable<(SchemaOperationStage Step, string Title)> Steps =>
        StepTitles.Select(step => step.Step == SchemaOperationStage.Completed ? (step.Step, FinishedTitle) : step);

    private string FinishedTitle => Stage switch
    {
        SchemaOperationStage.Aborted => "Stopped, nothing cut over",
        SchemaOperationStage.Failed => "Refused",
        _ => "Finished",
    };

    private string Headline => Stage switch
    {
        SchemaOperationStage.Starting => "Asking the cluster to start.",
        SchemaOperationStage.Running => _local is null ? "An operation is running on this tree." : "Running in the cluster.",
        SchemaOperationStage.Completed => "The last operation finished, and the tree serves its result.",
        SchemaOperationStage.Aborted => "The last operation stopped at a value that still fails.",
        SchemaOperationStage.Failed => "The cluster did not run the operation.",
        _ => string.Empty,
    };

    /// <inheritdoc />
    public void Dispose()
    {
        Operations.Changed -= OnOperationChanged;
        _timer?.Dispose();
        _timer = null;
        _lifetime.Cancel();
        _lifetime.Dispose();
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
            await ReadAsync();
            UpdatePolling();
        }
    }

    private int StepPosition(SchemaOperationStage step)
    {
        var current = Stage switch
        {
            SchemaOperationStage.Aborted or SchemaOperationStage.Failed => SchemaOperationStage.Completed,
            { } stage => stage,
            null => SchemaOperationStage.Starting,
        };

        // A finished operation has no step left to take: every step is done.
        if (current == SchemaOperationStage.Completed)
        {
            return step == SchemaOperationStage.Completed ? 0 : -1;
        }

        return step.CompareTo(current);
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
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
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

    private async Task RefreshAsync()
    {
        await ReadAsync();
        UpdatePolling();
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
        var running = _local is { IsActive: true } || _report is { InProgress: true };
        if (running && _timer is null)
        {
            _timer = Time.CreateTimer(_ => _ = InvokeAsync(TickAsync), null, PollInterval, PollInterval);
        }
        else if (!running && _timer is not null)
        {
            _timer.Dispose();
            _timer = null;
        }
    }

    private async Task TickAsync()
    {
        if (_lifetime.IsCancellationRequested)
        {
            return;
        }

        await ReadAsync();
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
            if (_lifetime.IsCancellationRequested)
            {
                return;
            }

            var before = _local;
            _local = Operations.Find(treeId);
            if (before is { IsActive: true } && _local is { IsActive: false })
            {
                // The operation ended: read the cluster's final word, and let the
                // heading and every tab see what it changed.
                await ReadAsync();
                await workspace.RefreshAsync();
            }

            UpdatePolling();
            StateHasChanged();
        });
    }
}
