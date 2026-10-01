using System.Collections.Concurrent;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Operations;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The long-running schema operations this circuit started, one per tree, kept
/// apart from any page so an operation keeps running - and its outcome is kept -
/// while the operator looks elsewhere. The cluster owns the operation itself; its
/// status page also reads the cluster's own status, so the page resumes even for
/// an operation another circuit started.
/// </summary>
internal sealed class SchemaOperations : IDisposable
{
    private readonly ConcurrentDictionary<string, SchemaOperation> _operations = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, OperationFollower> _followers = new(StringComparer.Ordinal);
    private readonly ComponentLifetime _circuit = new();
    private readonly SchemaFacades _facades;
    private readonly TimeProvider _time;

    /// <summary>Creates the tracker.</summary>
    /// <param name="time">The clock start, finish and follow reads are paced on.</param>
    /// <param name="facades">The schema facades for this circuit.</param>
    public SchemaOperations(TimeProvider time, SchemaFacades facades)
    {
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(facades);
        _time = time;
        _facades = facades;
    }

    /// <summary>Raised, possibly off the circuit's thread, whenever an operation moves on, with its tree id.</summary>
    public event Action<string>? Changed;

    /// <summary>The operation this circuit last started on <paramref name="treeId"/>, or <see langword="null"/>.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <returns>The operation.</returns>
    public SchemaOperation? Find(string treeId) =>
        _operations.TryGetValue(treeId, out var operation) ? operation : null;

    /// <summary>
    /// Starts <paramref name="run"/> as the operation on <paramref name="treeId"/>
    /// and returns at once; the operation moves on in the background.
    /// </summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="kind">What the operation does.</param>
    /// <param name="summary">One plain line describing it.</param>
    /// <param name="run">The facade call that accepts the operation and returns its handle.</param>
    /// <returns>The started operation.</returns>
    /// <exception cref="InvalidOperationException">An operation this circuit started on the tree is still running.</exception>
    public SchemaOperation Start(
        string treeId,
        SchemaOperationKind kind,
        string summary,
        Func<CancellationToken, Task<LatticeOperationHandle>> run)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrWhiteSpace(summary);
        ArgumentNullException.ThrowIfNull(run);

        var operation = new SchemaOperation(treeId, kind, summary, _time.GetUtcNow());
        var added = false;
        _operations.AddOrUpdate(
            treeId,
            _ =>
            {
                added = true;
                return operation;
            },
            (_, existing) =>
            {
                if (existing.IsActive)
                {
                    return existing;
                }

                added = true;
                return operation;
            });

        if (!added)
        {
            throw new InvalidOperationException("An operation on this tree is still running. Wait for it to finish.");
        }

        Changed?.Invoke(treeId);
        _ = RunAsync(operation, run);
        return operation;
    }

    /// <summary>Forgets a finished operation on <paramref name="treeId"/>; a running one is kept.</summary>
    /// <param name="treeId">The logical tree id.</param>
    public void Dismiss(string treeId)
    {
        if (_operations.TryGetValue(treeId, out var operation) && !operation.IsActive
            && _operations.TryRemove(new KeyValuePair<string, SchemaOperation>(treeId, operation)))
        {
            Changed?.Invoke(treeId);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _circuit.Leave();
        foreach (var follower in _followers.Values)
        {
            follower.Dispose();
        }

        _followers.Clear();
    }

    private async Task RunAsync(SchemaOperation started, Func<CancellationToken, Task<LatticeOperationHandle>> run)
    {
        try
        {
            var handle = await run(_circuit.Token).ConfigureAwait(false);
            var accepted = started with
            {
                Stage = SchemaOperationStage.Running,
                OperationId = handle.OperationId,
            };
            Update(accepted);
            await FollowAsync(accepted).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_circuit.IsLeft)
        {
            // The circuit ended; the cluster keeps the operation and its status page resumes it.
            return;
        }
        catch (Exception exception)
        {
            Update(started with
            {
                Stage = SchemaOperationStage.Failed,
                Failure = SchemaFailure.Describe(exception, Action(started.Kind)),
                FinishedAt = _time.GetUtcNow(),
            });
        }
    }

    private async Task FollowAsync(SchemaOperation accepted)
    {
        if (accepted.OperationId is not { } operationId)
        {
            return;
        }

        var follower = new OperationFollower(_time);
        if (_followers.TryGetValue(accepted.TreeId, out var previous))
        {
            // The tree's earlier operation has settled (a start needs it inactive);
            // release its follower rather than leaving it to the circuit's end.
            previous.Dispose();
        }

        _followers[accepted.TreeId] = follower;
        follower.Changed += () => OnFollowerChanged(accepted.TreeId, follower);
        try
        {
            await follower.StartAsync(
                ct => _facades.RequireSchemaOperations().GetOperationStatusAsync(operationId, ct),
                _circuit.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_circuit.IsLeft)
        {
        }
    }

    private void OnFollowerChanged(string treeId, OperationFollower follower)
    {
        if (!_operations.TryGetValue(treeId, out var current) || current.OperationId is null)
        {
            return;
        }

        if (follower.Status is { } status)
        {
            Update(current with
            {
                Stage = StageOf(status),
                Status = status,
                Failure = status.State == LatticeOperationState.Failed ? status.FailureReason : current.Failure,
                FinishedAt = status.FinishedAtUtc,
            });
        }
        else if (follower.NotFound)
        {
            Update(current with
            {
                Stage = SchemaOperationStage.Failed,
                Failure = "The operation is no longer visible.",
                FinishedAt = _time.GetUtcNow(),
            });
        }
    }

    private void Update(SchemaOperation operation)
    {
        _operations[operation.TreeId] = operation;
        Changed?.Invoke(operation.TreeId);
    }

    private static string Action(SchemaOperationKind kind) => kind switch
    {
        SchemaOperationKind.Remediate => "remediate this tree",
        SchemaOperationKind.AdvanceAndMigrate => "advance and migrate this tree",
        _ => "migrate this tree",
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
}
