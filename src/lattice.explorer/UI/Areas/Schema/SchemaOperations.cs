using System.Collections.Concurrent;
using Orleans.Lattice.Schema;

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
    private readonly CancellationTokenSource _circuit = new();
    private readonly TimeProvider _time;

    /// <summary>Creates the tracker.</summary>
    /// <param name="time">The clock start and finish times are read from.</param>
    public SchemaOperations(TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(time);
        _time = time;
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
    /// <param name="run">The facade call, which returns the terminal report.</param>
    /// <returns>The started operation.</returns>
    /// <exception cref="InvalidOperationException">An operation this circuit started on the tree is still running.</exception>
    public SchemaOperation Start(
        string treeId,
        SchemaOperationKind kind,
        string summary,
        Func<CancellationToken, Task<LatticeSchemaRemediationReport>> run)
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
        _circuit.Cancel();
        _circuit.Dispose();
    }

    private async Task RunAsync(SchemaOperation started, Func<CancellationToken, Task<LatticeSchemaRemediationReport>> run)
    {
        Update(started with { Stage = SchemaOperationStage.Running });
        SchemaOperation finished;
        try
        {
            var report = await run(_circuit.Token).ConfigureAwait(false);
            finished = started with
            {
                Stage = report.DidAbort ? SchemaOperationStage.Aborted : SchemaOperationStage.Completed,
                Report = report,
                FinishedAt = _time.GetUtcNow(),
            };
        }
        catch (OperationCanceledException) when (_circuit.IsCancellationRequested)
        {
            // The circuit ended; the cluster keeps the operation and its status page resumes it.
            return;
        }
        catch (Exception exception)
        {
            finished = started with
            {
                Stage = SchemaOperationStage.Failed,
                Failure = SchemaFailure.Describe(exception, Action(started.Kind)),
                FinishedAt = _time.GetUtcNow(),
            };
        }

        Update(finished);
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
}
