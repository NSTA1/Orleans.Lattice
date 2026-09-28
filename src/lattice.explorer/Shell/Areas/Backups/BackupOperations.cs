using System.Globalization;

namespace Orleans.Lattice.Explorer.Shell.Areas.Backups;

/// <summary>
/// The circuit's staged backup operations. An operation runs independently of
/// the page that started it, so the user can leave its status page and come
/// back to it; ending the circuit cancels whatever is still running.
/// </summary>
internal sealed class BackupOperations : IDisposable
{
    private readonly TimeProvider _time;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly object _gate = new();
    private readonly List<BackupOperation> _operations = [];
    private int _next;

    /// <summary>Creates the circuit's operation list.</summary>
    /// <param name="time">The clock operation times are read from.</param>
    public BackupOperations(TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(time);
        _time = time;
    }

    /// <summary>Every operation started in this circuit, newest first.</summary>
    public IReadOnlyList<BackupOperation> Recent
    {
        get
        {
            lock (_gate)
            {
                return [.. Enumerable.Reverse(_operations)];
            }
        }
    }

    /// <summary>
    /// Starts an operation and returns it at once, running <paramref name="work"/>
    /// in the background. The work advances the operation through its stages and
    /// may report results; a fault fails it with a plain sentence, and returning
    /// without finishing it succeeds it.
    /// </summary>
    /// <param name="kind">What it does.</param>
    /// <param name="title">Its title.</param>
    /// <param name="stages">Its stages, in order.</param>
    /// <param name="work">The work, given the operation and the circuit's lifetime token.</param>
    public BackupOperation Start(
        BackupOperationKind kind,
        string title,
        IReadOnlyList<string> stages,
        Func<BackupOperation, CancellationToken, Task> work)
    {
        ArgumentNullException.ThrowIfNull(work);
        ObjectDisposedException.ThrowIf(_lifetime.IsCancellationRequested, this);

        BackupOperation operation;
        lock (_gate)
        {
            _next++;
            operation = new BackupOperation(_next.ToString(CultureInfo.InvariantCulture), kind, title, stages, _time);
            _operations.Add(operation);
        }

        _ = RunAsync(operation, work, _lifetime.Token);
        return operation;
    }

    /// <summary>The operation with <paramref name="id"/>, or <see langword="null"/>.</summary>
    /// <param name="id">The operation id.</param>
    public BackupOperation? Find(string? id)
    {
        if (string.IsNullOrEmpty(id))
        {
            return null;
        }

        lock (_gate)
        {
            return _operations.Find(operation => string.Equals(operation.Id, id, StringComparison.Ordinal));
        }
    }

    /// <summary>The most recent operation of <paramref name="kind"/>, or <see langword="null"/>.</summary>
    /// <param name="kind">The kind.</param>
    public BackupOperation? Latest(BackupOperationKind kind)
    {
        lock (_gate)
        {
            return _operations.FindLast(operation => operation.Kind == kind);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (!_lifetime.IsCancellationRequested)
        {
            _lifetime.Cancel();
        }

        _lifetime.Dispose();
    }

    private static async Task RunAsync(BackupOperation operation, Func<BackupOperation, CancellationToken, Task> work, CancellationToken lifetime)
    {
        try
        {
            await work(operation, lifetime).ConfigureAwait(false);
            operation.Succeed("Done.");
        }
        catch (OperationCanceledException) when (lifetime.IsCancellationRequested)
        {
            operation.Cancel();
        }
        catch (Exception exception)
        {
            operation.Fail(BackupsFaults.Describe(exception));
        }
    }
}
