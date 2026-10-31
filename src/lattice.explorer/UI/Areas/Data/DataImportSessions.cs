namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The imports a circuit is running (issue #5365), one per tree. An import belongs to the
/// circuit rather than to its dialog: closing the dialog leaves it running, and reopening
/// the dialog on the same tree shows it again. When the circuit ends, this service is
/// disposed and every import still running is cancelled and recorded as cancelled.
/// </summary>
internal sealed class DataImportSessions : IDisposable
{
    private readonly Dictionary<string, (DataImportRun Run, Task Running)> _runs = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _circuit = new();
    private readonly Lock _gate = new();
    private bool _disposed;

    /// <summary>The import running or last run on <paramref name="treeId"/>, or <see langword="null"/>.</summary>
    /// <param name="treeId">The tree.</param>
    public DataImportRun? Find(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        lock (_gate)
        {
            return _runs.TryGetValue(treeId, out var entry) ? entry.Run : null;
        }
    }

    /// <summary>Starts <paramref name="run"/>, replacing the finished import on its tree.</summary>
    /// <param name="run">The import.</param>
    /// <exception cref="InvalidOperationException">An import is already running on the tree.</exception>
    public void Start(DataImportRun run)
    {
        ArgumentNullException.ThrowIfNull(run);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_runs.TryGetValue(run.TreeId, out var existing))
            {
                if (!existing.Run.IsFinished)
                {
                    throw new InvalidOperationException("An import is already running on this tree.");
                }

                existing.Run.Dispose();
            }

            _runs[run.TreeId] = (run, Task.Run(() => run.RunAsync(_circuit.Token)));
        }
    }

    /// <summary>Forgets the finished import on <paramref name="treeId"/>, deleting its list of refused lines.</summary>
    /// <param name="treeId">The tree.</param>
    public void Forget(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        lock (_gate)
        {
            if (_runs.TryGetValue(treeId, out var entry) && entry.Run.IsFinished)
            {
                _runs.Remove(treeId);
                entry.Run.Dispose();
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        (DataImportRun Run, Task Running)[] runs;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            runs = [.. _runs.Values];
            _runs.Clear();
        }

        _circuit.Cancel();
        foreach (var (run, running) in runs)
        {
            // Bounded: a cancelled import ends at its next read or write.
            running.Wait(TimeSpan.FromSeconds(5));
            run.Dispose();
        }

        _circuit.Dispose();
    }
}
