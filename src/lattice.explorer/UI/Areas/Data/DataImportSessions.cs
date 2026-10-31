using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The imports a circuit is running (issue #5365), one per tree. An import belongs to the
/// circuit rather than to its dialog: closing the dialog leaves it running, and reopening
/// the dialog on the same tree shows it again. When the circuit ends, this service is
/// disposed and every import still running is cancelled and recorded as cancelled.
/// </summary>
/// <remarks>
/// An import is filed under the caller that started it (<see cref="ShellCallerKey"/>). It is
/// shown only to that caller, and it is cancelled as soon as the caller changes - a sign-in,
/// a sign-out, a new endpoint or another tenant - because every call it makes carries the
/// credential and tenant of the moment, and an import must never go on writing, or show its
/// refused keys, as someone else.
/// </remarks>
internal sealed class DataImportSessions : IDisposable
{
    private static readonly TimeSpan CallerCheckInterval = TimeSpan.FromMilliseconds(500);

    private readonly Dictionary<string, Entry> _runs = new(StringComparer.Ordinal);
    private readonly ComponentLifetime _circuit = new();
    private readonly ShellCaller _caller;
    private readonly Lock _gate = new();
    private bool _disposed;

    /// <summary>Creates the circuit's imports.</summary>
    /// <param name="services">The circuit's services, from which the caller is read.</param>
    public DataImportSessions(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _caller = ShellCaller.Of(services);
    }

    /// <summary>The import the current caller is running or last ran on <paramref name="treeId"/>, or <see langword="null"/>.</summary>
    /// <param name="treeId">The tree.</param>
    public DataImportRun? Find(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var caller = _caller.Current;
        lock (_gate)
        {
            return _runs.TryGetValue(treeId, out var entry) && entry.Caller == caller ? entry.Run : null;
        }
    }

    /// <summary>Starts <paramref name="run"/> for the current caller, replacing the finished import on its tree.</summary>
    /// <param name="run">The import.</param>
    /// <exception cref="InvalidOperationException">An import is already running on the tree.</exception>
    public void Start(DataImportRun run)
    {
        ArgumentNullException.ThrowIfNull(run);
        var caller = _caller.Current;
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

            var token = _circuit.Token;
            _runs[run.TreeId] = new Entry(caller, run, Task.Run(() => RunAsCallerAsync(run, caller, token)));
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
        Entry[] runs;
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

        _circuit.Leave();
        foreach (var entry in runs)
        {
            // Bounded: a cancelled import ends at its next read or write.
            entry.Running.Wait(TimeSpan.FromSeconds(5));
            entry.Run.Dispose();
        }
    }

    /// <summary>Runs <paramref name="run"/>, cancelling it if the circuit ends or the caller changes.</summary>
    private async Task RunAsCallerAsync(DataImportRun run, ShellCallerKey caller, CancellationToken circuit)
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(circuit);
        var running = run.RunAsync(stop.Token);
        while (!running.IsCompleted)
        {
            await Task.WhenAny(running, Task.Delay(CallerCheckInterval, CancellationToken.None)).ConfigureAwait(false);
            if (!running.IsCompleted && _caller.Current != caller)
            {
                await stop.CancelAsync().ConfigureAwait(false);
            }
        }

        await running.ConfigureAwait(false);
    }

    private readonly record struct Entry(ShellCallerKey Caller, DataImportRun Run, Task Running);
}