namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The production <see cref="ISubjectSearchDebounce"/>: defers each scheduled
/// action behind a <see cref="Task.Delay(TimeSpan, CancellationToken)"/> and
/// cancels any still-pending action when a newer one is scheduled, so a rapid
/// keystroke burst collapses to a single directory search once input settles.
/// One instance is held per picker (registered transient) because it carries the
/// single in-flight timer for that picker.
/// </summary>
internal sealed class TimerSubjectSearchDebounce : ISubjectSearchDebounce, IDisposable
{
    /// <summary>The default settle interval applied between the last keystroke and the search.</summary>
    internal static readonly TimeSpan DefaultInterval = TimeSpan.FromMilliseconds(250);

    private readonly TimeSpan _interval;
    private CancellationTokenSource? _pending;
    private bool _disposed;

    /// <summary>Creates a debounce with the <see cref="DefaultInterval"/>.</summary>
    public TimerSubjectSearchDebounce()
        : this(DefaultInterval)
    {
    }

    /// <summary>Creates a debounce with an explicit settle <paramref name="interval"/>.</summary>
    /// <param name="interval">The settle interval between the last keystroke and the deferred search.</param>
    public TimerSubjectSearchDebounce(TimeSpan interval) => _interval = interval;

    /// <inheritdoc />
    public void Schedule(Func<Task> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        if (_disposed)
        {
            return;
        }

        _pending?.Cancel();
        _pending?.Dispose();
        var cts = new CancellationTokenSource();
        _pending = cts;
        _ = RunAsync(action, cts.Token);
    }

    private async Task RunAsync(Func<Task> action, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(_interval, cancellationToken).ConfigureAwait(false);
            await action().ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Superseded by a newer keystroke burst; the newer schedule owns the search.
        }
    }

    /// <summary>Cancels any pending action and releases the timer resources.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _pending?.Cancel();
        _pending?.Dispose();
        _pending = null;
    }
}
