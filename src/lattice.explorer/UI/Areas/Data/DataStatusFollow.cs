namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The cadence a Data tab follows a transient state on (a view that is behind, a
/// tag index reconciling, a non-zero view lag): while the tab says the state is
/// transient it re-reads every <see cref="Interval"/> on the injected clock, and it
/// stops once the state settles or the tab is left (#5519, #5549). A tick that
/// arrives while the previous one is still reading is skipped, so reads never overlap.
/// </summary>
internal sealed class DataStatusFollow : IDisposable
{
    /// <summary>How often a transient state is re-read.</summary>
    internal static readonly TimeSpan Interval = TimeSpan.FromSeconds(2);

    private readonly TimeProvider _time;
    private readonly Func<Func<Task>, Task> _dispatch;
    private readonly Func<Task> _tick;
    private ITimer? _timer;
    private bool _inFlight;
    private bool _disposed;

    /// <summary>Creates a stopped follow.</summary>
    /// <param name="time">The clock the follow ticks on.</param>
    /// <param name="dispatch">Runs a tick on the component's renderer (its <c>InvokeAsync</c>).</param>
    /// <param name="tick">One re-read.</param>
    public DataStatusFollow(TimeProvider time, Func<Func<Task>, Task> dispatch, Func<Task> tick)
    {
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(dispatch);
        ArgumentNullException.ThrowIfNull(tick);
        _time = time;
        _dispatch = dispatch;
        _tick = tick;
    }

    /// <summary>Whether the follow is ticking.</summary>
    public bool IsRunning => _timer is not null;

    /// <summary>Starts ticking while <paramref name="transient"/> holds, and stops otherwise.</summary>
    /// <param name="transient">Whether the tab shows a transient state.</param>
    public void Update(bool transient)
    {
        if (!transient || _disposed)
        {
            Stop();
            return;
        }

        _timer ??= _time.CreateTimer(_ => _ = TickAsync(), null, Interval, Interval);
    }

    /// <summary>Stops ticking.</summary>
    public void Stop()
    {
        _timer?.Dispose();
        _timer = null;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _disposed = true;
        Stop();
    }

    private async Task TickAsync()
    {
        try
        {
            await _dispatch(async () =>
            {
                if (_disposed || _inFlight)
                {
                    return;
                }

                _inFlight = true;
                try
                {
                    await _tick();
                }
                finally
                {
                    _inFlight = false;
                }
            });
        }
        catch (Exception ex) when (ex is ObjectDisposedException or OperationCanceledException)
        {
            // The tab went away mid-tick.
        }
    }
}
