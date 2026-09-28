namespace Orleans.Lattice.Explorer.Shell.Areas.Replication;

/// <summary>
/// A page's refresh cadence that runs only while the page is visible: it ticks
/// every <c>interval</c> on the given clock, stops the moment the page is hidden,
/// and on the page's return refreshes at once and resumes. A tick that arrives
/// while a refresh is still in flight is skipped, so refreshes never overlap.
/// </summary>
internal sealed class ReplicationRefreshLoop : IDisposable
{
    private readonly TimeProvider _time;
    private readonly IReplicationPageVisibility _visibility;
    private readonly TimeSpan _interval;
    private readonly Func<Func<Task>, Task> _dispatch;
    private readonly Func<Task> _refresh;
    private ITimer? _timer;
    private int _inFlight;
    private bool _disposed;

    /// <summary>Creates a stopped cadence.</summary>
    /// <param name="time">The clock the cadence ticks on.</param>
    /// <param name="visibility">The page's visibility.</param>
    /// <param name="interval">How often to refresh.</param>
    /// <param name="dispatch">Runs a refresh on the component's renderer (its <c>InvokeAsync</c>).</param>
    /// <param name="refresh">The refresh itself.</param>
    public ReplicationRefreshLoop(
        TimeProvider time,
        IReplicationPageVisibility visibility,
        TimeSpan interval,
        Func<Func<Task>, Task> dispatch,
        Func<Task> refresh)
    {
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(visibility);
        ArgumentNullException.ThrowIfNull(dispatch);
        ArgumentNullException.ThrowIfNull(refresh);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        _time = time;
        _visibility = visibility;
        _interval = interval;
        _dispatch = dispatch;
        _refresh = refresh;
    }

    /// <summary>Whether the cadence is currently ticking.</summary>
    public bool IsRunning { get; private set; }

    /// <summary>Starts observing visibility and, if the page is visible, the cadence.</summary>
    public async Task StartAsync()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_timer is not null)
        {
            return;
        }

        _timer = _time.CreateTimer(_ => _ = TickAsync(), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        _visibility.Changed += OnVisibilityChanged;
        await _visibility.StartAsync().ConfigureAwait(false);
        Apply(_visibility.IsVisible);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _visibility.Changed -= OnVisibilityChanged;
        _timer?.Dispose();
        _timer = null;
        IsRunning = false;
    }

    private void OnVisibilityChanged()
    {
        if (_disposed)
        {
            return;
        }

        var visible = _visibility.IsVisible;
        Apply(visible);
        if (visible)
        {
            _ = TickAsync();
        }
    }

    private void Apply(bool visible)
    {
        if (_disposed || _timer is not { } timer)
        {
            return;
        }

        IsRunning = visible;
        if (visible)
        {
            timer.Change(_interval, _interval);
        }
        else
        {
            timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        }
    }

    private async Task TickAsync()
    {
        if (_disposed || !_visibility.IsVisible || Interlocked.Exchange(ref _inFlight, 1) == 1)
        {
            return;
        }

        try
        {
            await _dispatch(_refresh).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is ObjectDisposedException or OperationCanceledException)
        {
            // The page went away mid-refresh.
        }
        finally
        {
            Volatile.Write(ref _inFlight, 0);
        }
    }
}
