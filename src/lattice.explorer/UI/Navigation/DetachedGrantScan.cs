namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// One in-flight per-tree grant scan that outlives the directory's time-box: a
/// check that stops waiting leaves the scan running, and the next check for the
/// same caller joins it instead of starting it again.
/// </summary>
/// <remarks>
/// <para>
/// A denied caller's area availability falls back to probing the catalogue's trees
/// a wave at a time. On a busy cluster that can take longer than the directory
/// waits; were the scan cancelled with the check, every recheck and every
/// navigation would restart it from nothing and the area would never be answered
/// (#6114). The scan instead runs on its own token, bounded by
/// <see cref="Lifetime"/> and cancelled by <see cref="Reset"/>, and each waiter
/// only stops waiting for it.
/// </para>
/// <para>
/// A completed scan is never reused: its owner remembers the answer, and an
/// unanswered (<see langword="null"/>) result must be asked again. A scan cancelled
/// by <see cref="Reset"/> or by its lifetime reads as unanswered.
/// </para>
/// </remarks>
internal sealed class DetachedGrantScan : IDisposable
{
    /// <summary>The longest a scan runs once nobody may be left to join it.</summary>
    public static readonly TimeSpan Lifetime = TimeSpan.FromMinutes(2);

    private readonly object _gate = new();
    private Task<bool?>? _scan;
    private object? _key;
    private CancellationTokenSource? _cancellation;

    /// <summary>Joins the running scan for <paramref name="key"/>, or starts one.</summary>
    /// <param name="key">The caller the scan answers for; a scan for another caller is cancelled and replaced.</param>
    /// <param name="scan">Starts the scan on the token it is given.</param>
    /// <param name="cancellationToken">Stops this waiter waiting; the scan runs on.</param>
    /// <returns>The scan's answer; <see langword="null"/> when it was not answered.</returns>
    public Task<bool?> JoinAsync(object key, Func<CancellationToken, Task<bool?>> scan, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(scan);

        Task<bool?> running;
        lock (_gate)
        {
            if (_scan is null || _scan.IsCompleted || !Equals(_key, key))
            {
                CancelLocked();
                var cancellation = new CancellationTokenSource(Lifetime);
                _cancellation = cancellation;
                _key = key;
                _scan = RunAsync(scan, cancellation.Token);
            }

            running = _scan;
        }

        return running.WaitAsync(cancellationToken);
    }

    /// <summary>Cancels the running scan, so the next check starts afresh.</summary>
    public void Reset()
    {
        lock (_gate)
        {
            CancelLocked();
        }
    }

    /// <inheritdoc />
    public void Dispose() => Reset();

    private static async Task<bool?> RunAsync(Func<CancellationToken, Task<bool?>> scan, CancellationToken cancellationToken)
    {
        // Yield first, so the scan never runs inline under the caller's lock.
        await Task.Yield();
        try
        {
            return await scan(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return null;
        }
    }

    private void CancelLocked()
    {
        if (_cancellation is { } cancellation)
        {
            // Not disposed: the scan may still be observing its token. The lifetime
            // timer releases the source once it fires.
            cancellation.Cancel();
        }

        _cancellation = null;
        _scan = null;
        _key = null;
    }
}
