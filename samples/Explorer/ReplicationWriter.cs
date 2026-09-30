using System.Text;

namespace Orleans.Lattice.Samples.Explorer;

/// <summary>
/// A small background writer that keeps the replication links busy, so the
/// Replication area shows live contact, backlog and health. Each tick it
/// overwrites one key per target, cycling through a fixed key set: the trees
/// stay the same size however long the sample runs.
/// </summary>
internal sealed class ReplicationWriter : IAsyncDisposable
{
    private readonly IReadOnlyList<ReplicationWriterTarget> _targets;
    private readonly TimeSpan _interval;
    private readonly CancellationTokenSource _stop = new();
    private Task? _loop;
    private long _ticks;

    /// <summary>Creates a writer; nothing is written until <see cref="Start"/> or <see cref="WriteOnceAsync"/>.</summary>
    /// <param name="targets">The trees written, one key each per tick.</param>
    /// <param name="interval">The time between ticks.</param>
    public ReplicationWriter(IReadOnlyList<ReplicationWriterTarget> targets, TimeSpan interval)
    {
        ArgumentNullException.ThrowIfNull(targets);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        _targets = targets;
        _interval = interval;
    }

    /// <summary>The number of ticks written so far.</summary>
    public long Ticks => Interlocked.Read(ref _ticks);

    /// <summary>The key a target writes on tick <paramref name="tick"/>: one of its <see cref="ReplicationWriterTarget.KeyCount"/> keys, in turn.</summary>
    /// <param name="target">The target.</param>
    /// <param name="tick">The zero-based tick.</param>
    public static string KeyFor(ReplicationWriterTarget target, long tick)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentOutOfRangeException.ThrowIfNegative(tick);
        return $"{target.KeyPrefix}{tick % target.KeyCount:D3}";
    }

    /// <summary>Starts writing on a timer. Calling it again does nothing.</summary>
    public void Start() => _loop ??= RunAsync(_stop.Token);

    /// <summary>Writes one tick: one key per target.</summary>
    /// <param name="cancellationToken">Cancels the tick.</param>
    public async Task WriteOnceAsync(CancellationToken cancellationToken = default)
    {
        var tick = Interlocked.Increment(ref _ticks) - 1;
        var value = Encoding.UTF8.GetBytes($"status-{tick}");

        // A trusted co-hosted writer, like the seeding: system-origin bypasses
        // the deny-by-default gate for this process's own writes.
        using var _ = LatticeSystemOrigin.Enter();
        for (var i = 0; i < _targets.Count; i++)
        {
            var target = _targets[i];
            await target.Tree.SetAsync(KeyFor(target, tick), value, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync().ConfigureAwait(false);
        if (_loop is not null)
        {
            await _loop.ConfigureAwait(false);
        }

        _stop.Dispose();
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_interval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                try
                {
                    await WriteOnceAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    // A write that fails (a silo still starting or already
                    // stopping) is skipped; the next tick tries again.
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Stopped.
        }
    }
}
