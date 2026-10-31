using System.Collections.Concurrent;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.Silo;

/// <summary>
/// A test-only hold on one tree's compliance scan, armed through <c>/e2e/compliance-delay</c>
/// when the fault seams are on. A scan of an armed tree waits, Running in its first phase,
/// until the arm's deadline before it reads a value, so a journey can act on a scan that is
/// still running (U2 stops one; C2 kills the silo running one). An arm is bounded by
/// <see cref="MaximumDelay"/>, is spent by the first scan of its tree on this silo, lapses at
/// its deadline whether or not a scan spent it, and can be withdrawn at any time. The hold
/// ends at once when the scan's operation is cancelled.
/// </summary>
internal sealed class JourneyComplianceScanDelay(TimeProvider clock)
{
    /// <summary>The longest hold an arm may ask for.</summary>
    public static readonly TimeSpan MaximumDelay = TimeSpan.FromMinutes(3);

    private readonly ConcurrentDictionary<string, DateTimeOffset> _armed = new(StringComparer.Ordinal);

    /// <summary>Arms a hold on <paramref name="treeId"/>'s next scan, ending <paramref name="delay"/> from now.</summary>
    /// <param name="treeId">The tree, as the scan names it.</param>
    /// <param name="delay">How long from now the hold lasts; at most <see cref="MaximumDelay"/>.</param>
    /// <returns>When the hold ends.</returns>
    public DateTimeOffset Arm(string treeId, TimeSpan delay)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        if (delay <= TimeSpan.Zero || delay > MaximumDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(delay), delay, $"A hold lasts more than 0 and at most {MaximumDelay.TotalSeconds:0} seconds.");
        }

        var until = clock.GetUtcNow() + delay;
        _armed[treeId] = until;
        return until;
    }

    /// <summary>Withdraws any hold armed on <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The tree.</param>
    /// <returns>Whether a hold was armed.</returns>
    public bool Disarm(string treeId) => _armed.TryRemove(treeId, out _);

    /// <summary>Spends <paramref name="treeId"/>'s hold, if one is armed and live, and waits it out.</summary>
    /// <param name="treeId">The tree being scanned.</param>
    /// <param name="cancellationToken">The scan's cancellation, which ends the hold at once.</param>
    public async Task HoldAsync(string treeId, CancellationToken cancellationToken)
    {
        if (!_armed.TryRemove(treeId, out var until))
        {
            return;
        }

        var remaining = until - clock.GetUtcNow();
        if (remaining > TimeSpan.Zero)
        {
            await Task.Delay(remaining, clock, cancellationToken).ConfigureAwait(false);
        }
    }
}
