namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Sender-side additive-increase / multiplicative-decrease (AIMD)
/// controller for the per-<c>(tree, peer)</c> outbound batch size.
/// Tracks recent ack latency over a fixed-length sliding window and
/// nudges an effective batch size within the closed interval
/// <c>[1, maxBatchSize]</c>: it grows the size additively while the
/// window-mean ack latency stays at or below a threshold (and the link
/// is healthy), and shrinks it multiplicatively when the mean rises
/// above the threshold or a send fails.
/// <para>
/// Pure in-memory and not serialized: the controller is owned by a
/// single shipper activation, so its state is reconstructed on
/// reactivation rather than persisted. Not thread-safe; the owning
/// shipper grain serialises all access through Orleans's single-turn
/// model. Every mutator is allocation-free - the sliding window is a
/// circular buffer sized once at construction.
/// </para>
/// </summary>
internal sealed class AdaptiveBatchSizeController
{
    private readonly int _maxBatchSize;
    private readonly int _additiveIncrement;
    private readonly double _multiplicativeDecreaseFactor;
    private readonly double _latencyThresholdMs;
    private readonly double[] _window;
    private int _windowCount;
    private int _windowHead;
    private double _windowSumMs;
    private double _effectiveSize;

    /// <summary>
    /// Initialises a controller that adapts the effective batch size
    /// within <c>[1, <paramref name="maxBatchSize"/>]</c>. The effective
    /// size starts at <paramref name="maxBatchSize"/> (the optimistic
    /// posture - a healthy link stays at the configured ceiling and only
    /// backs off on observed degradation).
    /// </summary>
    /// <param name="maxBatchSize">The configured ceiling
    /// (<see cref="LatticeReplicationOptions.ShipBatchSize"/>); the
    /// effective size never exceeds it. Must be at least <c>1</c>.</param>
    /// <param name="additiveIncrement">Entries added to the effective size
    /// on each healthy ack. Must be at least <c>1</c>.</param>
    /// <param name="multiplicativeDecreaseFactor">Factor the effective size
    /// is multiplied by on a latency rise or send failure. Must be in the
    /// open interval <c>(0.0, 1.0)</c>.</param>
    /// <param name="latencyThreshold">Sliding-window mean ack latency at or
    /// below which the controller increases (above which it decreases).
    /// Must be strictly positive.</param>
    /// <param name="windowLength">Number of recent acks averaged. Must be
    /// at least <c>1</c>.</param>
    public AdaptiveBatchSizeController(
        int maxBatchSize,
        int additiveIncrement,
        double multiplicativeDecreaseFactor,
        TimeSpan latencyThreshold,
        int windowLength)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxBatchSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(additiveIncrement, 1);
        if (multiplicativeDecreaseFactor is <= 0.0 or >= 1.0 || double.IsNaN(multiplicativeDecreaseFactor))
        {
            throw new ArgumentOutOfRangeException(
                nameof(multiplicativeDecreaseFactor),
                multiplicativeDecreaseFactor,
                "The multiplicative-decrease factor must be in the open interval (0.0, 1.0).");
        }
        if (latencyThreshold <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(latencyThreshold),
                latencyThreshold,
                "The latency threshold must be strictly greater than TimeSpan.Zero.");
        }
        ArgumentOutOfRangeException.ThrowIfLessThan(windowLength, 1);

        _maxBatchSize = maxBatchSize;
        _additiveIncrement = additiveIncrement;
        _multiplicativeDecreaseFactor = multiplicativeDecreaseFactor;
        _latencyThresholdMs = latencyThreshold.TotalMilliseconds;
        _window = new double[windowLength];
        _effectiveSize = maxBatchSize;
    }

    /// <summary>
    /// The current effective batch size, clamped to
    /// <c>[1, maxBatchSize]</c>. The shipper composes this with the
    /// configured ceiling and any active receiver flow-control hint as a
    /// minimum, so this value is only ever a lower-or-equal bound on the
    /// actual per-tick cap.
    /// </summary>
    public int CurrentBatchSize => Math.Clamp((int)_effectiveSize, 1, _maxBatchSize);

    /// <summary>
    /// The sliding-window mean ack latency, or <see langword="null"/>
    /// when no ack has been recorded yet. Diagnostic surface.
    /// </summary>
    public TimeSpan? WindowAckLatency =>
        _windowCount == 0 ? null : TimeSpan.FromMilliseconds(_windowSumMs / _windowCount);

    /// <summary>
    /// Records a successful ack with its measured latency. Pushes the
    /// sample onto the sliding window and applies the AIMD rule: additive
    /// increase when the window mean is at or below the configured
    /// threshold, multiplicative decrease when it is above. A negative
    /// latency (clock skew) is floored at zero.
    /// </summary>
    public void RecordAck(TimeSpan ackLatency)
    {
        var ms = Math.Max(0.0, ackLatency.TotalMilliseconds);
        PushWindow(ms);
        var mean = _windowSumMs / _windowCount;
        if (mean <= _latencyThresholdMs)
        {
            _effectiveSize = Math.Min(_maxBatchSize, _effectiveSize + _additiveIncrement);
        }
        else
        {
            _effectiveSize = Math.Max(1.0, _effectiveSize * _multiplicativeDecreaseFactor);
        }
    }

    /// <summary>
    /// Records a failed round-trip (transport throw or ack rejection) and
    /// applies a multiplicative decrease. A rising error rate therefore
    /// shrinks the effective batch size the same way rising latency does.
    /// Does not touch the latency window - an error carries no latency
    /// sample.
    /// </summary>
    public void RecordError() =>
        _effectiveSize = Math.Max(1.0, _effectiveSize * _multiplicativeDecreaseFactor);

    private void PushWindow(double ms)
    {
        if (_windowCount == _window.Length)
        {
            _windowSumMs -= _window[_windowHead];
            _window[_windowHead] = ms;
            _windowHead = (_windowHead + 1) % _window.Length;
            _windowSumMs += ms;
        }
        else
        {
            var idx = (_windowHead + _windowCount) % _window.Length;
            _window[idx] = ms;
            _windowCount++;
            _windowSumMs += ms;
        }
    }
}
