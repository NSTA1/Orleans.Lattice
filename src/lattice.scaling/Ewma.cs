namespace Orleans.Lattice.Scaling;

/// <summary>
/// A deterministic exponentially-weighted moving average parameterised by a
/// half-life, driven by explicit timestamps rather than wall-clock elapsed time
/// so it is fully testable through an injected <see cref="TimeProvider"/>. Used
/// to damp the one-sample lag and per-tick noise on the replica-demand scalar.
/// <para>
/// The smoothing factor for a step of duration <c>dt</c> is
/// <c>1 - 2^(-dt / halfLife)</c>: after exactly one half-life the average has
/// moved halfway from its previous value toward the new sample, independent of
/// the sampling cadence. Not thread-safe: the owning collector updates it from a
/// single sampling timer.
/// </para>
/// </summary>
internal sealed class Ewma
{
    private double _value;
    private bool _seeded;
    private DateTimeOffset _lastTimestamp;

    /// <summary>The current smoothed value, or <c>0.0</c> before the first update.</summary>
    internal double Current => _seeded ? _value : 0d;

    /// <summary>
    /// Folds <paramref name="sample"/> into the moving average using the elapsed
    /// time between <paramref name="now"/> and the previous update and the
    /// configured <paramref name="halfLife"/>. The first call seeds the average to
    /// the sample exactly. A non-positive <paramref name="halfLife"/>, or a
    /// non-positive elapsed step, disables smoothing for that step (the sample is
    /// adopted directly).
    /// </summary>
    /// <param name="sample">The new raw sample.</param>
    /// <param name="now">The timestamp of this sample.</param>
    /// <param name="halfLife">The smoothing half-life.</param>
    /// <returns>The updated smoothed value.</returns>
    internal double Update(double sample, DateTimeOffset now, TimeSpan halfLife)
    {
        if (!_seeded)
        {
            Set(sample, now);
            return _value;
        }

        var dtSeconds = (now - _lastTimestamp).TotalSeconds;
        if (halfLife <= TimeSpan.Zero || dtSeconds <= 0d)
        {
            Set(sample, now);
            return _value;
        }

        var alpha = 1d - Math.Pow(2d, -dtSeconds / halfLife.TotalSeconds);
        _value += alpha * (sample - _value);
        _lastTimestamp = now;
        return _value;
    }

    /// <summary>
    /// Hard-sets the smoothed value and resets the timestamp baseline to
    /// <paramref name="now"/>. Used to implement fast-attack behaviour: when the
    /// scalar jumps up, the average is snapped to the peak so the subsequent
    /// slow-release decay starts from there.
    /// </summary>
    /// <param name="value">The value to set the average to.</param>
    /// <param name="now">The timestamp baseline for the next update.</param>
    internal void Set(double value, DateTimeOffset now)
    {
        _value = value;
        _lastTimestamp = now;
        _seeded = true;
    }
}
