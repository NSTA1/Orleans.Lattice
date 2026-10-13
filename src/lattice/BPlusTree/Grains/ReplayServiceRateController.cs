using System.Diagnostics;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Experimental, release-driven one-permit service-rate probes. The caller
/// serializes observations and owns the separate service withholding ledger.
/// </summary>
internal sealed class ReplayServiceRateController(long windowTicks)
{
    internal static readonly long DefaultWindowTicks = 30L * Stopwatch.Frequency;
    internal const int MinimumCompletions = 8;
    internal const int RecoveryWindows = 4;

    private long _windowStart;
    private long _servedAtStart;
    private bool _hasWindow;
    private double _referenceRate;
    private int _probe;
    private int _direction = -1;
    private int _cooldown;
    private int _oldHolds;
    private int _rollback;
    private long _generation;
    private int _activeHolds;

    /// <summary>Enrolls replay work after acquisition, before any work starts.</summary>
    internal long Begin()
    {
        _activeHolds++;
        return _generation;
    }

    /// <summary>
    /// Returns -1 to withhold this returning permit, +1 to restore a service
    /// permit, or zero. Idle demand, faults and memory changes invalidate the
    /// comparison; they never restore service permits through the memory ledger.
    /// </summary>
    internal int Observe(
        long now, long served, bool saturated, int memoryWithheld,
        int circulating, int serviceWithheld)
    {
        if (memoryWithheld != 0)
        {
            Reset();
            return 0;
        }

        if (_rollback != 0)
        {
            var rollback = _rollback;
            _rollback = 0;
            return rollback;
        }

        if (!saturated)
        {
            Reset();
            var rollback = _rollback;
            _rollback = 0;
            return rollback;
        }

        if (_oldHolds > 0)
            return 0;

        if (!_hasWindow || now < _windowStart || served < _servedAtStart)
        {
            StartWindow(now, served);
            return 0;
        }

        var elapsed = now - _windowStart;
        var completions = served - _servedAtStart;
        if (elapsed < windowTicks || completions < MinimumCompletions)
            return 0;

        var rate = (double)completions / elapsed;
        StartWindow(now, served);

        if (_probe != 0)
        {
            var rejected = _probe < 0 ? rate < _referenceRate : rate <= _referenceRate;
            var probe = _probe;
            _probe = 0;
            if (rejected)
            {
                _direction = -probe;
                _cooldown = RecoveryWindows;
                DrainOldHolds();
                return -probe;
            }
            _cooldown = 1;
            return 0;
        }

        if (_cooldown > 0)
        {
            _cooldown--;
            return 0;
        }

        if ((_direction < 0 && circulating <= 1)
            || (_direction > 0 && serviceWithheld <= 0))
        {
            _direction = -_direction;
            _cooldown = RecoveryWindows;
            return 0;
        }

        _referenceRate = rate;
        _probe = _direction;
        DrainOldHolds();
        return _probe;
    }

    /// <summary>
    /// Excludes all holds enrolled before a limit change, including minute-long
    /// cold replays. A fixed settling delay cannot prove those holds drained.
    /// Completed before returning a physical permit, even if metrics fail.
    /// </summary>
    internal void Complete(long generation)
    {
        _activeHolds--;
        if (_oldHolds > 0 && generation < _generation)
            _oldHolds--;
    }

    /// <summary>Invalidates a window when a probe could not be applied.</summary>
    internal void Reset()
    {
        _rollback -= _probe;
        _hasWindow = false;
        _probe = 0;
        _oldHolds = 0;
        _generation++;
        _direction = -1;
        _cooldown = 0;
    }

    private void DrainOldHolds()
    {
        _oldHolds = _activeHolds;
        _generation++;
        _hasWindow = false;
    }

    private void StartWindow(long now, long served)
    {
        _windowStart = now;
        _servedAtStart = served;
        _hasWindow = true;
    }
}
