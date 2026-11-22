namespace Orleans.Lattice.Views;

internal sealed partial class ViewMaintainerGrain
{
    // Lag age (issue #6145): how long the oldest source entry this view has not
    // applied has been unapplied. Derived from the entry lag that every drain pass
    // and status read already computes, so it costs no grain call and four scalars
    // per activation.
    //
    // The behind stamp is (when, the source head total observed then). Once the
    // applied total reaches that head total, every entry that existed at the stamp
    // has been applied, so the stamp moves forward to the last observation whose
    // head total is still ahead of the applied total - the entries at or below it
    // existed when it was observed - or to now when none is. The reported age is a
    // lower bound on the true age that undershoots by at most one observation
    // interval, and it restarts from zero when the maintainer reactivates.
    private DateTime? _lagBehindSinceUtc;
    private long _lagBehindHeadTotal;
    private DateTime _lagLastObservedUtc;
    private long _lagLastObservedHeadTotal;

    /// <summary>
    /// Records one entry-lag observation and returns the lag age it implies:
    /// <see cref="TimeSpan.Zero"/> when caught up.
    /// </summary>
    internal TimeSpan ObserveLagAge(long lag)
    {
        var now = _sourceIdentityClock.GetUtcNow().UtcDateTime;
        if (lag <= 0)
        {
            _lagBehindSinceUtc = null;
            return TimeSpan.Zero;
        }

        var applied = AppliedEntryTotal();
        var headTotal = applied + lag;
        if (_lagBehindSinceUtc is null)
        {
            _lagBehindSinceUtc = now;
            _lagBehindHeadTotal = headTotal;
        }
        else if (applied >= _lagBehindHeadTotal)
        {
            if (applied < _lagLastObservedHeadTotal)
            {
                _lagBehindSinceUtc = _lagLastObservedUtc;
                _lagBehindHeadTotal = _lagLastObservedHeadTotal;
            }
            else
            {
                _lagBehindSinceUtc = now;
                _lagBehindHeadTotal = headTotal;
            }
        }

        _lagLastObservedUtc = now;
        _lagLastObservedHeadTotal = headTotal;
        var age = now - _lagBehindSinceUtc.Value;
        return age > TimeSpan.Zero ? age : TimeSpan.Zero;
    }

    private long AppliedEntryTotal()
    {
        long total = 0;
        foreach (var offset in state.State.AppliedOffsets.Values)
        {
            total += offset + 1;
        }

        return total;
    }
}
