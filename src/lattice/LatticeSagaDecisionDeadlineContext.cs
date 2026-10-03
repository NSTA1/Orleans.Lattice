using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient signal carrying the UTC tick by which an atomic-write saga
/// must have recorded its commit decision for the caller currently waiting on
/// it.
/// </summary>
/// <remarks>
/// <para>
/// A saga's caller can stop waiting while the saga is still working: its call
/// times out, or the silo it called through retries the saga after a transient
/// fault until long after the original caller was answered. A saga that then
/// commits makes a batch visible after writes the caller issued once it had
/// been told the batch failed, so a newer batch reads back at the older one's
/// values. <c>LatticeGrain</c> stamps this deadline around every saga call from
/// its own response timeout, and the saga persists it and refuses to record a
/// commit decision past it - it rolls the batch back instead. A saga already
/// past its commit decision is unaffected: that decision made the batch
/// visible, and the saga only finishes delivering it.
/// </para>
/// <para>
/// The deadline is an estimate of when the caller gives up, derived from the
/// silo's response timeout rather than the caller's own (which is not
/// observable), less a margin for the caller's queueing and transit time.
/// </para>
/// </remarks>
internal static class LatticeSagaDecisionDeadlineContext
{
    /// <summary>
    /// The ambient decide-by deadline in UTC ticks, or <see langword="null"/>
    /// when the current call carries none.
    /// </summary>
    public static long? Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.SagaDecisionDeadlineRequestContextKey);
            return raw is long ticks && ticks > 0 ? ticks : null;
        }
        set
        {
            if (value is null || value.Value <= 0)
            {
                RequestContext.Remove(LatticeEventConstants.SagaDecisionDeadlineRequestContextKey);
            }
            else
            {
                RequestContext.Set(LatticeEventConstants.SagaDecisionDeadlineRequestContextKey, value.Value);
            }
        }
    }

    /// <summary>
    /// The decide-by deadline for a saga call issued now by a grain whose own
    /// caller times out after <paramref name="responseTimeout"/>: the timeout
    /// less a tenth (at least one second, at most half) for the time the
    /// caller's request spent queued and in transit before it arrived.
    /// </summary>
    public static long DeadlineFor(TimeSpan responseTimeout, DateTime utcNow)
    {
        if (responseTimeout <= TimeSpan.Zero || responseTimeout == Timeout.InfiniteTimeSpan)
            return 0;

        var marginTicks = Math.Min(
            Math.Max(responseTimeout.Ticks / 10, TimeSpan.TicksPerSecond),
            responseTimeout.Ticks / 2);
        return (utcNow + responseTimeout - TimeSpan.FromTicks(marginTicks)).Ticks;
    }

    /// <summary>
    /// Stamps <paramref name="deadlineUtcTicks"/> as the ambient decide-by
    /// deadline for the lifetime of the returned scope, restoring the prior
    /// value on <see cref="IDisposable.Dispose"/>. A non-positive value clears
    /// the ambient.
    /// </summary>
    public static IDisposable With(long deadlineUtcTicks)
    {
        var previous = Current;
        Current = deadlineUtcTicks;
        return new Scope(previous);
    }

    private sealed class Scope(long? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Current = previous;
        }
    }
}
