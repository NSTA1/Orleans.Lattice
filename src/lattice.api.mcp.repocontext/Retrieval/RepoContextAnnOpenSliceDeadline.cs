namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The open-slice deadline of issue #3284: a wall-clock bound that fires only at
/// a boundary the sliced work could actually have banked progress across.
/// </summary>
/// <remarks>
/// <para>
/// <b>What this replaces, and why a plain timer was not merely imprecise but
/// regenerative.</b> The open budget used to be a bare
/// <c>CancellationTokenSource(budget, TimeProvider)</c>, so it measured wall-clock
/// from the moment the slice started - including time the slice spent queued for a
/// WAL replay permit, in which the key walk can bank nothing by construction,
/// because the walk banks position per entry and an entry cannot be read before
/// its leaf activates. With a measured permit wait above the budget, every slice
/// was therefore guaranteed to expire having banked zero, and each expiry enqueued
/// another waiter behind the queue that caused it. Three such slices tripped the
/// empty-deferral escalation and the index could not open at all.
/// </para>
/// <para>
/// <b>The fix is to arm the deadline on progress rather than on elapsed time
/// alone.</b> At each budget boundary this asks the slice what it has banked. If
/// the count moved, the boundary is real - the slice was productive, its turn is
/// owed back, and the deadline fires. If nothing moved, the budget is not measuring
/// what it was meant to bound, so the slice is extended by one further period
/// instead of being cancelled into a retry that would queue afresh.
/// </para>
/// <para>
/// <b>The extension count is capped, and the cap is load-bearing in two
/// directions.</b> Without it this would be an unbounded open again, which is the
/// wedge issue #3130 removed - the coordinator turn must stay bounded. With it, the
/// existing empty-deferral escalation stays reachable: storage that genuinely
/// answers nothing still exhausts the extensions, still banks zero, and still fails
/// loudly after <c>MaxEmptyOpenDeferrals</c> attempts rather than deferring for
/// ever. The cap converts "expired having banked nothing" from the <i>normal</i>
/// outcome under permit contention back into the <i>exceptional</i> one it was
/// always meant to be.
/// </para>
/// <para>
/// <b>Only the resumable key walk is bounded by this, exactly as before.</b> The
/// restore phase banks nothing when interrupted, so bounding it would produce an
/// index that could never open; that split is issue #2953's correctness
/// requirement and is preserved by the caller, not by this type.
/// </para>
/// </remarks>
internal sealed class RepoContextAnnOpenSliceDeadline : IDisposable
{
    private readonly CancellationTokenSource _source = new();
    private readonly Func<int> _bankedProbe;
    private readonly int _startCount;
    private readonly int _maxExtensions;
    private readonly ITimer _timer;
    private int _extensions;

    /// <summary>Creates an armed deadline and starts its first period.</summary>
    /// <param name="budget">One slice period. Must be positive.</param>
    /// <param name="maxExtensions">
    /// How many further periods an unproductive slice may be granted before the
    /// deadline fires regardless. Zero reproduces the historical elapsed-only
    /// bound exactly, which is the configuration an operator uses to opt out.
    /// </param>
    /// <param name="bankedProbe">
    /// Reads how much the sliced work has banked so far. Called on a timer thread,
    /// so it must be safe to read concurrently and must not block.
    /// </param>
    /// <param name="timeProvider">The clock the period is measured against.</param>
    internal RepoContextAnnOpenSliceDeadline(
        TimeSpan budget, int maxExtensions, Func<int> bankedProbe, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(bankedProbe);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(budget, TimeSpan.Zero);

        _bankedProbe = bankedProbe;
        _maxExtensions = Math.Max(0, maxExtensions);
        _startCount = bankedProbe();
        _timer = timeProvider.CreateTimer(static s => ((RepoContextAnnOpenSliceDeadline)s!).Tick(), this, budget, budget);
    }

    /// <summary>The token cancelled when the deadline fires.</summary>
    internal CancellationToken Token => _source.Token;

    /// <summary>Whether the deadline has fired.</summary>
    internal bool IsCancellationRequested => _source.IsCancellationRequested;

    /// <summary>
    /// How many extra periods were granted because the slice had banked nothing at
    /// a boundary. Exposed so a fixture can assert that an extension happened at
    /// all rather than inferring it from a wall-clock reading.
    /// </summary>
    internal int Extensions => Volatile.Read(ref _extensions);

    private void Tick()
    {
        // Ordered progress-first on purpose. A slice that banked something at the
        // same boundary at which it exhausted its last extension is a PRODUCTIVE
        // slice, and must be recorded as one; testing the cap first would attribute
        // its cancellation to exhaustion and make the deferral read as empty.
        if (_bankedProbe() > _startCount || Volatile.Read(ref _extensions) >= _maxExtensions)
        {
            // Disposed sources are reached on the shutdown race, where the open has
            // already returned and there is nothing left to cancel.
            try
            {
                _source.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }

            return;
        }

        Interlocked.Increment(ref _extensions);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _timer.Dispose();
        _source.Dispose();
    }
}
