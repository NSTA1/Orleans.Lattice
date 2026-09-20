namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// What an approximate-index open has most recently observed about admission, as
/// a state a reader can act on rather than as a counter a reader must interpret.
/// </summary>
internal enum RepoContextAnnOpenSaturationState
{
    /// <summary>
    /// No unbroken run of admission refusals is in progress. The open has either
    /// never been refused or has banked progress since it last was, so whatever
    /// refusals happened are behind a walk that is converging.
    /// </summary>
    Clear = 0,

    /// <summary>
    /// The open is being refused and neither bound has been reached yet. This is
    /// the <b>transient</b> reading, and the one that means "wait": admission
    /// back-pressure is normal on a busy silo and clears on its own.
    /// </summary>
    Refusing = 1,

    /// <summary>
    /// The open has been refused past a declared bound, so the plane is reported
    /// unavailable-saturated rather than merely arming.
    /// <para>
    /// <b>Terminal as a CLAIM, not as a behaviour.</b> The open goes on retrying,
    /// so a plane whose saturation clears still self-heals without an operator. The
    /// state exists so that a reader can tell "still arming" from "will never arm
    /// at this capacity", which is the distinction a plane emitting only a rising
    /// refusal count cannot offer.
    /// </para>
    /// </summary>
    Unavailable = 2,
}

/// <summary>
/// Bounds an unbroken run of admission refusals on the approximate-index open and
/// reports when that run has gone on long enough to be declared rather than merely
/// counted.
/// <para>
/// <b>Why this type exists (issue #3286).</b> The open already refuses correctly:
/// a saturated silo declines the WAL replay permit, the walk yields, the refusal
/// is booked as its own outcome, and the next tick tries again. What it had no way
/// to express was that the retrying had stopped being a strategy. Measured live,
/// <c>repocontext.ann.index.load{outcome="refused"}</c> rose at 0.66 per minute
/// for as long as it was watched while <c>fresh</c> and <c>resumed</c> stayed at
/// zero, the opening phase's in-flight gauge sat at 273 seconds with every later
/// phase at zero, and <c>/health/ready</c> returned 503 throughout. Every one of
/// those readings is also what a large, healthy, slow cold open produces. An
/// operator cannot act on a signal that means both "wait" and "this will not
/// finish".
/// </para>
/// <para>
/// <b>Two bounds, because either alone is unreachable on some deployment.</b> A
/// count bound is never reached by a host whose coordinator ticks slowly, which is
/// precisely the host whose operator most needs the signal. An elapsed bound is
/// reached by a host that happened to take two refusals across a long, slow, and
/// entirely healthy startup. Whichever is reached first declares.
/// </para>
/// <para>
/// <b>The run is PRESENT-TENSE, deliberately mirroring the empty-slice deferral
/// counter beside it.</b> Any open that banks progress clears it outright, so a
/// walk that advances on every attempt and is refused on every attempt - which is
/// converging, not wedged - can never reach either bound however long it runs. A
/// lifetime tally would declare such a plane unavailable on its twelfth refusal
/// and turn a healthy sliced open over a busy silo into a reported outage.
/// </para>
/// </summary>
/// <remarks>
/// Mutated only by its owning handle, under an open the coordinator has already
/// serialised on a non-reentrant turn, so there is exactly one writer.
/// <para>
/// <b>The run is one field, so a reader cannot observe a half-started run.</b> An
/// earlier shape carried a separate <c>bool</c> beside the start timestamp, and the
/// two together could be read mid-write as "a run is in progress, and it started at
/// tick zero" - which measures elapsed time from the year 1 and declares a healthy
/// plane unavailable on its very first refusal. That is the same family of false
/// signal this type exists to remove, pointing the other way, so the invariant was
/// deleted rather than ordered: the sentinel start value <b>is</b> the "no run"
/// state, the bad combination is unrepresentable, and correctness no longer rests
/// on two writes landing in a particular order. Reads and writes are volatile so a
/// reader outside the owning turn sees a start value that is current rather than
/// cached.
/// </para>
/// <para>
/// A torn read across the start value and the refusal count is still possible and
/// is deliberately left alone, because it is benign in both directions: the count
/// only ever rises within a run, so a stale count under-reports and can only delay
/// a declaration, never manufacture one.
/// </para>
/// </remarks>
internal sealed class RepoContextAnnOpenSaturationLatch
{
    /// <summary>
    /// The start value meaning "no run in progress". Chosen below zero because
    /// <see cref="DateTimeOffset.UtcTicks"/> is never negative, so no real clock
    /// reading can collide with it and be mistaken for an idle latch.
    /// </summary>
    private const long NoRun = long.MinValue;

    private readonly TimeProvider _timeProvider;
    private readonly int _maxConsecutiveRefusals;
    private readonly long _terminalPeriodTicks;

    private long _firstRefusalTicks = NoRun;
    private int _consecutiveRefusals;

    /// <summary>Creates a latch over the supplied bounds.</summary>
    /// <param name="timeProvider">The clock measuring the elapsed bound. Must not be <see langword="null"/>.</param>
    /// <param name="maxConsecutiveRefusals">Consecutive refusals that declare the run terminal; non-positive removes the count bound.</param>
    /// <param name="terminalPeriod">Elapsed time that declares the run terminal; non-positive removes the elapsed bound.</param>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider"/> is null.</exception>
    public RepoContextAnnOpenSaturationLatch(
        TimeProvider timeProvider, int maxConsecutiveRefusals, TimeSpan terminalPeriod)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        _timeProvider = timeProvider;
        _maxConsecutiveRefusals = maxConsecutiveRefusals;
        _terminalPeriodTicks = terminalPeriod.Ticks <= 0 ? 0 : terminalPeriod.Ticks;
    }

    /// <summary>
    /// The current state. Recomputed from the run rather than stored, so a run that
    /// crosses the elapsed bound between two refusals is reported as terminal on the
    /// read rather than only on the next refusal.
    /// </summary>
    public RepoContextAnnOpenSaturationState State
    {
        get
        {
            var startedAt = Volatile.Read(ref _firstRefusalTicks);
            if (startedAt == NoRun)
            {
                return RepoContextAnnOpenSaturationState.Clear;
            }

            return HasReachedCountBound() || HasReachedPeriodBound(startedAt)
                ? RepoContextAnnOpenSaturationState.Unavailable
                : RepoContextAnnOpenSaturationState.Refusing;
        }
    }

    /// <summary>
    /// How many refusals the open has taken in an unbroken run, reset to zero by
    /// any open that banked progress.
    /// </summary>
    public int ConsecutiveRefusals => Volatile.Read(ref _consecutiveRefusals);

    /// <summary>
    /// How long the unbroken run of refusals has lasted, or
    /// <see cref="TimeSpan.Zero"/> when no run is in progress.
    /// </summary>
    public TimeSpan RefusedFor
    {
        get
        {
            var startedAt = Volatile.Read(ref _firstRefusalTicks);
            if (startedAt == NoRun)
            {
                return TimeSpan.Zero;
            }

            var elapsed = _timeProvider.GetUtcNow().UtcTicks - startedAt;
            return elapsed <= 0 ? TimeSpan.Zero : new TimeSpan(elapsed);
        }
    }

    /// <summary>
    /// Accounts one admission refusal and reports the state it leaves the run in.
    /// </summary>
    /// <returns>The state after this refusal.</returns>
    public RepoContextAnnOpenSaturationState RecordRefusal()
    {
        if (Volatile.Read(ref _firstRefusalTicks) == NoRun)
        {
            // The run starts at the FIRST refusal, not at the latest one, or the
            // elapsed bound would measure the gap between two refusals and could
            // never be reached however long the saturation lasted.
            Volatile.Write(ref _firstRefusalTicks, _timeProvider.GetUtcNow().UtcTicks);
        }

        Volatile.Write(ref _consecutiveRefusals, _consecutiveRefusals + 1);
        return State;
    }

    /// <summary>
    /// Ends the run, because the open banked progress or completed. Idempotent, and
    /// deliberately callable from the success path as well as the progress path:
    /// the state has to be able to leave <see cref="RepoContextAnnOpenSaturationState.Unavailable"/>,
    /// or a plane whose saturation cleared would go on reporting an outage it had
    /// already recovered from.
    /// </summary>
    /// <returns><see langword="true"/> when this call actually ended a run.</returns>
    public bool Clear()
    {
        if (Volatile.Read(ref _firstRefusalTicks) == NoRun
            && Volatile.Read(ref _consecutiveRefusals) == 0)
        {
            return false;
        }

        // The run is ended before the count is cleared, so a reader that catches
        // the pair mid-write resolves to Clear rather than to a run with no
        // refusals in it.
        Volatile.Write(ref _firstRefusalTicks, NoRun);
        Volatile.Write(ref _consecutiveRefusals, 0);
        return true;
    }

    private bool HasReachedCountBound()
        => _maxConsecutiveRefusals > 0 && ConsecutiveRefusals >= _maxConsecutiveRefusals;

    private bool HasReachedPeriodBound(long startedAt)
        => _terminalPeriodTicks > 0
            && _timeProvider.GetUtcNow().UtcTicks - startedAt >= _terminalPeriodTicks;
}
