namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// The refusal-run latch read directly, covering the one property the handle-level
/// fixtures cannot reach: what a reader outside the owning turn sees while a run is
/// being started (issue #3286).
/// <para>
/// <b>Why this needs its own fixture.</b> The sibling
/// <c>RepoContextAnnIndexLoadResumeTests.OpenSaturationTerminal</c> tests drive the
/// latch through a real refused open, so every read they make happens between
/// attempts, when the latch is quiescent. The hazard here is only observable
/// <b>during</b> <see cref="RepoContextAnnOpenSaturationLatch.RecordRefusal"/>, and
/// the handle exposes no seam at that instant.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnOpenSaturationLatchTests
{
    /// <summary>
    /// A clock far from the epoch, so an elapsed bound measured from tick zero is
    /// enormous and an elapsed bound measured from the run's real start is nil. That
    /// gap is what makes the regression below a decisive reading rather than a
    /// timing-sensitive one.
    /// </summary>
    private static readonly DateTimeOffset Origin = new(2026, 9, 20, 12, 0, 0, TimeSpan.Zero);

    /// <summary>
    /// A fixed clock that runs a callback on the first <see cref="GetUtcNow"/> of any
    /// call stack, letting a test observe the latch at the instant the latch is asking
    /// the clock what time a run started. The re-entrancy guard is load-bearing: the
    /// observation itself reads state that consults the clock again.
    /// </summary>
    private sealed class ObservingTimeProvider : TimeProvider
    {
        private bool _observing;

        public Action? Observe { get; set; }

        public override DateTimeOffset GetUtcNow()
        {
            if (Observe is not null && !_observing)
            {
                _observing = true;
                try
                {
                    Observe();
                }
                finally
                {
                    _observing = false;
                }
            }

            return Origin;
        }
    }

    /// <summary>
    /// A latch that has never been refused reports no run at all, which is the
    /// property that lets the start timestamp double as the run flag.
    /// </summary>
    [Test]
    public void An_unrefused_latch_reports_no_run()
    {
        var latch = new RepoContextAnnOpenSaturationLatch(
            TimeProvider.System, maxConsecutiveRefusals: 4, terminalPeriod: TimeSpan.FromMinutes(10));

        Assert.Multiple(() =>
        {
            Assert.That(latch.State, Is.EqualTo(RepoContextAnnOpenSaturationState.Clear));
            Assert.That(latch.ConsecutiveRefusals, Is.Zero);
            Assert.That(latch.RefusedFor, Is.EqualTo(TimeSpan.Zero));
            Assert.That(latch.Clear(), Is.False, "clearing a latch with no run must report that it ended nothing");
        });
    }

    /// <summary>
    /// A reader that catches the latch mid-start sees no run rather than a breached
    /// elapsed bound.
    /// </summary>
    /// <remarks>
    /// <b>This is a regression test for a state that is now unrepresentable, and the
    /// distinction matters.</b> The latch first held the run as a <c>bool</c> beside
    /// the start timestamp, assigned in that order, so a reader arriving between the
    /// two writes saw "a run is in progress, and it started at tick zero" - an elapsed
    /// time of roughly two thousand years, which clears any bound and declares a
    /// healthy plane unavailable <b>on its very first refusal</b>. That is the same
    /// family of false signal issue #3286 exists to remove, pointing the other way.
    /// <para>
    /// Collapsing the pair into one field whose sentinel value IS the no-run state
    /// deletes the invariant rather than ordering it, so no write order and no memory
    /// model can resurrect the bad reading. This test pins that by making the race
    /// deterministic instead of hoping to catch it: the clock the latch consults to
    /// stamp the run start reads the latch back, which under the old shape lands
    /// exactly in the window between the two assignments, and under the current shape
    /// necessarily precedes the single write.
    /// </para>
    /// <para>
    /// Note the count bound is disabled so the only thing that could return
    /// <c>Unavailable</c> here is the elapsed bound being computed from the wrong
    /// origin. A failure is therefore unambiguous about its cause.
    /// </para>
    /// <para>
    /// The clock is consulted <b>twice</b> per refusal - once to stamp the start, and
    /// once by the state this method returns - so the observations read
    /// <c>Clear, Refusing</c>. Only the first is about the window under test; the
    /// second happens after the run has legitimately started and <c>Refusing</c> is
    /// the right answer there. Hence the assertions pin the first observation and the
    /// absence of <c>Unavailable</c> anywhere, rather than demanding every
    /// observation be <c>Clear</c>.
    /// </para>
    /// </remarks>
    [Test]
    public void A_reader_catching_the_first_refusal_mid_start_never_sees_a_breached_elapsed_bound()
    {
        var clock = new ObservingTimeProvider();
        var latch = new RepoContextAnnOpenSaturationLatch(
            clock, maxConsecutiveRefusals: 0, terminalPeriod: TimeSpan.FromMinutes(10));

        var observed = new List<RepoContextAnnOpenSaturationState>();
        clock.Observe = () => observed.Add(latch.State);

        var after = latch.RecordRefusal();

        Assert.Multiple(() =>
        {
            Assert.That(
                observed,
                Is.Not.Empty,
                "the latch must consult the clock while starting a run, or this test observes nothing and passes vacuously");
            Assert.That(
                observed,
                Has.None.EqualTo(RepoContextAnnOpenSaturationState.Unavailable),
                "a reader racing the start of a run must never see the elapsed bound measured from tick zero");
            Assert.That(
                observed[0],
                Is.EqualTo(RepoContextAnnOpenSaturationState.Clear),
                "the start stamp is read before the run is marked, so the first observation is of an idle latch");
            Assert.That(
                after,
                Is.EqualTo(RepoContextAnnOpenSaturationState.Refusing),
                "one refusal inside both bounds is transient back-pressure, not an outage");
        });
    }

    /// <summary>
    /// A reader that catches the latch mid-clear sees no run rather than a run with no
    /// refusals in it, which is the other half of the same ordering argument and the
    /// reason <c>Clear</c> ends the run before it zeroes the count.
    /// </summary>
    [Test]
    public void A_cleared_run_leaves_no_residue_a_later_refusal_could_inherit()
    {
        var latch = new RepoContextAnnOpenSaturationLatch(
            TimeProvider.System, maxConsecutiveRefusals: 2, terminalPeriod: TimeSpan.Zero);

        latch.RecordRefusal();
        Assert.That(latch.RecordRefusal(), Is.EqualTo(RepoContextAnnOpenSaturationState.Unavailable));
        Assert.That(latch.Clear(), Is.True);

        Assert.Multiple(() =>
        {
            Assert.That(latch.State, Is.EqualTo(RepoContextAnnOpenSaturationState.Clear));
            Assert.That(latch.ConsecutiveRefusals, Is.Zero);
            Assert.That(latch.RefusedFor, Is.EqualTo(TimeSpan.Zero));
            Assert.That(
                latch.RecordRefusal(),
                Is.EqualTo(RepoContextAnnOpenSaturationState.Refusing),
                "the first refusal of a fresh run must start a fresh count, not resume the declared one");
        });
    }
}
