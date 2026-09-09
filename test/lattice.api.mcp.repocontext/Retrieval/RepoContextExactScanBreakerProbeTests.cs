namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests the half-open probe <see cref="RepoContextExactScanBreaker"/> grew for
/// issue #2362. The breaker's trip and suppress behaviour is covered by
/// <see cref="RepoContextExactScanBreakerTests"/>; what is asserted here is the
/// exit, which before this had no representation in the type at all.
/// <para>
/// <b>Why the exit needed to be part of the breaker rather than its caller.</b>
/// The breaker previously closed only when
/// <see cref="RepoContextExactScanBreaker.Reset"/> was called, and its single
/// caller invoked it only after the approximate plane had answered a query for
/// itself. That makes the exit conditional on a subsystem the breaker does not
/// control and, on a deployment whose plane never finishes a build, cannot
/// influence - so on such a host the open state was not slow to clear, it was
/// unreachable from every state the process could occupy. A timer the breaker
/// owns is the smallest thing that removes that dependency.
/// </para>
/// <para>
/// Every test here drives an injected clock. The alternative - waiting out a real
/// delay - would make the fixture either slow enough to be excluded from the
/// developer loop or short enough to be timing-dependent, and a flaky test of a
/// recovery path teaches a reader to distrust exactly the signal the path exists
/// to give.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextExactScanBreakerProbeTests
{
    private const string RepoId = "acme";

    private static readonly TimeSpan Initial = TimeSpan.FromSeconds(60);

    private static readonly TimeSpan Max = TimeSpan.FromMinutes(16);

    [Test]
    public void A_repository_that_never_stalled_evaluates_closed()
    {
        var breaker = Create(out _);

        Assert.Multiple(() =>
        {
            Assert.That(breaker.Evaluate(RepoId), Is.EqualTo(RepoContextExactScanBreakerDecision.Closed));
            Assert.That(breaker.IsTripped(RepoId), Is.False,
                "Evaluating a repository must not enrol it. This runs on the read path of every search, so "
                + "a lookup that created state would grow the map with one entry per repository ever "
                + "queried and make the breaker's own memory a function of traffic.");
            Assert.That(breaker.ConsecutiveStalls(RepoId), Is.Zero);
            Assert.That(breaker.OpenFor(RepoId), Is.Null);
            Assert.That(breaker.ProbeDueIn(RepoId), Is.Null);
        });
    }

    [Test]
    public void An_open_breaker_stays_open_until_its_delay_elapses()
    {
        var breaker = Create(out var clock);
        breaker.Trip(RepoId);

        var immediately = breaker.Evaluate(RepoId);
        clock.Advance(Initial - TimeSpan.FromSeconds(1));
        var justBefore = breaker.Evaluate(RepoId);
        clock.Advance(TimeSpan.FromSeconds(1));
        var whenDue = breaker.Evaluate(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(immediately, Is.EqualTo(RepoContextExactScanBreakerDecision.Open));
            Assert.That(justBefore, Is.EqualTo(RepoContextExactScanBreakerDecision.Open));
            Assert.That(whenDue, Is.EqualTo(RepoContextExactScanBreakerDecision.Probe),
                "The delay is what makes the exit affordable. A breaker that probed immediately would pay "
                + "the page-fill ceiling it just paid, on the next query, forever.");
        });
    }

    [Test]
    public void Only_one_caller_per_window_is_granted_the_probe()
    {
        var breaker = Create(out var clock);
        breaker.Trip(RepoId);
        clock.Advance(Initial);

        var granted = breaker.Evaluate(RepoId);
        var second = breaker.Evaluate(RepoId);
        var third = breaker.Evaluate(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(granted, Is.EqualTo(RepoContextExactScanBreakerDecision.Probe));
            Assert.That(second, Is.EqualTo(RepoContextExactScanBreakerDecision.Open));
            Assert.That(third, Is.EqualTo(RepoContextExactScanBreakerDecision.Open),
                "The grant re-arms the window as it is handed out, rather than waiting to hear how the "
                + "probe went. A probe that never reports back - it faults, or its query is cancelled - "
                + "must not leave the window open to every subsequent caller, or one recovery attempt "
                + "becomes a stampede against the tree the build is already competing for.");
        });
    }

    [Test]
    public void The_delay_doubles_with_each_stall()
    {
        var breaker = Create(out var clock);

        breaker.Trip(RepoId);
        var afterFirst = breaker.ProbeDueIn(RepoId);
        breaker.Trip(RepoId);
        var afterSecond = breaker.ProbeDueIn(RepoId);
        breaker.Trip(RepoId);
        var afterThird = breaker.ProbeDueIn(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.EqualTo(Initial));
            Assert.That(afterSecond, Is.EqualTo(Initial * 2));
            Assert.That(afterThird, Is.EqualTo(Initial * 4));
            Assert.That(breaker.ConsecutiveStalls(RepoId), Is.EqualTo(3));
        });

        Assert.That(clock.GetUtcNow(), Is.EqualTo(DateTimeOffset.UnixEpoch),
            "Sanity: none of the above advanced the clock, so the growth measured is the backoff and not "
            + "elapsed time.");
    }

    [Test]
    public void The_delay_is_capped_so_a_wedged_repository_keeps_probing()
    {
        var breaker = Create(out _);

        for (var i = 0; i < 40; i++)
        {
            breaker.Trip(RepoId);
        }

        Assert.That(breaker.ProbeDueIn(RepoId), Is.EqualTo(Max),
            "Unbounded doubling would be indistinguishable from having no exit within any timeframe an "
            + "operator watches, which is the state issue #2362 was filed from. The cap is what keeps the "
            + "guarantee a real one rather than an asymptotic one.");
    }

    /// <summary>
    /// The shift used to grow the delay must not run off the end of a 64-bit tick
    /// count. A cap that was only reached by arithmetic that overflowed first would
    /// produce a delay that wrapped to something small or negative, turning the
    /// backoff into a busy retry at exactly the point it was supposed to be
    /// quietest.
    /// </summary>
    [Test]
    public void A_pathological_stall_count_does_not_overflow_the_backoff()
    {
        var breaker = Create(out _);

        for (var i = 0; i < 200; i++)
        {
            breaker.Trip(RepoId);
        }

        Assert.Multiple(() =>
        {
            Assert.That(breaker.ProbeDueIn(RepoId), Is.EqualTo(Max));
            Assert.That(breaker.ProbeDueIn(RepoId), Is.GreaterThan(TimeSpan.Zero));
        });
    }

    [Test]
    public void Resetting_closes_the_breaker_and_forgets_the_backoff()
    {
        var breaker = Create(out _);
        breaker.Trip(RepoId);
        breaker.Trip(RepoId);
        breaker.Trip(RepoId);

        var closed = breaker.Reset(RepoId);
        breaker.Trip(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(closed, Is.True);
            Assert.That(breaker.ConsecutiveStalls(RepoId), Is.EqualTo(1));
            Assert.That(breaker.ProbeDueIn(RepoId), Is.EqualTo(Initial),
                "A repository that recovered and later stalled again is in a new episode, not a "
                + "continuation of the old one. Carrying the old backoff forward would make a single bad "
                + "hour permanently raise the cost of every later recovery.");
        });
    }

    [Test]
    public void An_open_breaker_reports_how_long_it_has_been_open()
    {
        var breaker = Create(out var clock);
        breaker.Trip(RepoId);
        clock.Advance(TimeSpan.FromMinutes(90));
        breaker.Trip(RepoId);

        Assert.That(breaker.OpenFor(RepoId), Is.EqualTo(TimeSpan.FromMinutes(90)),
            "The age is measured from when the episode opened, not from the last stall. The deployment in "
            + "issue #2362 had been open for 96 minutes on a single trip, and it is that age - not the "
            + "trip count - that distinguishes a breaker riding out contention from one that is wedged.");
    }

    [Test]
    public void A_maximum_below_the_initial_delay_is_clamped_up_to_it()
    {
        var breaker = new RepoContextExactScanBreaker(
            new ProbeClock(), TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(1));

        breaker.Trip(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(breaker.MaxProbeDelay, Is.EqualTo(breaker.InitialProbeDelay));
            Assert.That(breaker.ProbeDueIn(RepoId), Is.EqualTo(TimeSpan.FromMinutes(5)),
                "A misconfiguration has to degrade to a fixed interval rather than to an incoherent one. "
                + "Letting the cap sit below the first delay would make the first probe late and every "
                + "later one early, which is not a policy anyone chose.");
        });
    }

    [Test]
    public void A_negative_initial_delay_is_clamped_to_probing_immediately()
    {
        var breaker = new RepoContextExactScanBreaker(new ProbeClock(), TimeSpan.FromSeconds(-30));

        breaker.Trip(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(breaker.InitialProbeDelay, Is.EqualTo(TimeSpan.Zero));
            Assert.That(breaker.Evaluate(RepoId), Is.EqualTo(RepoContextExactScanBreakerDecision.Probe));
        });
    }

    [Test]
    public void Episodes_are_tracked_per_repository()
    {
        var breaker = Create(out var clock);
        breaker.Trip("first");
        clock.Advance(Initial);
        breaker.Trip("second");

        Assert.Multiple(() =>
        {
            Assert.That(breaker.Evaluate("first"), Is.EqualTo(RepoContextExactScanBreakerDecision.Probe));
            Assert.That(breaker.Evaluate("second"), Is.EqualTo(RepoContextExactScanBreakerDecision.Open));
            Assert.That(breaker.Evaluate("third"), Is.EqualTo(RepoContextExactScanBreakerDecision.Closed));
        });
    }

    [Test]
    public void The_new_members_reject_a_null_repository()
    {
        var breaker = Create(out _);

        Assert.Multiple(() =>
        {
            Assert.That(() => breaker.Evaluate(null!), Throws.ArgumentNullException);
            Assert.That(() => breaker.ConsecutiveStalls(null!), Throws.ArgumentNullException);
            Assert.That(() => breaker.OpenFor(null!), Throws.ArgumentNullException);
            Assert.That(() => breaker.ProbeDueIn(null!), Throws.ArgumentNullException);
        });
    }

    private static RepoContextExactScanBreaker Create(out ProbeClock clock)
    {
        clock = new ProbeClock();
        return new RepoContextExactScanBreaker(clock, Initial, Max);
    }

    private sealed class ProbeClock : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.UnixEpoch;

        public override DateTimeOffset GetUtcNow() => _now;

        public void Advance(TimeSpan by) => _now += by;
    }
}
